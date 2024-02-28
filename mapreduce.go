package mr

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gofish2020/easymr/syncx"
	"github.com/gofish2020/easymr/threading"
)

const (
	defaultWorkers = 16
	minWorkers     = 1
)

var (
	Placeholder PlaceholderType
	// ErrCancelWithNil is an error that mapreduce was cancelled with nil.
	ErrCancelWithNil = errors.New("mapreduce cancelled with nil")
	// ErrReduceNoOutput is an error that reduce did not output a value.
	ErrReduceNoOutput = errors.New("reduce not writing value")
)

type (
	// 生产数据
	GenerateFunc func(source chan<- interface{})

	// 处理数据
	MapperFunc  func(item interface{}, writer Writer, cancel func(error)) // 有输出，可取消
	MapFunc     func(item interface{}, writer Writer)                     // 有输出（MapperFunc函数简化版）
	VoidMapFunc func(item interface{})                                    // 无输出（MapperFunc函数简化版）

	// 汇总结果
	ReducerFunc     func(pipe <-chan interface{}, writer Writer, cancel func(error)) // 有输出，可取消
	VoidReducerFunc func(pipe <-chan interface{}, cancel func(error))                // 无输出（ReducerFunc函数简化版）

	// 配置项
	Option func(opts *mapReduceOptions)

	mapReduceOptions struct {
		workers int // 并发处理数据的最大协程数量
	}

	// Writer interface wraps Write method.
	Writer interface {
		Write(v interface{})
	}

	PlaceholderType = struct{}
)

// WithWorkers 子协程的数量
func WithWorkers(workers int) Option {
	return func(opts *mapReduceOptions) {
		if workers < minWorkers {
			opts.workers = minWorkers
		} else {
			opts.workers = workers
		}
	}
}

// Finish 多个函数fns并发执行（任何一个函数出错，终止执行）
func Finish(fns ...func() error) error {
	if len(fns) == 0 {
		return nil
	}

	return MapReduceVoid(func(source chan<- interface{}) {
		for _, fn := range fns { // 将函数保存到source通道中
			source <- fn
		}
	}, func(item interface{}, writer Writer, cancel func(error)) {
		// item是从source通道中获取的数据
		fn := item.(func() error)
		if err := fn(); err != nil {
			cancel(err) // 函数出错，终止
		}
	}, func(pipe <-chan interface{}, cancel func(error)) {
		drain(pipe) //pipe指代就是 collector通道
	}, WithWorkers(len(fns)))
}

// 对MapReduce的二次包装（区别：不需要记录返回值）
// MapReduceVoid 从source中读取数据，通过mapper并发处理后，将中间结果保存到collector中，并通过reducer函数对中间结果进行汇总（但是不返回）
func MapReduceVoid(generate GenerateFunc, mapper MapperFunc, reducer VoidReducerFunc, opts ...Option) error {
	_, err := MapReduce(generate, mapper, func(input <-chan interface{}, writer Writer, cancel func(error)) {
		// input就是collector通道；reducer函数做的事情就是从collector中读取数据，进行最后的汇总
		reducer(input, cancel)
		// 含义：往output通道写入数据
		writer.Write(Placeholder)
	}, opts...)
	return err
}

// FinishVoid 并发执行fns函数（出错也不停止）
func FinishVoid(fns ...func()) {
	if len(fns) == 0 {
		return
	}

	MapVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}) {
		fn := item.(func())
		fn()
	}, WithWorkers(len(fns)))
}

// MapVoid 从source中读取数据，经过mapper处理每一个数据item
func MapVoid(generate GenerateFunc, mapper VoidMapFunc, opts ...Option) {
	drain(Map(generate, func(item interface{}, writer Writer) {
		mapper(item)
	}, opts...))
}

// Map 从source中读取数据，通过mapper处理后，并将结果保存到collector中，并返回collector供外部读取
func Map(generate GenerateFunc, mapper MapFunc, opts ...Option) chan interface{} {
	options := buildOptions(opts...)
	source := buildSource(generate)
	collector := make(chan interface{}, options.workers)
	done := syncx.NewDoneChan()
	go executeMappers(mapper, source, collector, done.Done(), options.workers)
	return collector
}

// MapReduce 从source中读取数据，通过mapper并发处理后，将中间结果保存到collector中，并通过reducer函数对中间结果进行汇总，返回
func MapReduce(generate GenerateFunc, mapper MapperFunc, reducer ReducerFunc, opts ...Option) (interface{}, error) {
	// 创建source通道
	source := buildSource(generate)
	// 核心处理方法
	return MapReduceWithSource(source, mapper, reducer, opts...)
}

func MapReduceWithSource(source <-chan interface{}, mapper MapperFunc, reducer ReducerFunc, opts ...Option) (interface{}, error) {

	// 1.配置协程数
	options := buildOptions(opts...)
	// 2.输出结果
	output := make(chan interface{})

	defer func() {
		// 限定输出结果只能有一个，不能往output中多次写入
		for range output {
			panic("more than one element written in reducer")
		}
	}()

	// 3.mapper处理的中间结果，存储collector通道中
	collector := make(chan interface{}, options.workers)

	done := syncx.NewDoneChan() // 理解为：用通道作为关闭标识

	writer := newGuardedWriter(output, done.Done()) // writer就是指代的output通道

	var closeOnce sync.Once // 目的：在finish中只执行一次关闭
	var retErr error

	finish := func() {

		closeOnce.Do(func() {
			done.Close()  //理解为：打开了关闭标识
			close(output) // 关闭output可以避免，如果没有向output中写入数据，output最后不会阻塞
		})
	}

	cancel := once(func(err error) { // 取消操作
		// 设置错误信息
		if err != nil {
			retErr = err
		} else {
			retErr = ErrCancelWithNil
		}
		drain(source) //一旦取消，source中的数据就不会被消费，那么生产协程就会一直阻塞中（避免内存泄漏，让生产协程能正常退出）
		finish()
	})

	// 4. 本协程的目的就是从collector通道中读取数据，在reducer函数中进行最后的汇总后，写入到writer中（即：output)
	go func() {
		defer func() {
			// drain函数：如果collector不关闭，等待
			drain(collector) // （这里也是防御编程，避免在reducer中忘记读取collector数据）
			if r := recover(); r != nil {
				cancel(fmt.Errorf("%v", r))
			} else {
				finish()
			}
		}()
		// writer就是指代的output通道
		reducer(collector, writer, cancel)
	}()

	// 5. 从source中读取数据，并启动最多workers个子协程并行处理，处理后到结果保存到collector中
	go executeMappers(func(item interface{}, w Writer) {
		// item就是从source中获取的数据
		// w 指代的是collector
		mapper(item, w, cancel)
	}, source, collector, done.Done(), options.workers)

	// 这里是阻塞中（解除阻塞条件：要么关闭output，要么向output写入数据）
	value, ok := <-output
	if err := retErr; err != nil {
		return nil, err
	} else if ok {
		return value, nil
	} else {
		return nil, ErrReduceNoOutput
	}
}

// executeMappers 从input中读取数据item，并行开启多个协程处理，并将结果保存到 collector 中
func executeMappers(mapper MapFunc, input <-chan interface{}, collector chan<- interface{},
	done <-chan PlaceholderType, workers int) {

	var wg sync.WaitGroup

	// 只有source or done 关闭了，defer才会执行
	defer func() {
		wg.Wait()        // 等协程处理完数据
		close(collector) // 关闭collector，reducer协程才会退出
	}()

	// 目的：限定启动的协程的最大数量
	pool := make(chan PlaceholderType, workers)

	// 这里的writer指代的是collector
	writer := newGuardedWriter(collector, done)

	// 这里是一个死循环，负责消费source中的数据（和buildSource函数呼应）
	for {
		select {
		case <-done: // done关闭
			return
		case pool <- Placeholder: // 最多启动 workers个协程处理
			item, ok := <-input
			if !ok { //source被关闭（buildSource函数中，发送完数据后会关闭source）
				<-pool
				return
			}
			wg.Add(1)

			// 协程
			threading.GoSafe(func() {
				defer func() {
					wg.Done()
					<-pool
				}()
				// source中的item数据，处理完成的结果保存到 collector中
				mapper(item, writer)
			})
		}
	}
}

func buildOptions(opts ...Option) *mapReduceOptions {
	options := newOptions()
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func buildSource(generate GenerateFunc) chan interface{} {
	source := make(chan interface{})

	// 启动协程，调用generate函数生产数据到 source通道中
	threading.GoSafe(func() {
		// generate函数执行完毕后，关闭source
		defer close(source)
		// generate内部，负责将数据写入到source中
		generate(source)
	})

	return source
}

// drain 函数有两种含义：
// 1.一直阻塞直到 channel关闭
// 2.不断的读取channel中的数据，让写入端可以一直写入（直到没有新数据写入）
func drain(channel <-chan interface{}) {
	for range channel {
	}
}

func newOptions() *mapReduceOptions {
	return &mapReduceOptions{
		workers: defaultWorkers, // 默认16个子协程
	}
}

func once(fn func(error)) func(error) {
	once := new(sync.Once)
	return func(err error) {
		once.Do(func() {
			fn(err)
		})
	}
}

type guardedWriter struct {
	//写入chan
	channel chan<- interface{}
	//完成标志
	done <-chan PlaceholderType
}

// 结构体中包装两个通道
func newGuardedWriter(channel chan<- interface{}, done <-chan PlaceholderType) guardedWriter {
	return guardedWriter{
		channel: channel,
		done:    done,
	}
}

func (gw guardedWriter) Write(v interface{}) {
	select {
	case <-gw.done: // 完成标识
		return
	default:
		gw.channel <- v // 写入数据
	}
}
