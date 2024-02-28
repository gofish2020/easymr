package mr

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 并发执行不同的函数(最终的结果保存到total中)
func TestFinish(t *testing.T) {
	var total uint32

	err := Finish(func() error {
		atomic.AddUint32(&total, 2)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 3)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 5)
		return nil
	})

	assert.Equal(t, uint32(10), atomic.LoadUint32(&total))
	assert.Nil(t, err)
}

func TestFinishErr(t *testing.T) {

	errNew := errors.New("test error")
	var total uint32

	err := Finish(func() error {
		atomic.AddUint32(&total, 2)
		return errNew
	}, func() error {
		atomic.AddUint32(&total, 3)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 5)
		return nil
	})

	assert.Equal(t, err, errNew)
}

// 过滤的效果（当有一批数据需要处理，只过滤留下 >5的结果）
func TestMap(t *testing.T) {
	collector := Map(func(source chan<- interface{}) {
		for i := 0; i < 10; i++ {
			source <- i
		}
	}, func(item interface{}, writer Writer) {
		val, _ := item.(int)
		if val > 5 {
			writer.Write(val)
		}
	}, WithWorkers(-1))
	result := 0
	for v := range collector {
		result += v.(int)
	}
	assert.Equal(t, 30, result)
}

func TestMapReduce(t *testing.T) {

	uids := []int64{1, 2, 3, 4, 5, 6, 79}
	r, err := MapReduce(func(source chan<- interface{}) {
		for _, uid := range uids {
			source <- uid
		}
	}, func(item interface{}, writer Writer, cancel func(error)) {
		uid := item.(int64)
		ok, err := check(uid)
		if err != nil {
			cancel(err)
		}
		if ok {
			writer.Write(uid)
		}
	}, func(pipe <-chan interface{}, writer Writer, cancel func(error)) {
		var uids []int64
		for p := range pipe {
			uids = append(uids, p.(int64))
		}
		writer.Write(uids)
	})

	assert.Nil(t, err)
	assert.Equal(t, []int64{79}, r)

}

func check(uid int64) (bool, error) {
	// do something check user legal
	if uid > 6 {
		return true, nil
	}
	return false, nil
}

func TestDrain(t *testing.T) {

	ch := make(chan interface{})
	for i := 0; i < 3; i++ {
		go func(val int) {
			ch <- val // 只有ch被读取，val才能保存到ch中，协程才能继续往下执行，打印出结果
			fmt.Println(val)
		}(i)
	}
	// 如果不调用drain，ch就一直不被读取，那么上面的3个协程就一直阻塞在ch上，无法退出（造成内存泄漏）
	drain(ch)
}
