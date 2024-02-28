package threading

import (
	"fmt"
)

func GoSafe(fn func()) {

	go func() {
		defer func() {
			if p := recover(); p != nil {
				fmt.Println(p)
			}
		}()
		fn()
	}()
}
