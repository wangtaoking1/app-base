package heap

import "fmt"

func ExampleHeap() {
	hp := New[int]()
	hp.Push(3)
	hp.Push(2)
	hp.Push(5)
	fmt.Println(hp.Pop())
	fmt.Println("Empty:", hp.Empty())
	fmt.Println("Size:", hp.Size())

	// Output:
	// 2
	// Empty: false
	// Size: 2
}
