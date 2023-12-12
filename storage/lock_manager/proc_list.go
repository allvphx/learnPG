package lock_manager

type ProcListNode struct {
	prev int
	next int
}

type ProcListHead struct {
	head int
	tail int
}

// ProcListMutableIterator the iterator that allows modification while iterating.
type ProcListMutableIterator struct {
	cur  int
	next int
}
