package lock_manager

type ProcListNode struct {
	prev  *ProcListNode
	next  *ProcListNode
	value *Proc
}

type ProcListHead struct {
	head *ProcListNode
	tail *ProcListNode
}

func NewProcList() *ProcListHead {
	return &ProcListHead{}
}

func (head *ProcListHead) NewMultableIterator() *ProcListMutableIterator {
	if head.head != nil {
		return &ProcListMutableIterator{cur: head.head.value, next: head.head.next}
	} else {
		return nil
	}
}

func (head *ProcListHead) PushHead(proc *Proc) {
	node := &ProcListNode{value: proc}
	if head.head == nil {
		head.head = node
		head.tail = node
	} else {
		node.next = head.head
		head.head.prev = node
		head.head = node
	}
}

func (head *ProcListHead) PushTail(proc *Proc) {
	node := &ProcListNode{value: proc}
	if head.head == nil {
		head.head = node
		head.tail = node
	} else {
		node.prev = head.tail
		head.tail.next = node
		head.tail = node
	}
}

func (head *ProcListHead) IsEmpty() bool {
	return head.head == nil
}

func (head *ProcListHead) Delete(c *ProcListMutableIterator) {
	if c.next == nil {
		// current item is tail, update the tail node and head info.
		head.tail = head.tail.prev
		if head.tail == nil {
			// list is clean now.
			head.head = nil
			head.tail = nil
		} else {
			// cut the list tail.
			head.tail.next = nil
		}
		return
	}
	cur := c.next.prev
	if cur == head.head && cur == head.tail {
		panic("impossible case")
	} else if cur == head.head {
		head.head = cur.next
		head.head.prev = nil
	} else if cur == head.tail {
		head.tail = cur.prev
		head.tail.next = nil
	} else {
		// normal case.
		cur.next.prev = cur.prev
		cur.prev.next = cur.next
	}
}

// ProcListMutableIterator the iterator that allows modification while iterating.
type ProcListMutableIterator struct {
	cur  *Proc
	next *ProcListNode
	//from *ProcListHead
}

func (c *ProcListMutableIterator) Next() *ProcListMutableIterator {
	if c.next != nil {
		return &ProcListMutableIterator{c.next.value, c.next}
	} else {
		return nil
	}
}
