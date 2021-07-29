package atomic

import "sync/atomic"

// Bool is an atomic Boolean.
type Bool int32

// Set sets the Boolean to value.
func (b *Bool) Set(value bool) {
	var i int32 = 0
	if value {
		i = 1
	}

	atomic.StoreInt32((*int32)(b), int32(i))
}

// Get gets the Boolean value.
func (b *Bool) Get() bool {
	return atomic.LoadInt32((*int32)(b)) != 0
}
