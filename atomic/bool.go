package atomic

import "sync/atomic"

// Bool is an atomic Boolean.
type Bool int32

// Set sets the Boolean to value.
func (ab *Bool) Set(value bool) {
	var i int32 = 0
	if value {
		i = 1
	}

	atomic.StoreInt32((*int32)(ab), int32(i))
}

// Get gets the Boolean value.
func (ab *Bool) Get() bool {
	if atomic.LoadInt32((*int32)(ab)) != 0 {
		return true
	}

	return false
}
