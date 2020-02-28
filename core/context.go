package core

// CancelFuncKey is the key name of the cancel function in context
const CancelFuncKey = contextKey("cancelFunc")

type contextKey string

func (c contextKey) String() string {
	return "s5cmd context key " + string(c)
}
