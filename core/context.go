package core

// ExitFuncKey is the key name of the exit function in context
const ExitFuncKey = contextKey("exitFunc")

// CancelFuncKey is the key name of the cancel function in context
const CancelFuncKey = contextKey("cancelFunc")

type contextKey string

func (c contextKey) String() string {
	return "s5cmd context key " + string(c)
}
