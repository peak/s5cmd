package parallel

import "testing"

func TestRunPanicsWithoutInit(t *testing.T) {
	// Ensure global is nil so Run will panic.
	old := global
	global = nil
	defer func() { global = old }()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Run() did not panic")
		}
		got, ok := r.(string)
		if !ok {
			t.Fatalf("Run() panicked with non-string value: %v", r)
		}
		want := "parallel: Run called before Init"
		if got != want {
			t.Errorf("Run() panic message = %q, want %q", got, want)
		}
	}()

	Run(nil, nil)
}
