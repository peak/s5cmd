# unparam

	go install mvdan.cc/unparam@latest

Reports unused function parameters and results in your code.

To minimise false positives, it ignores certain cases such as:

* Exported functions (by default, see `-exported`)
* Unnamed and underscore parameters (like `_` and `_foo`)
* Funcs that may satisfy an interface
* Funcs that may satisfy a function signature
* Funcs that are stubs (empty, only error, immediately return, etc)
* Funcs that have multiple implementations via build tags

It also reports results that always return the same value, parameters
that always receive the same value, and results that are never used. In
the last two cases, a minimum number of calls is required to ensure that
the warnings are useful.

False positives can still occur by design. The aim of the tool is to be
as precise as possible - if you find any mistakes, file a bug.
