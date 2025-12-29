package main

type exitCodeError struct {
	code  int
	msg   string
	quiet bool
}

func (e *exitCodeError) Error() string {
	return e.msg
}

func (e *exitCodeError) ExitCode() int {
	return e.code
}

func (e *exitCodeError) Quiet() bool {
	return e.quiet
}
