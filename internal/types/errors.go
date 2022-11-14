package types

import "fmt"

type Error struct {
	Code    string
	Context map[string]any
}

func (err Error) Error() string {
	return fmt.Sprintf("%+v: %+v", err.Code, err.Context)
}

func NewError(code string, args ...any) Error {
	n := len(args)
	if n%2 != 0 {
		panic("Invalid error context args")
	}
	err := Error{Code: code, Context: make(map[string]any, n/2)}
	for i := 0; i < n; i += 2 {
		s, ok := args[i].(string)
		if !ok {
			panic("Invalid error context args")
		}
		err.Context[s] = args[i+1]
	}
	return err
}
