package jserror

import (
	"fmt"
	"os"
)

type ListAddFunc func(item []byte) error

func ErrExit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
