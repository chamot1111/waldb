// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wutils

import (
	"fmt"
	"io"
)

// ErrorList is a list of *Errors.
// The zero value for an ErrorList is an empty ErrorList ready to use.
//
type ErrorList []error

// Add adds an Error with given position and error message to an ErrorList.
func (p *ErrorList) Add(err error) {
	if err == nil {
		return
	}
	*p = append(*p, err)
}

// Reset resets an ErrorList to no errors.
func (p *ErrorList) Reset() { *p = (*p)[0:0] }

// Len of the list
func (p ErrorList) Len() int { return len(p) }

// An ErrorList implements the error interface.
func (p ErrorList) Error() string {
	switch len(p) {
	case 0:
		return "no errors"
	case 1:
		return p[0].Error()
	}
	return fmt.Sprintf("%s (and %d more errors)", p[0], len(p)-1)
}

// Err returns an error equivalent to this error list.
// If the list is empty, Err returns nil.
func (p ErrorList) Err() error {
	if len(p) == 0 {
		return nil
	}
	return p
}

// PrintError is a utility function that prints a list of errors to w,
// one error per line, if the err parameter is an ErrorList. Otherwise
// it prints the err string.
//
func PrintError(w io.Writer, err error) {
	if list, ok := err.(ErrorList); ok {
		for _, e := range list {
			fmt.Fprintf(w, "%s\n", e)
		}
	} else if err != nil {
		fmt.Fprintf(w, "%s\n", err)
	}
}
