// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fileop

import (
	"fmt"
)

// ErrorFOP describe an error to a specific operation index
type ErrorFOP struct {
	Err            error
	Fbo            *FileBatchOp
	OperationIndex int
}

// ErrorFOPList is a list of *Errors.
// The zero value for an ErrorFOPList is an empty ErrorFOPList ready to use.
//
type ErrorFOPList []ErrorFOP

// Add adds an Error with given position and error message to an ErrorFOPList.
func (p *ErrorFOPList) Add(err ErrorFOP) {
	*p = append(*p, err)
}

// Reset resets an ErrorFOPList to no errors.
func (p *ErrorFOPList) Reset() { *p = (*p)[0:0] }

// Len of the list
func (p ErrorFOPList) Len() int { return len(p) }

// GetErrorForFop get the ErrorFOP corresponding to a FileBatchOp
func (p ErrorFOPList) GetErrorForFop(fbo *FileBatchOp) *ErrorFOP {
	for _, v := range p {
		if v.Fbo == fbo {
			return &v
		}
	}
	return nil
}

// An ErrorFOPList implements the error interface.
func (p ErrorFOPList) Error() string {
	switch len(p) {
	case 0:
		return "no errors"
	case 1:
		return p[0].Err.Error()
	}
	return fmt.Sprintf("%s (and %d more errors)", p[0].Err.Error(), len(p)-1)
}

// Err returns an error equivalent to this error list.
// If the list is empty, Err returns nil.
func (p ErrorFOPList) Err() error {
	if len(p) == 0 {
		return nil
	}
	return p
}
