package errf

import "errors"

var ErrSpinStuck = errors.New("the spinlock got stuck")
