package access

import (
	"fmt"
	"testing"
)

func TestAlignLength(t *testing.T) {
	fmt.Printf("%d\n", MaxAlign(7))
	fmt.Printf("%d\n", MaxAlign(9))
	fmt.Printf("%d\n", MaxAlign(21))
}
