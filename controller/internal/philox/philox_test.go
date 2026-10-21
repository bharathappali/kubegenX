package philox

import "testing"

func TestPhiloxVector(t *testing.T) {
	// Frozen so the Rust worker can assert the same words.
	got := Four(0, 0, [4]uint32{0, 0, 0, 0})
	want := [4]uint32{0x6627e8d5, 0xe169c58d, 0xbc57ac4c, 0x9b00dbd8}
	if got != want {
		t.Fatalf("philox %08x %08x %08x %08x", got[0], got[1], got[2], got[3])
	}
	if U32(424242, 1, 7, 0, 0, 0) != 2916870100 {
		t.Fatal(U32(424242, 1, 7, 0, 0, 0))
	}
	if U32(424242, 1, 7, 1, 3, 2) != 3711581225 {
		t.Fatal("second vector")
	}
}

func TestDeterministic(t *testing.T) {
	a := U32(7, 2, 9, 3, 100, 1)
	b := U32(7, 2, 9, 3, 100, 1)
	if a != b {
		t.Fatalf("%d != %d", a, b)
	}
	if U32(7, 2, 9, 3, 100, 2) == a && U32(8, 2, 9, 3, 100, 1) == a {
		t.Fatal("different addresses collided")
	}
}
