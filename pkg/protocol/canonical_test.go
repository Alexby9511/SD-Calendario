package protocol

import "testing"

func TestCanonicalizeSortsKeysAndDropsWhitespace(t *testing.T) {
	in := []byte(`{ "b": 2, "a": [ 3, { "z": 1, "y": 2 } ] }`)
	got, err := Canonicalize(in)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":[3,{"y":2,"z":1}],"b":2}`
	if string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestCanonicalizeIsIdempotent(t *testing.T) {
	in := []byte(`{"a":1,"b":"x"}`)
	once, err := Canonicalize(in)
	if err != nil {
		t.Fatal(err)
	}
	twice, err := Canonicalize(once)
	if err != nil {
		t.Fatal(err)
	}
	if string(once) != string(twice) {
		t.Fatalf("%s != %s", once, twice)
	}
}

func TestCanonicalizeRejectsInvalidJSON(t *testing.T) {
	if _, err := Canonicalize([]byte(`{"a":`)); err == nil {
		t.Fatal("accepted invalid JSON")
	}
}

func TestIsULID(t *testing.T) {
	cases := map[string]bool{
		testULID:                      true,
		"01J9Z3K6Y7Q8R9S0T1V2W3X4Y":   false, // too short
		"01J9Z3K6Y7Q8R9S0T1V2W3X4Y5Z": false, // too long
		"81J9Z3K6Y7Q8R9S0T1V2W3X4Y5":  false, // overflows 128 bits
		"01J9Z3K6Y7Q8R9S0T1V2W3X4YI":  false, // I is not in the alphabet
	}
	for in, want := range cases {
		if got := IsULID(in); got != want {
			t.Errorf("IsULID(%q) = %v, want %v", in, got, want)
		}
	}
}
