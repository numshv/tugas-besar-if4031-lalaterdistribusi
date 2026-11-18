package envx

import (
	"strings"
	"testing"
	"time"
)

func TestStringAndInt(t *testing.T) {
	t.Setenv("ENVX_S", "")
	t.Setenv("ENVX_N", "42")
	t.Setenv("ENVX_BAD", "4x")
	if String("ENVX_S", "def") != "def" || String("ENVX_N", "def") != "42" {
		t.Fatal("String must fall back only when empty")
	}
	if Int("ENVX_N", 1) != 42 || Int("ENVX_BAD", 7) != 7 || Int("ENVX_MISSING", 9) != 9 {
		t.Fatal("Int must fall back when empty or not a number")
	}
	if Seconds("ENVX_N", 1) != 42*time.Second || Millis("ENVX_MISSING", 250) != 250*time.Millisecond {
		t.Fatal("durations")
	}
}

func TestList(t *testing.T) {
	t.Setenv("ENVX_L", " kafka-0:9092 , ,kafka-1:9092,")
	if got := strings.Join(List("ENVX_L"), "|"); got != "kafka-0:9092|kafka-1:9092" {
		t.Fatalf("List = %q", got)
	}
	t.Setenv("ENVX_L", "")
	if got := List("ENVX_L"); len(got) != 0 {
		t.Fatalf("empty env must give no items, got %q", got)
	}
}
