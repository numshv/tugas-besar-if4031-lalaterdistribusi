package envx

import (
	"os"
	"strconv"
	"strings"
	"time"
)

func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func Int(key string, def int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return def
	}
	return v
}

func Seconds(key string, def int) time.Duration {
	return time.Duration(Int(key, def)) * time.Second
}

func Millis(key string, def int) time.Duration {
	return time.Duration(Int(key, def)) * time.Millisecond
}

func List(key string) []string {
	return SplitList(os.Getenv(key))
}

func SplitList(raw string) []string {
	var out []string
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
