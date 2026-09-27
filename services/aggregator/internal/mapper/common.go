package mapper

import (
	"fmt"
	"time"
)

func reqString(m map[string]any, key string) (string, error) {
	v, ok := m[key].(string)
	if !ok || v == "" {
		return "", fmt.Errorf("field %q missing or not a string", key)
	}
	return v, nil
}

// reqFloat: encoding/json men-decode SEMUA angka JSON sebagai float64 saat
// target-nya map[string]any. Ini bukan bug, ini konsekuensi memilih parsing
// longgar demi P1 — konversi ke int (kalau perlu) dilakukan eksplisit di
// pemanggil, bukan di sini.
func reqFloat(m map[string]any, key string) (float64, error) {
	v, ok := m[key].(float64)
	if !ok {
		return 0, fmt.Errorf("field %q missing or not a number", key)
	}
	return v, nil
}

func reqTime(m map[string]any, key string) (time.Time, error) {
	raw, ok := m[key].(string)
	if !ok {
		return time.Time{}, fmt.Errorf("field %q missing or not a string timestamp", key)
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("field %q is not RFC3339: %w", key, err)
	}
	return t.UTC(), nil
}

// attributesOf adalah SATU-SATUNYA tempat kebijakan P1 diterapkan. Field
// yang tidak ada di `known` otomatis ikut, termasuk field yang baru muncul
// setelah PVMBG memanggil /admin/schema-version — tidak ada logic khusus
// yang perlu ditambah saat itu terjadi.
func attributesOf(raw map[string]any, known map[string]struct{}, policy UnknownFieldPolicy) map[string]any {
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		if _, isKnown := known[k]; isKnown {
			continue
		}
		if policy == PolicyDrop {
			continue
		}
		out[k] = v
	}
	return out
}
