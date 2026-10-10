package hazard

import "github.com/google/uuid"

// Namespace tetap untuk seluruh sistem BNPB. JANGAN diubah setelah dipakai,
// karena mengubahnya berarti hazard_id yang sama akan menghasilkan ID
// berbeda dan upsert akan dianggap record baru (duplikat).
var idNamespace = uuid.MustParse("6ba7b813-9dad-11d1-80b4-00c04fd430c8")

// NewID membangkitkan hazard_id yang deterministik dari source dan referensi
// asli di sumbernya. Deterministik berarti re-poll data yang sama selalu
// menghasilkan ID yang sama, sehingga upsert ke Canonical Store idempoten
// by design, bukan bergantung pada logic pengecekan manual di lapisan lain.
func NewID(source Source, sourceRefID string) string {
	name := string(source) + ":" + sourceRefID
	return uuid.NewSHA1(idNamespace, []byte(name)).String()
}
