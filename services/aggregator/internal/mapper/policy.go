package mapper

type UnknownFieldPolicy string

const (
	PolicyPassthrough UnknownFieldPolicy = "passthrough"
	PolicyDrop        UnknownFieldPolicy = "drop"
)

func ParsePolicy(s string) UnknownFieldPolicy {
	if UnknownFieldPolicy(s) == PolicyDrop {
		return PolicyDrop
	}
	return PolicyPassthrough
}
