package telemetry

import "testing"

func TestDeploymentOf(t *testing.T) {
	for pod, want := range map[string]string{
		"aggregator-7dc6f586c5-6zfnc":         "aggregator",
		"consumer-dashboard-7b8ff7bf7c-8kbfq": "consumer-dashboard",
		"laptop":                              "laptop",
	} {
		if got := deploymentOf(pod); got != want {
			t.Errorf("deploymentOf(%q) = %q, want %q", pod, got, want)
		}
	}
}

func TestServiceNamePrefersEnv(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "client-api")
	if got := serviceName(); got != "client-api" {
		t.Fatalf("serviceName() = %q, want client-api", got)
	}
}
