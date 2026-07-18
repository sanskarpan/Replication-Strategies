package config

import "testing"

func TestApplyEnvOverrides(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("MAX_CLUSTERS", "3")
	t.Setenv("CORS_ORIGINS", "http://a.com, http://b.com ,")

	c := Defaults()
	c.ApplyEnvOverrides()

	if c.Server.Port != 9090 {
		t.Fatalf("PORT override failed: got %d", c.Server.Port)
	}
	if c.Simulation.MaxClusters != 3 {
		t.Fatalf("MAX_CLUSTERS override failed: got %d", c.Simulation.MaxClusters)
	}
	if len(c.Server.CORSOrigins) != 2 || c.Server.CORSOrigins[0] != "http://a.com" || c.Server.CORSOrigins[1] != "http://b.com" {
		t.Fatalf("CORS_ORIGINS override/trim failed: %#v", c.Server.CORSOrigins)
	}
}

func TestValidate(t *testing.T) {
	ok := Defaults()
	if err := ok.Validate(); err != nil {
		t.Fatalf("defaults should validate, got %v", err)
	}
	bad := Defaults()
	bad.Server.Port = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("port 0 must fail validation")
	}
	bad2 := Defaults()
	bad2.Simulation.MaxClusters = -1
	if err := bad2.Validate(); err == nil {
		t.Fatal("negative max_clusters must fail validation")
	}
}
