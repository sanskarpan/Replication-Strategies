package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config mirrors config.yaml. Missing fields fall back to Defaults().
type Config struct {
	Server struct {
		Port        int      `yaml:"port"`
		CORSOrigins []string `yaml:"cors_origins"`
	} `yaml:"server"`
	Simulation struct {
		DefaultLagThresholdMs int `yaml:"default_lag_threshold_ms"`
		HeartbeatIntervalMs   int `yaml:"heartbeat_interval_ms"`
		MaxClusters           int `yaml:"max_clusters"`
	} `yaml:"simulation"`
}

// Defaults returns the built-in configuration used when config.yaml is absent or
// a field is unset.
func Defaults() *Config {
	c := &Config{}
	c.Server.Port = 8080
	c.Simulation.MaxClusters = 10
	c.Simulation.DefaultLagThresholdMs = 100
	c.Simulation.HeartbeatIntervalMs = 50
	return c
}

// Load reads config from path, layering it over Defaults(). A missing file is not
// an error — the defaults are returned so the server always starts.
func Load(path string) (*Config, error) {
	cfg := Defaults()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return cfg, err
	}
	// Re-apply defaults for any numeric field left at zero after unmarshalling (an
	// omitted key and an explicit 0 are indistinguishable in yaml.v3).
	d := Defaults()
	if cfg.Server.Port == 0 {
		cfg.Server.Port = d.Server.Port
	}
	if cfg.Simulation.MaxClusters == 0 {
		cfg.Simulation.MaxClusters = d.Simulation.MaxClusters
	}
	if cfg.Simulation.DefaultLagThresholdMs == 0 {
		cfg.Simulation.DefaultLagThresholdMs = d.Simulation.DefaultLagThresholdMs
	}
	if cfg.Simulation.HeartbeatIntervalMs == 0 {
		cfg.Simulation.HeartbeatIntervalMs = d.Simulation.HeartbeatIntervalMs
	}
	return cfg, nil
}
