package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

const CollectionName = "shinzo"

type Config struct {
	DefraDB DefraDBConfig `yaml:"defradb"`
	Shinzo  ShinzoConfig  `yaml:"shinzo"`
	Logger  LoggerConfig  `yaml:"logger"`
}

type DefraDBConfig struct {
	Url           string           `yaml:"url"`
	KeyringSecret string           `yaml:"keyring_secret"`
	P2P           DefraP2PConfig   `yaml:"p2p"`
	Store         DefraStoreConfig `yaml:"store"`
}

type DefraP2PConfig struct {
	BootstrapPeers []string `yaml:"bootstrap_peers"`
	ListenAddr     string   `yaml:"listen_addr"`
}

type DefraStoreConfig struct {
	Path string `yaml:"path"`
}

type ShinzoConfig struct {
	MinimumAttestations string `yaml:"minimum_attestations"`
}

type LoggerConfig struct {
	Development bool `yaml:"development"`
}

// LoadConfig loads configuration from a YAML file and environment variables
func LoadConfig(path string) (*Config, error) {
	// Load .env file if it exists
	_ = godotenv.Load()

	// Load YAML config
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Override with environment variables
	if keyringSecret := os.Getenv("DEFRA_KEYRING_SECRET"); keyringSecret != "" {
		cfg.DefraDB.KeyringSecret = keyringSecret
	}

	if url := os.Getenv("DEFRA_URL"); url != "" {
		cfg.DefraDB.Url = url
	}

	return &cfg, nil
}

// GetMinimumAttestations returns the minimum attestations threshold as a uint.
// Returns 0 if the value is not set or cannot be parsed.
func (c *Config) GetMinimumAttestations() uint {
	if c == nil || c.Shinzo.MinimumAttestations == "" {
		return 0
	}
	
	var threshold uint
	_, err := fmt.Sscanf(c.Shinzo.MinimumAttestations, "%d", &threshold)
	if err != nil {
		return 0
	}
	return threshold
}
