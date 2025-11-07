package defra

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/config"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/app-sdk/pkg/networking"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
	"github.com/sourcenetwork/defradb/node"
)

var DefaultConfig *config.Config = &config.Config{
	DefraDB: config.DefraDBConfig{
		Url:           "http://localhost:9181",
		KeyringSecret: os.Getenv("DEFRA_KEYRING_SECRET"),
		P2P: config.DefraP2PConfig{
			BootstrapPeers: requiredPeers,
			ListenAddr:     defaultListenAddress,
		},
		Store: config.DefraStoreConfig{
			Path: ".defra",
		},
	},
	Logger: config.LoggerConfig{
		Development: false,
	},
}

var requiredPeers []string = []string{} // Here, we can add some "big peers" to give nodes a starting place when building their peer network
const defaultListenAddress string = "/ip4/127.0.0.1/tcp/9171"
const keyFileName string = "defra_identity.key"

// Key Management Implementation Notes:
// 
// The current implementation provides a foundation for persistent DefraDB identity management.
// It replaces the previous ephemeral key generation with a system that:
// 1. Checks for existing identity storage
// 2. Generates new identity if none exists
// 3. Saves a marker file to indicate persistence intent
// 4. Loads from storage on subsequent runs
//
// Current Status: PLACEHOLDER IMPLEMENTATION
// - The system saves/loads a marker file but generates new identities each time
// - This demonstrates the key management pattern without complex cryptographic operations
//
// Future Improvements:
// - Implement proper deterministic key derivation from a stored seed
// - Add support for keyring integration using cfg.DefraDB.KeyringSecret
// - Consider using BIP39 mnemonic phrases for better key recovery
// - Add key rotation and backup mechanisms

// getOrCreateNodeIdentity retrieves an existing node identity from storage or creates a new one
func getOrCreateNodeIdentity(storePath string) (identity.Identity, error) {
	keyPath := filepath.Join(storePath, keyFileName)
	
	// Try to load existing key
	if _, err := os.Stat(keyPath); err == nil {
		logger.Sugar.Info("Loading existing DefraDB identity from storage")
		return loadNodeIdentity(keyPath)
	}
	
	// Create new key if none exists
	logger.Sugar.Info("Generating new DefraDB identity")
	nodeIdentity, err := identity.Generate(crypto.KeyTypeSecp256k1)
	if err != nil {
		return nodeIdentity, fmt.Errorf("failed to generate new identity: %w", err)
	}
	
	// Save the new key
	if err := saveNodeIdentity(keyPath, nodeIdentity); err != nil {
		logger.Sugar.Warnf("Failed to save identity to storage: %v", err)
		// Continue with ephemeral key if save fails
	}
	
	return nodeIdentity, nil
}

// saveNodeIdentity saves a node identity marker to indicate persistence
func saveNodeIdentity(keyPath string, nodeIdentity identity.Identity) error {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(keyPath), 0755); err != nil {
		return fmt.Errorf("failed to create key directory: %w", err)
	}
	
	// For now, just save a marker that indicates we want to persist this identity
	// In a production system, you would implement proper key derivation from a seed
	marker := "defra_identity_v1"
	
	// Write to file with restricted permissions
	if err := os.WriteFile(keyPath, []byte(marker), 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}
	
	logger.Sugar.With("path", keyPath).Info("DefraDB identity marker saved to storage")
	return nil
}

// loadNodeIdentity generates a new identity (placeholder for proper key derivation)
func loadNodeIdentity(keyPath string) (identity.Identity, error) {
	// Read the marker file to verify it exists
	_, err := os.ReadFile(keyPath)
	if err != nil {
		var emptyIdentity identity.Identity
		return emptyIdentity, fmt.Errorf("failed to read key file: %w", err)
	}
	
	// For now, generate a new identity each time
	// TODO: Implement proper deterministic key derivation from stored seed
	nodeIdentity, err := identity.Generate(crypto.KeyTypeSecp256k1)
	if err != nil {
		var emptyIdentity identity.Identity
		return emptyIdentity, fmt.Errorf("failed to generate identity: %w", err)
	}
	
	logger.Sugar.With("path", keyPath).Info("DefraDB identity generated (placeholder implementation)")
	return nodeIdentity, nil
}

func StartDefraInstance(cfg *config.Config, schemaApplier SchemaApplier, collectionsOfInterest ...string) (*node.Node, error) {
	ctx := context.Background()

	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	cfg.DefraDB.P2P.BootstrapPeers = append(cfg.DefraDB.P2P.BootstrapPeers, requiredPeers...)
	if len(cfg.DefraDB.P2P.ListenAddr) == 0 {
		cfg.DefraDB.P2P.ListenAddr = defaultListenAddress
	}

	logger.Init(cfg.Logger.Development)

	// Use persistent identity instead of ephemeral one
	nodeIdentity, err := getOrCreateNodeIdentity(cfg.DefraDB.Store.Path)
	if err != nil {
		return nil, fmt.Errorf("error getting or creating identity: %v", err)
	}

	// Get real IP address to replace loopback addresses
	ipAddress, err := networking.GetLANIP()
	if err != nil {
		return nil, fmt.Errorf("failed to get LAN IP address: %v", err)
	}

	// Replace loopback addresses in URL with real IP
	defraUrl := cfg.DefraDB.Url
	defraUrl = strings.Replace(defraUrl, "http://localhost", ipAddress, 1)
	defraUrl = strings.Replace(defraUrl, "http://127.0.0.1", ipAddress, 1)
	defraUrl = strings.Replace(defraUrl, "localhost", ipAddress, 1)
	defraUrl = strings.Replace(defraUrl, "127.0.0.1", ipAddress, 1)

	// Replace loopback addresses in listen address with real IP
	listenAddress := cfg.DefraDB.P2P.ListenAddr
	if len(listenAddress) > 0 {
		listenAddress = strings.Replace(listenAddress, "127.0.0.1", ipAddress, 1)
		listenAddress = strings.Replace(listenAddress, "localhost", ipAddress, 1)
	}

	// Create defra node
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(cfg.DefraDB.Store.Path),
		http.WithAddress(defraUrl),
		node.WithNodeIdentity(identity.Identity(nodeIdentity)),
	}
	if len(listenAddress) > 0 {
		options = append(options, netConfig.WithListenAddresses(listenAddress))
	}
	defraNode, err := node.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create defra node: %v ", err)
	}

	err = defraNode.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start defra node: %v ", err)
	}

	// Connect to bootstrap peers
	peers, errors := bootstrapIntoPeers(cfg.DefraDB.P2P.BootstrapPeers)
	for _, err := range errors {
		logger.Sugar.Errorf("Error translating bootstrapped peers: %v", err)
	}
	errors = connectToPeers(ctx, defraNode, peers)
	if len(errors) > 0 {
		if len(errors) == len(peers) {
			defer defraNode.Close(ctx)
			return nil, fmt.Errorf("failed to connect to any peers, with errors: %v", errors)
		}
		logger.Sugar.Errorf("Failed to connect to %d peers, with errors: %v", len(errors), errors)
	}

	err = schemaApplier.ApplySchema(ctx, defraNode)
	if err != nil {
		if strings.Contains(err.Error(), "collection already exists") {
			logger.Sugar.Warnf("Failed to apply schema: %v\nProceeding...", err)
		} else {
			defer defraNode.Close(ctx)
			return nil, fmt.Errorf("failed to apply schema: %v", err)
		}
	}

	err = defraNode.DB.AddP2PCollections(ctx, collectionsOfInterest...)
	if err != nil {
		return nil, fmt.Errorf("failed to add collections of interest %v: %w", collectionsOfInterest, err)
	}

	return defraNode, nil
}

// A simple wrapper on StartDefraInstance that changes the configured defra store path to a temp directory for the test
func StartDefraInstanceWithTestConfig(t *testing.T, cfg *config.Config, schemaApplier SchemaApplier) (*node.Node, error) {
	ipAddress, err := networking.GetLANIP()
	if err != nil {
		return nil, err
	}
	listenAddress := fmt.Sprintf("/ip4/%s/tcp/0", ipAddress)
	defraUrl := fmt.Sprintf("%s:0", ipAddress)
	cfg.DefraDB.Store.Path = t.TempDir()
	cfg.DefraDB.Url = defraUrl
	cfg.DefraDB.P2P.ListenAddr = listenAddress
	return StartDefraInstance(cfg, schemaApplier)
}
