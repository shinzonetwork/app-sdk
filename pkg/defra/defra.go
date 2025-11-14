package defra

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/shinzonetwork/app-sdk/pkg/config"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/shinzonetwork/app-sdk/pkg/networking"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/http"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/go-p2p"
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
// This implementation provides persistent DefraDB identity management by:
// 1. Extracting private key bytes from generated FullIdentity
// 2. Storing the raw key bytes as hex-encoded strings in secure files (0600 permissions)
// 3. Reconstructing the same identity from stored private key bytes on subsequent runs
// 4. Ensuring the same cryptographic identity is used across application restarts
//
// Current Status: FULLY FUNCTIONAL
// - Private keys are properly extracted and stored
// - Identities are reconstructed from stored keys, maintaining consistency
// - File permissions are secure (0600)
// - Comprehensive error handling and logging
//
// Security Features:
// - Keys stored in DefraDB store directory (.defra/defra_identity.key)
// - File permissions restricted to owner only (0600)
// - Hex encoding for safe text storage
// - Proper error handling for corrupted or missing key files
//
// Future Enhancements:
// - Add support for keyring integration using cfg.DefraDB.KeyringSecret
// - Consider key rotation and backup mechanisms
// - Add optional encryption of stored key files

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

// saveNodeIdentity saves the private key bytes of a node identity for persistence
func saveNodeIdentity(keyPath string, nodeIdentity identity.Identity) error {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(keyPath), 0755); err != nil {
		return fmt.Errorf("failed to create key directory: %w", err)
	}

	// Cast to FullIdentity to access private key
	fullIdentity, ok := nodeIdentity.(identity.FullIdentity)
	if !ok {
		return fmt.Errorf("identity is not a FullIdentity, cannot extract private key")
	}

	// Get the private key from the identity
	privateKey := fullIdentity.PrivateKey()
	if privateKey == nil {
		return fmt.Errorf("failed to get private key from identity")
	}

	// Get raw key bytes
	keyBytes := privateKey.Raw()
	if len(keyBytes) == 0 {
		return fmt.Errorf("private key has no raw bytes")
	}

	// Encode as hex string for storage
	keyHex := hex.EncodeToString(keyBytes)

	// Write to file with restricted permissions
	if err := os.WriteFile(keyPath, []byte(keyHex), 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}

	logger.Sugar.With("path", keyPath).Info("DefraDB identity private key saved to storage")
	return nil
}

// loadNodeIdentity loads a node identity from stored private key bytes
func loadNodeIdentity(keyPath string) (identity.Identity, error) {
	// Read the stored key file
	keyHex, err := os.ReadFile(keyPath)
	if err != nil {
		var emptyIdentity identity.Identity
		return emptyIdentity, fmt.Errorf("failed to read key file: %w", err)
	}

	// Decode hex string to bytes
	keyBytes, err := hex.DecodeString(string(keyHex))
	if err != nil {
		var emptyIdentity identity.Identity
		return emptyIdentity, fmt.Errorf("failed to decode key hex: %w", err)
	}

	// Reconstruct private key from bytes
	privateKey, err := crypto.PrivateKeyFromBytes(crypto.KeyTypeSecp256k1, keyBytes)
	if err != nil {
		var emptyIdentity identity.Identity
		return emptyIdentity, fmt.Errorf("failed to reconstruct private key: %w", err)
	}

	// Reconstruct identity from private key
	fullIdentity, err := identity.FromPrivateKey(privateKey)
	if err != nil {
		var emptyIdentity identity.Identity
		return emptyIdentity, fmt.Errorf("failed to reconstruct identity from private key: %w", err)
	}

	logger.Sugar.With("path", keyPath).Info("DefraDB identity successfully loaded from storage")
	return fullIdentity, nil
}

// createLibP2PKeyFromIdentity creates a LibP2P private key from a DefraDB identity
// This ensures the LibP2P peer ID is deterministically derived from the same identity
func createLibP2PKeyFromIdentity(nodeIdentity identity.Identity) (libp2pcrypto.PrivKey, error) {
	// Cast to FullIdentity to access private key
	fullIdentity, ok := nodeIdentity.(identity.FullIdentity)
	if !ok {
		return nil, fmt.Errorf("identity is not a FullIdentity, cannot extract private key")
	}

	// Get the private key from the identity
	privateKey := fullIdentity.PrivateKey()
	if privateKey == nil {
		return nil, fmt.Errorf("failed to get private key from identity")
	}

	// Get raw key bytes
	keyBytes := privateKey.Raw()
	if len(keyBytes) == 0 {
		return nil, fmt.Errorf("private key has no raw bytes")
	}

	// DefraDB expects Ed25519 keys, but DefraDB identities use secp256k1
	// We need to derive an Ed25519 key deterministically from the secp256k1 key
	// Use the secp256k1 key bytes as seed for Ed25519 key generation
	if len(keyBytes) != 32 {
		return nil, fmt.Errorf("expected 32-byte secp256k1 key, got %d bytes", len(keyBytes))
	}

	// Generate Ed25519 key from secp256k1 seed
	libp2pPrivKey, _, err := libp2pcrypto.GenerateEd25519Key(strings.NewReader(string(keyBytes)))
	if err != nil {
		return nil, fmt.Errorf("failed to generate Ed25519 key from identity seed: %w", err)
	}

	return libp2pPrivKey, nil
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

	// Create LibP2P private key from the same identity to ensure consistent peer ID
	libp2pPrivKey, err := createLibP2PKeyFromIdentity(nodeIdentity)
	if err != nil {
		return nil, fmt.Errorf("error creating LibP2P private key from identity: %v", err)
	}

	// Get raw bytes for P2P private key configuration (DefraDB 0.20 API TBD)
	libp2pKeyBytes, err := libp2pPrivKey.Raw()
	if err != nil {
		return nil, fmt.Errorf("error getting LibP2P private key bytes: %v", err)
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

	// Create defra node options
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false), // Enable P2P networking
		node.WithStorePath(cfg.DefraDB.Store.Path),
		http.WithAddress(defraUrl),
		node.WithNodeIdentity(identity.Identity(nodeIdentity)),
	}
	
	// Add P2P configuration options - DefraDB 0.20 accepts go-p2p NodeOpt as node.Option
	// This ensures consistent peer ID by using our persistent private key
	if len(listenAddress) > 0 {
		options = append(options, p2p.WithListenAddresses(listenAddress))
		logger.Sugar.Infof("P2P Listen Address configured: %s", listenAddress)
	}
	
	if len(libp2pKeyBytes) > 0 {
		options = append(options, p2p.WithPrivateKey(libp2pKeyBytes))
		logger.Sugar.Info("P2P Private Key configured for consistent peer ID")
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
	err = connectToPeers(ctx, defraNode, cfg.DefraDB.P2P.BootstrapPeers)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to any peers, with error: %w", err)
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
func StartDefraInstanceWithTestConfig(t *testing.T, cfg *config.Config, schemaApplier SchemaApplier, collectionsOfInterest ...string) (*node.Node, error) {
	ipAddress, err := networking.GetLANIP()
	if err != nil {
		return nil, err
	}
	listenAddress := fmt.Sprintf("/ip4/%s/tcp/0", ipAddress)
	defraUrl := fmt.Sprintf("%s:0", ipAddress)
	if cfg == nil {
		cfg = DefaultConfig
	}
	cfg.DefraDB.Store.Path = t.TempDir()
	cfg.DefraDB.Url = defraUrl
	cfg.DefraDB.P2P.ListenAddr = listenAddress
	return StartDefraInstance(cfg, schemaApplier, collectionsOfInterest...)
}
