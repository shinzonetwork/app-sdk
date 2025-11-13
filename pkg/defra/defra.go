package defra

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
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
const peerKeyFileName string = "defra_peer.key"

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
// - Keys stored in DefraDB store directory (.defra/defra_identity.key and .defra/defra_peer.key)
// - File permissions restricted to owner only (0600)
// - Hex encoding for safe text storage
// - Proper error handling for corrupted or missing key files
//
// Future Enhancements:
// - Add support for keyring integration using cfg.DefraDB.KeyringSecret
// - Consider key rotation and backup mechanisms
// - Add optional encryption of stored key files

// NodeKeys holds both the node identity and P2P peer key
type NodeKeys struct {
	Identity identity.FullIdentity
	PeerKey  ed25519.PrivateKey
}

// getOrCreateNodeIdentity retrieves existing node identity and peer key from storage or creates new ones
func getOrCreateNodeIdentity(storePath string) (NodeKeys, error) {
	identityKeyPath := filepath.Join(storePath, keyFileName)
	peerKeyPath := filepath.Join(storePath, peerKeyFileName)

	var nodeIdentity identity.FullIdentity
	var peerKey ed25519.PrivateKey

	// Try to load existing node identity
	if _, err := os.Stat(identityKeyPath); err == nil {
		logger.Sugar.Info("Loading existing DefraDB identity from storage")
		nodeIdentity, err = loadNodeIdentity(identityKeyPath)
		if err != nil {
			return NodeKeys{}, fmt.Errorf("failed to load node identity: %w", err)
		}
	} else {
		// Create new node identity if none exists
		logger.Sugar.Info("Generating new DefraDB identity")
		nodeIdentity, err = identity.Generate(crypto.KeyTypeSecp256k1)
		if err != nil {
			return NodeKeys{}, fmt.Errorf("failed to generate new identity: %w", err)
		}

		// Save the new node identity
		if err := saveNodeIdentity(identityKeyPath, nodeIdentity); err != nil {
			logger.Sugar.Warnf("Failed to save identity to storage: %v", err)
			// Continue with ephemeral key if save fails
		}
	}

	// Try to load existing peer key
	if _, err := os.Stat(peerKeyPath); err == nil {
		logger.Sugar.Info("Loading existing P2P peer key from storage")
		peerKey, err = loadPeerKey(peerKeyPath)
		if err != nil {
			return NodeKeys{}, fmt.Errorf("failed to load peer key: %w", err)
		}
	} else {
		// Create new peer key if none exists
		logger.Sugar.Info("Generating new P2P peer key")
		peerKey, err = crypto.GenerateEd25519()
		if err != nil {
			return NodeKeys{}, fmt.Errorf("failed to generate new peer key: %w", err)
		}

		// Save the new peer key
		if err := savePeerKey(peerKeyPath, peerKey); err != nil {
			logger.Sugar.Warnf("Failed to save peer key to storage: %v", err)
			// Continue with ephemeral key if save fails
		}
	}

	return NodeKeys{
		Identity: nodeIdentity,
		PeerKey:  peerKey,
	}, nil
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
func loadNodeIdentity(keyPath string) (identity.FullIdentity, error) {
	// Read the stored key file
	keyHex, err := os.ReadFile(keyPath)
	if err != nil {
		var emptyIdentity identity.FullIdentity
		return emptyIdentity, fmt.Errorf("failed to read key file: %w", err)
	}

	// Decode hex string to bytes
	keyBytes, err := hex.DecodeString(string(keyHex))
	if err != nil {
		var emptyIdentity identity.FullIdentity
		return emptyIdentity, fmt.Errorf("failed to decode key hex: %w", err)
	}

	// Reconstruct private key from bytes
	privateKey, err := crypto.PrivateKeyFromBytes(crypto.KeyTypeSecp256k1, keyBytes)
	if err != nil {
		var emptyIdentity identity.FullIdentity
		return emptyIdentity, fmt.Errorf("failed to reconstruct private key: %w", err)
	}

	// Reconstruct identity from private key
	fullIdentity, err := identity.FromPrivateKey(privateKey)
	if err != nil {
		var emptyIdentity identity.FullIdentity
		return emptyIdentity, fmt.Errorf("failed to reconstruct identity from private key: %w", err)
	}

	logger.Sugar.With("path", keyPath).Info("DefraDB identity successfully loaded from storage")
	return fullIdentity, nil
}

// savePeerKey saves the Ed25519 peer key bytes for persistence
func savePeerKey(keyPath string, peerKey ed25519.PrivateKey) error {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(keyPath), 0755); err != nil {
		return fmt.Errorf("failed to create key directory: %w", err)
	}

	// Encode as hex string for storage
	keyHex := hex.EncodeToString(peerKey)

	// Write to file with restricted permissions
	if err := os.WriteFile(keyPath, []byte(keyHex), 0600); err != nil {
		return fmt.Errorf("failed to write peer key file: %w", err)
	}

	logger.Sugar.With("path", keyPath).Info("P2P peer key saved to storage")
	return nil
}

// loadPeerKey loads an Ed25519 peer key from stored bytes
func loadPeerKey(keyPath string) (ed25519.PrivateKey, error) {
	// Read the stored key file
	keyHex, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read peer key file: %w", err)
	}

	// Decode hex string to bytes
	keyBytes, err := hex.DecodeString(string(keyHex))
	if err != nil {
		return nil, fmt.Errorf("failed to decode peer key hex: %w", err)
	}

	// Validate key length (Ed25519 private keys are 64 bytes)
	if len(keyBytes) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid peer key length: expected %d bytes, got %d", ed25519.PrivateKeySize, len(keyBytes))
	}

	logger.Sugar.With("path", keyPath).Info("P2P peer key successfully loaded from storage")
	return ed25519.PrivateKey(keyBytes), nil
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

	// Use persistent identity and peer key instead of ephemeral ones
	nodeKeys, err := getOrCreateNodeIdentity(cfg.DefraDB.Store.Path)
	if err != nil {
		return nil, fmt.Errorf("error getting or creating identity and peer key: %v", err)
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
		node.WithNodeIdentity(identity.Identity(nodeKeys.Identity)),
		netConfig.WithPrivateKey(nodeKeys.PeerKey),
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
