package defra

import (
	"context"
	"crypto/ed25519"
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
	"github.com/sourcenetwork/defradb/keyring"
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

// Key names matching DefraDB's keyring constants
const (
	peerKeyName         = "peer-key"
	nodeIdentityKeyName = "node-identity-key"
)

// Key Management Implementation Notes:
//
// This implementation uses DefraDB's keyring system for secure, encrypted key storage:
// 1. Keys are stored encrypted using PBES2_HS512_A256KW encryption
// 2. Uses the same keyring system as DefraDB CLI for consistency
// 3. Keys are stored in the "keys" subdirectory of the store path
// 4. Requires KeyringSecret to be set in config for encryption/decryption
//
// Security Features:
// - Keys are encrypted at rest using the KeyringSecret
// - Uses DefraDB's standard keyring interface
// - Proper error handling for missing keys and corrupted data
//
// Future Enhancements:
// - Consider key rotation and backup mechanisms
// - Support for system keyring backend option

// NodeKeys holds both the node identity and P2P peer key
type NodeKeys struct {
	Identity identity.FullIdentity
	PeerKey  ed25519.PrivateKey
}

// openKeyring opens a file-based keyring for the given store path and password
func openKeyring(storePath string, password string) (keyring.Keyring, error) {
	if password == "" {
		return nil, fmt.Errorf("keyring secret is required for encrypted key storage")
	}
	keyringDir := filepath.Join(storePath, "keys")
	return keyring.OpenFileKeyring(keyringDir, []byte(password))
}

// getOrCreateNodeIdentity retrieves existing node identity and peer key from keyring or creates new ones
func getOrCreateNodeIdentity(storePath string, keyringSecret string) (NodeKeys, error) {
	kr, err := openKeyring(storePath, keyringSecret)
	if err != nil {
		return NodeKeys{}, fmt.Errorf("failed to open keyring: %w", err)
	}

	nodeIdentity, err := getOrCreateNodeIdentityFromKeyring(kr)
	if err != nil {
		return NodeKeys{}, err
	}

	peerKey, err := getOrCreatePeerKeyFromKeyring(kr)
	if err != nil {
		return NodeKeys{}, err
	}

	return NodeKeys{
		Identity: nodeIdentity,
		PeerKey:  peerKey,
	}, nil
}

// getOrCreateNodeIdentityFromKeyring retrieves or creates the node identity from the keyring
func getOrCreateNodeIdentityFromKeyring(kr keyring.Keyring) (identity.FullIdentity, error) {
	// Try to load existing node identity from keyring
	identityBytes, err := kr.Get(nodeIdentityKeyName)
	if err != nil && err != keyring.ErrNotFound {
		return nil, fmt.Errorf("failed to get node identity from keyring: %w", err)
	}

	if err == keyring.ErrNotFound {
		return createAndStoreNodeIdentity(kr)
	}

	return loadNodeIdentityFromKeyring(kr, identityBytes)
}

// createAndStoreNodeIdentity generates a new node identity and stores it in the keyring
func createAndStoreNodeIdentity(kr keyring.Keyring) (identity.FullIdentity, error) {
	logger.Sugar.Info("Generating new DefraDB identity")
	nodeIdentity, err := identity.Generate(crypto.KeyTypeSecp256k1)
	if err != nil {
		return nil, fmt.Errorf("failed to generate new identity: %w", err)
	}

	// Store the identity in keyring
	rawIdentity := nodeIdentity.IntoRawIdentity()
	keyType := rawIdentity.KeyType
	privKeyBytes := nodeIdentity.PrivateKey().Raw()
	// Format: "keyType:privateKeyBytes" (matching DefraDB's format)
	identityBytes := append([]byte(keyType+":"), privKeyBytes...)
	if err := kr.Set(nodeIdentityKeyName, identityBytes); err != nil {
		logger.Sugar.Warnf("Failed to save identity to keyring: %v", err)
		// Continue with ephemeral key if save fails
	} else {
		logger.Sugar.Info("DefraDB identity saved to keyring")
	}

	return nodeIdentity, nil
}

// loadNodeIdentityFromKeyring loads an existing node identity from the keyring
func loadNodeIdentityFromKeyring(kr keyring.Keyring, identityBytes []byte) (identity.FullIdentity, error) {
	logger.Sugar.Info("Loading existing DefraDB identity from keyring")

	// Parse the identity bytes format: "keyType:privateKeyBytes"
	sepPos := -1
	for i, b := range identityBytes {
		if b == ':' {
			sepPos = i
			break
		}
	}

	if sepPos == -1 {
		// Old format without key type prefix, assume Secp256k1
		identityBytes = append([]byte(crypto.KeyTypeSecp256k1+":"), identityBytes...)
		sepPos = len(crypto.KeyTypeSecp256k1)
		// Update keyring with new format
		_ = kr.Set(nodeIdentityKeyName, identityBytes)
	}

	keyType := string(identityBytes[:sepPos])
	privKeyBytes := identityBytes[sepPos+1:]
	privateKey, err := crypto.PrivateKeyFromBytes(crypto.KeyType(keyType), privKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct private key: %w", err)
	}

	nodeIdentity, err := identity.FromPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct identity from private key: %w", err)
	}

	logger.Sugar.Info("DefraDB identity successfully loaded from keyring")
	return nodeIdentity, nil
}

// getOrCreatePeerKeyFromKeyring retrieves or creates the P2P peer key from the keyring
func getOrCreatePeerKeyFromKeyring(kr keyring.Keyring) (ed25519.PrivateKey, error) {
	// Try to load existing peer key from keyring
	peerKeyBytes, err := kr.Get(peerKeyName)
	if err != nil && err != keyring.ErrNotFound {
		return nil, fmt.Errorf("failed to get peer key from keyring: %w", err)
	}

	if err == keyring.ErrNotFound {
		return createAndStorePeerKey(kr)
	}

	return loadPeerKeyFromKeyring(peerKeyBytes)
}

// createAndStorePeerKey generates a new P2P peer key and stores it in the keyring
func createAndStorePeerKey(kr keyring.Keyring) (ed25519.PrivateKey, error) {
	logger.Sugar.Info("Generating new P2P peer key")
	peerKey, err := crypto.GenerateEd25519()
	if err != nil {
		return nil, fmt.Errorf("failed to generate new peer key: %w", err)
	}

	// Store the peer key in keyring
	if err := kr.Set(peerKeyName, peerKey); err != nil {
		logger.Sugar.Warnf("Failed to save peer key to keyring: %v", err)
		// Continue with ephemeral key if save fails
	} else {
		logger.Sugar.Info("P2P peer key saved to keyring")
	}

	return peerKey, nil
}

// loadPeerKeyFromKeyring loads an existing P2P peer key from the keyring
func loadPeerKeyFromKeyring(peerKeyBytes []byte) (ed25519.PrivateKey, error) {
	logger.Sugar.Info("Loading existing P2P peer key from keyring")

	if len(peerKeyBytes) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid peer key length: expected %d bytes, got %d", ed25519.PrivateKeySize, len(peerKeyBytes))
	}

	peerKey := ed25519.PrivateKey(peerKeyBytes)
	logger.Sugar.Info("P2P peer key successfully loaded from keyring")
	return peerKey, nil
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
	// KeyringSecret is required for encrypted key storage
	keyringSecret := cfg.DefraDB.KeyringSecret
	if keyringSecret == "" {
		return nil, fmt.Errorf("keyring secret is required for persistent key storage. Set DEFRA_KEYRING_SECRET environment variable or keyring_secret in config")
	}

	nodeKeys, err := getOrCreateNodeIdentity(cfg.DefraDB.Store.Path, keyringSecret)
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
