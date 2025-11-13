package defra

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/stretchr/testify/require"
)

const testKeyringSecret = "test-keyring-secret-for-testing-only"

func TestMain(m *testing.M) {
	logger.Init(true)
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestKeyPersistence(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// First call should generate a new key
	nodeKeys1, err := getOrCreateNodeIdentity(tempDir, testKeyringSecret)
	require.NoError(t, err)
	require.NotEmpty(t, nodeKeys1.Identity)
	require.NotEmpty(t, nodeKeys1.PeerKey)

	// Verify keyring files were created (encrypted files in keys/ directory)
	keysDir := filepath.Join(tempDir, "keys")
	identityKeyPath := filepath.Join(keysDir, nodeIdentityKeyName)
	peerKeyPath := filepath.Join(keysDir, peerKeyName)
	_, err = os.Stat(identityKeyPath)
	require.NoError(t, err, "Identity key file should exist in keyring")
	_, err = os.Stat(peerKeyPath)
	require.NoError(t, err, "Peer key file should exist in keyring")

	// Second call should load from the existing keyring
	nodeKeys2, err := getOrCreateNodeIdentity(tempDir, testKeyringSecret)
	require.NoError(t, err)
	require.NotEmpty(t, nodeKeys2.Identity)
	require.NotEmpty(t, nodeKeys2.PeerKey)

	// With proper key persistence, the loaded identity and peer key should be the same
	require.Equal(t, nodeKeys1.Identity, nodeKeys2.Identity, "Loaded identity should match the original")
	require.Equal(t, nodeKeys1.PeerKey, nodeKeys2.PeerKey, "Loaded peer key should match the original")
}

func TestKeyFilePermissions(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Generate keys
	_, err := getOrCreateNodeIdentity(tempDir, testKeyringSecret)
	require.NoError(t, err)

	// Check identity key file permissions (keyring files use 0755 by default)
	keysDir := filepath.Join(tempDir, "keys")
	identityKeyPath := filepath.Join(keysDir, nodeIdentityKeyName)
	fileInfo, err := os.Stat(identityKeyPath)
	require.NoError(t, err)

	// Keyring files are encrypted and use 0755 permissions (handled by keyring system)
	expectedMode := os.FileMode(0755)
	actualMode := fileInfo.Mode().Perm()
	require.Equal(t, expectedMode, actualMode, "Identity key file should have 0755 permissions (keyring default)")

	// Check peer key file permissions
	peerKeyPath := filepath.Join(keysDir, peerKeyName)
	fileInfo, err = os.Stat(peerKeyPath)
	require.NoError(t, err)

	actualMode = fileInfo.Mode().Perm()
	require.Equal(t, expectedMode, actualMode, "Peer key file should have 0755 permissions (keyring default)")
}

func TestKeyLoadingWithCorruptedFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	keysDir := filepath.Join(tempDir, "keys")
	identityKeyPath := filepath.Join(keysDir, nodeIdentityKeyName)

	// Create a corrupted identity key file (invalid encrypted data)
	err := os.MkdirAll(keysDir, 0755)
	require.NoError(t, err)

	err = os.WriteFile(identityKeyPath, []byte("corrupted_encrypted_data"), 0755)
	require.NoError(t, err)

	// Should fail to load corrupted key and return error
	_, err = getOrCreateNodeIdentity(tempDir, testKeyringSecret)
	require.Error(t, err, "Should fail to load corrupted key file")
	// Keyring decryption will fail, but the error message may vary
	require.Contains(t, err.Error(), "failed", "Error should indicate failure")
}

func TestKeyPersistenceAcrossRestarts(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	keysDir := filepath.Join(tempDir, "keys")
	identityKeyPath := filepath.Join(keysDir, nodeIdentityKeyName)
	peerKeyPath := filepath.Join(keysDir, peerKeyName)

	// Simulate first startup - no keys exist
	require.NoFileExists(t, identityKeyPath, "Identity key file should not exist initially")
	require.NoFileExists(t, peerKeyPath, "Peer key file should not exist initially")

	// First startup: generate and save keys
	nodeKeys1, err := getOrCreateNodeIdentity(tempDir, testKeyringSecret)
	require.NoError(t, err)
	require.NotEmpty(t, nodeKeys1.Identity)
	require.NotEmpty(t, nodeKeys1.PeerKey)

	// Verify keyring files were created
	require.FileExists(t, identityKeyPath, "Identity key file should exist after first startup")
	require.FileExists(t, peerKeyPath, "Peer key file should exist after first startup")

	// Read the encrypted key file contents to verify they persist
	identityKeyContent1, err := os.ReadFile(identityKeyPath)
	require.NoError(t, err)
	require.NotEmpty(t, identityKeyContent1)

	peerKeyContent1, err := os.ReadFile(peerKeyPath)
	require.NoError(t, err)
	require.NotEmpty(t, peerKeyContent1)

	// Simulate shutdown and restart - key files should still exist
	require.FileExists(t, identityKeyPath, "Identity key file should persist after shutdown")
	require.FileExists(t, peerKeyPath, "Peer key file should persist after shutdown")

	// Second startup: load existing keys
	nodeKeys2, err := getOrCreateNodeIdentity(tempDir, testKeyringSecret)
	require.NoError(t, err)
	require.NotEmpty(t, nodeKeys2.Identity)
	require.NotEmpty(t, nodeKeys2.PeerKey)

	// Verify the encrypted key file contents haven't changed
	identityKeyContent2, err := os.ReadFile(identityKeyPath)
	require.NoError(t, err)
	require.Equal(t, identityKeyContent1, identityKeyContent2, "Identity key file content should remain the same across restarts")

	peerKeyContent2, err := os.ReadFile(peerKeyPath)
	require.NoError(t, err)
	require.Equal(t, peerKeyContent1, peerKeyContent2, "Peer key file content should remain the same across restarts")

	// With proper key persistence, identities and peer keys should be the same across restarts
	require.Equal(t, nodeKeys1.Identity, nodeKeys2.Identity, "Identities should be identical across restarts")
	require.Equal(t, nodeKeys1.PeerKey, nodeKeys2.PeerKey, "Peer keys should be identical across restarts")

	// Third startup: verify key files are still used
	nodeKeys3, err := getOrCreateNodeIdentity(tempDir, testKeyringSecret)
	require.NoError(t, err)
	require.NotEmpty(t, nodeKeys3.Identity)
	require.NotEmpty(t, nodeKeys3.PeerKey)

	// Key file contents should still be the same
	identityKeyContent3, err := os.ReadFile(identityKeyPath)
	require.NoError(t, err)
	require.Equal(t, identityKeyContent1, identityKeyContent3, "Identity key file content should remain consistent across multiple restarts")

	peerKeyContent3, err := os.ReadFile(peerKeyPath)
	require.NoError(t, err)
	require.Equal(t, peerKeyContent1, peerKeyContent3, "Peer key file content should remain consistent across multiple restarts")

	// All identities and peer keys should be the same
	require.Equal(t, nodeKeys1.Identity, nodeKeys3.Identity, "All identities should be identical across multiple restarts")
	require.Equal(t, nodeKeys1.PeerKey, nodeKeys3.PeerKey, "All peer keys should be identical across multiple restarts")
}
