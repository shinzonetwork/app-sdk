package defra

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logger.Init(true)
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestKeyPersistence(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// First call should generate a new key
	nodeKeys1, err := getOrCreateNodeIdentity(tempDir)
	require.NoError(t, err)
	require.NotEmpty(t, nodeKeys1.Identity)
	require.NotEmpty(t, nodeKeys1.PeerKey)

	// Verify key files were created
	identityKeyPath := filepath.Join(tempDir, keyFileName)
	peerKeyPath := filepath.Join(tempDir, peerKeyFileName)
	_, err = os.Stat(identityKeyPath)
	require.NoError(t, err, "Identity key file should exist")
	_, err = os.Stat(peerKeyPath)
	require.NoError(t, err, "Peer key file should exist")

	// Second call should load from the existing files
	nodeKeys2, err := getOrCreateNodeIdentity(tempDir)
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
	_, err := getOrCreateNodeIdentity(tempDir)
	require.NoError(t, err)

	// Check identity key file permissions
	identityKeyPath := filepath.Join(tempDir, keyFileName)
	fileInfo, err := os.Stat(identityKeyPath)
	require.NoError(t, err)

	// File should have restricted permissions (0600)
	expectedMode := os.FileMode(0600)
	actualMode := fileInfo.Mode().Perm()
	require.Equal(t, expectedMode, actualMode, "Identity key file should have 0600 permissions")

	// Check peer key file permissions
	peerKeyPath := filepath.Join(tempDir, peerKeyFileName)
	fileInfo, err = os.Stat(peerKeyPath)
	require.NoError(t, err)

	actualMode = fileInfo.Mode().Perm()
	require.Equal(t, expectedMode, actualMode, "Peer key file should have 0600 permissions")
}

func TestKeyLoadingWithCorruptedFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	identityKeyPath := filepath.Join(tempDir, keyFileName)

	// Create a corrupted identity key file (invalid hex data)
	err := os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

	err = os.WriteFile(identityKeyPath, []byte("corrupted_hex_data"), 0600)
	require.NoError(t, err)

	// Should fail to load corrupted key and return error
	_, err = getOrCreateNodeIdentity(tempDir)
	require.Error(t, err, "Should fail to load corrupted key file")
	require.Contains(t, err.Error(), "failed to decode key hex", "Error should mention hex decoding failure")
}

func TestKeyPersistenceAcrossRestarts(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	identityKeyPath := filepath.Join(tempDir, keyFileName)
	peerKeyPath := filepath.Join(tempDir, peerKeyFileName)

	// Simulate first startup - no keys exist
	require.NoFileExists(t, identityKeyPath, "Identity key file should not exist initially")
	require.NoFileExists(t, peerKeyPath, "Peer key file should not exist initially")

	// First startup: generate and save keys
	nodeKeys1, err := getOrCreateNodeIdentity(tempDir)
	require.NoError(t, err)
	require.NotEmpty(t, nodeKeys1.Identity)
	require.NotEmpty(t, nodeKeys1.PeerKey)

	// Verify key files were created
	require.FileExists(t, identityKeyPath, "Identity key file should exist after first startup")
	require.FileExists(t, peerKeyPath, "Peer key file should exist after first startup")

	// Read the key file contents to verify they persist
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
	nodeKeys2, err := getOrCreateNodeIdentity(tempDir)
	require.NoError(t, err)
	require.NotEmpty(t, nodeKeys2.Identity)
	require.NotEmpty(t, nodeKeys2.PeerKey)

	// Verify the key file contents haven't changed
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
	nodeKeys3, err := getOrCreateNodeIdentity(tempDir)
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
