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
	identity1, err := getOrCreateNodeIdentity(tempDir)
	require.NoError(t, err)
	require.NotEmpty(t, identity1)

	// Verify key file was created
	keyPath := filepath.Join(tempDir, keyFileName)
	_, err = os.Stat(keyPath)
	require.NoError(t, err, "Key file should exist")

	// Second call should load from the existing marker file
	identity2, err := getOrCreateNodeIdentity(tempDir)
	require.NoError(t, err)
	require.NotEmpty(t, identity2)

	// With proper key persistence, the loaded identity should be the same
	require.Equal(t, identity1, identity2, "Loaded identity should match the original")
}

func TestKeyFilePermissions(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Generate a key
	_, err := getOrCreateNodeIdentity(tempDir)
	require.NoError(t, err)

	// Check file permissions
	keyPath := filepath.Join(tempDir, keyFileName)
	fileInfo, err := os.Stat(keyPath)
	require.NoError(t, err)

	// File should have restricted permissions (0600)
	expectedMode := os.FileMode(0600)
	actualMode := fileInfo.Mode().Perm()
	require.Equal(t, expectedMode, actualMode, "Key file should have 0600 permissions")
}

func TestKeyLoadingWithCorruptedFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	keyPath := filepath.Join(tempDir, keyFileName)

	// Create a corrupted key file (invalid hex data)
	err := os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

	err = os.WriteFile(keyPath, []byte("corrupted_hex_data"), 0600)
	require.NoError(t, err)

	// Should fail to load corrupted key and return error
	_, err = getOrCreateNodeIdentity(tempDir)
	require.Error(t, err, "Should fail to load corrupted key file")
	require.Contains(t, err.Error(), "failed to decode key hex", "Error should mention hex decoding failure")
}

func TestKeyPersistenceAcrossRestarts(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	keyPath := filepath.Join(tempDir, keyFileName)

	// Simulate first startup - no key exists
	require.NoFileExists(t, keyPath, "Key file should not exist initially")

	// First startup: generate and save key
	identity1, err := getOrCreateNodeIdentity(tempDir)
	require.NoError(t, err)
	require.NotEmpty(t, identity1)

	// Verify key file was created
	require.FileExists(t, keyPath, "Key file should exist after first startup")

	// Read the key file content to verify it persists
	keyContent1, err := os.ReadFile(keyPath)
	require.NoError(t, err)
	require.NotEmpty(t, keyContent1)

	// Simulate shutdown and restart - key file should still exist
	require.FileExists(t, keyPath, "Key file should persist after shutdown")

	// Second startup: load existing key
	identity2, err := getOrCreateNodeIdentity(tempDir)
	require.NoError(t, err)
	require.NotEmpty(t, identity2)

	// Verify the key file content hasn't changed
	keyContent2, err := os.ReadFile(keyPath)
	require.NoError(t, err)
	require.Equal(t, keyContent1, keyContent2, "Key file content should remain the same across restarts")

	// With proper key persistence, identities should be the same across restarts
	require.Equal(t, identity1, identity2, "Identities should be identical across restarts")

	// Third startup: verify key file is still used
	identity3, err := getOrCreateNodeIdentity(tempDir)
	require.NoError(t, err)
	require.NotEmpty(t, identity3)

	// Key file content should still be the same
	keyContent3, err := os.ReadFile(keyPath)
	require.NoError(t, err)
	require.Equal(t, keyContent1, keyContent3, "Key file content should remain consistent across multiple restarts")

	// All identities should be the same
	require.Equal(t, identity1, identity3, "All identities should be identical across multiple restarts")
}
