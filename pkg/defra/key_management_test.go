package defra

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/config"
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

func TestDefraNodeIdentityPersistenceAcrossStartStopRestart(t *testing.T) {
	// Create a persistent directory for this test
	tempDir := t.TempDir()

	// Create test config with persistent store path
	testConfig := &config.Config{
		DefraDB: config.DefraDBConfig{
			Url:           "http://localhost:0",
			KeyringSecret: "test-secret",
			P2P: config.DefraP2PConfig{
				BootstrapPeers: []string{},
				ListenAddr:     "",
			},
			Store: config.DefraStoreConfig{
				Path: tempDir,
			},
		},
		Logger: config.LoggerConfig{
			Development: true,
		},
	}

	// Create schema applier
	schemaApplier := NewSchemaApplierFromProvidedSchema(`
		type User {
			name: String
		}
	`)

	ctx := context.Background()
	var firstIdentityKeyContent []byte
	var secondIdentityKeyContent []byte
	var thirdIdentityKeyContent []byte
	keyPath := filepath.Join(tempDir, keyFileName)

	// First startup: Create DefraDB node and capture its identity
	t.Run("first startup", func(t *testing.T) {
		defraNode1, err := StartDefraInstance(testConfig, schemaApplier)
		require.NoError(t, err)
		require.NotNil(t, defraNode1)

		// Verify key file was created
		require.FileExists(t, keyPath, "Key file should exist after first startup")

		// Read the key file content to compare later
		firstIdentityKeyContent, err = os.ReadFile(keyPath)
		require.NoError(t, err)
		require.NotEmpty(t, firstIdentityKeyContent, "Key file should have content")

		// Properly close the node
		err = defraNode1.Close(ctx)
		require.NoError(t, err)

		// Add a small delay to ensure complete cleanup
		time.Sleep(100 * time.Millisecond)
	})

	// Second startup: Restart with same config and verify same identity
	t.Run("second startup (restart)", func(t *testing.T) {
		defraNode2, err := StartDefraInstance(testConfig, schemaApplier)
		require.NoError(t, err)
		require.NotNil(t, defraNode2)

		// Read the key file content and verify it's the same
		secondIdentityKeyContent, err = os.ReadFile(keyPath)
		require.NoError(t, err)
		require.NotEmpty(t, secondIdentityKeyContent, "Key file should have content on restart")

		// Verify the key content is identical to the first startup
		require.Equal(t, firstIdentityKeyContent, secondIdentityKeyContent, "Identity key should be identical after restart")

		fmt.Println(firstIdentityKeyContent, "====", secondIdentityKeyContent)
		// Properly close the node
		err = defraNode2.Close(ctx)
		require.NoError(t, err)

		// Add a small delay to ensure complete cleanup
		time.Sleep(100 * time.Millisecond)
	})

	// Third startup: Another restart to verify consistency
	t.Run("third startup (second restart)", func(t *testing.T) {
		defraNode3, err := StartDefraInstance(testConfig, schemaApplier)
		require.NoError(t, err)
		require.NotNil(t, defraNode3)

		// Read the key file content and verify it's still the same
		thirdIdentityKeyContent, err = os.ReadFile(keyPath)
		require.NoError(t, err)
		require.NotEmpty(t, thirdIdentityKeyContent, "Key file should have content on second restart")

		// Verify the key content is still identical
		require.Equal(t, firstIdentityKeyContent, thirdIdentityKeyContent, "Identity key should remain identical across multiple restarts")
		require.Equal(t, secondIdentityKeyContent, thirdIdentityKeyContent, "Identity key should be consistent between all restarts")

		fmt.Println(secondIdentityKeyContent, "====", thirdIdentityKeyContent)
		// Properly close the node
		err = defraNode3.Close(ctx)
		require.NoError(t, err)
	})

	// Verify all three identity key contents are identical
	require.Equal(t, firstIdentityKeyContent, secondIdentityKeyContent, "First and second startup should have identical identity keys")
	require.Equal(t, secondIdentityKeyContent, thirdIdentityKeyContent, "Second and third startup should have identical identity keys")
	require.Equal(t, firstIdentityKeyContent, thirdIdentityKeyContent, "First and third startup should have identical identity keys")
}
