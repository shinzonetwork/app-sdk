package defra

import (
	"context"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/config"
	"github.com/shinzonetwork/app-sdk/pkg/file"
	"github.com/stretchr/testify/require"
)

func TestStartDefra(t *testing.T) {
	testConfig := DefaultConfig
	testConfig.DefraDB.Url = "127.0.0.1:0"
	myNode, err := StartDefraInstance(testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	myNode.Close(context.Background())
}

func TestStartDefraUsingConfig(t *testing.T) {
	configPath, err := file.FindFile("config.yaml")
	require.NoError(t, err)

	testConfig, err := config.LoadConfig(configPath)
	require.NoError(t, err)

	testConfig.DefraDB.Url = "127.0.0.1:0" // In case we have something else running

	myNode, err := StartDefraInstance(testConfig, &MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	myNode.Close(context.Background())
}
