package views

import (
	"context"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

func TestSubscribeToView(t *testing.T) {
	testView := View{
		Query:  "Log {address topics data transactionHash blockNumber}",
		Sdl:    "type FilteredAndDecodedLogs {transactionHash: String}",
		Lenses: nil,
		Name:   "FilteredAndDecodedLogs",
	}

	myDefra, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	err = testView.SubscribeTo(context.Background(), myDefra)
	require.NoError(t, err)
}

func TestSubscribeToInvalidViewFails(t *testing.T) {
	testView := View{
		Query:  "Log {address topics data transactionHash blockNumber}",
		Sdl:    "type FilteredAndDecodedLogs @materialized(if: false) {transactionHash: String}",
		Lenses: nil,
		Name:   "FilteredAndDecodedLogs",
	}

	myDefra, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	err = testView.SubscribeTo(context.Background(), myDefra)
	require.Error(t, err)
}
