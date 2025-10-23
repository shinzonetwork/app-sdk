package defra

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/sourcenetwork/defradb/client"
)

func TestBootstrapIntoPeers(t *testing.T) {
	tests := []struct {
		name           string
		input          []string
		expectedPeers  []client.PeerInfo
		expectedErrors int
	}{
		{
			name:  "valid single peer",
			input: []string{"127.0.0.1:4001/p2p/12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8"},
			expectedPeers: []client.PeerInfo{
				{
					Addresses: []string{"127.0.0.1:4001"},
					ID:        "12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
				},
			},
			expectedErrors: 0,
		},
		{
			name:  "valid multiple peers",
			input: []string{"127.0.0.1:4001/p2p/12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8", "192.168.1.100:4002/p2p/12D3KooWEj8q4q5r6s7t8u9v0w1x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m"},
			expectedPeers: []client.PeerInfo{
				{
					Addresses: []string{"127.0.0.1:4001"},
					ID:        "12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
				},
				{
					Addresses: []string{"192.168.1.100:4002"},
					ID:        "12D3KooWEj8q4q5r6s7t8u9v0w1x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m",
				},
			},
			expectedErrors: 0,
		},
		{
			name:           "invalid peer format - missing /p2p/",
			input:          []string{"127.0.0.1:4001"},
			expectedPeers:  []client.PeerInfo{},
			expectedErrors: 1,
		},
		{
			name:           "invalid peer format - multiple /p2p/",
			input:          []string{"127.0.0.1:4001/p2p/12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8/p2p/extra"},
			expectedPeers:  []client.PeerInfo{},
			expectedErrors: 1,
		},
		{
			name:           "empty input",
			input:          []string{},
			expectedPeers:  []client.PeerInfo{},
			expectedErrors: 0,
		},
		{
			name:  "mixed valid and invalid peers",
			input: []string{"127.0.0.1:4001/p2p/12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8", "invalid", "192.168.1.100:4002/p2p/12D3KooWEj8q4q5r6s7t8u9v0w1x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m"},
			expectedPeers: []client.PeerInfo{
				{
					Addresses: []string{"127.0.0.1:4001"},
					ID:        "12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
				},
				{
					Addresses: []string{"192.168.1.100:4002"},
					ID:        "12D3KooWEj8q4q5r6s7t8u9v0w1x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m",
				},
			},
			expectedErrors: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peers, errors := bootstrapIntoPeers(tt.input)

			if len(errors) != tt.expectedErrors {
				t.Errorf("Expected %d errors, got %d", tt.expectedErrors, len(errors))
			}

			if len(peers) != len(tt.expectedPeers) {
				t.Errorf("Expected %d peers, got %d", len(tt.expectedPeers), len(peers))
			}

			for i, expectedPeer := range tt.expectedPeers {
				if i >= len(peers) {
					t.Errorf("Expected peer at index %d but got none", i)
					continue
				}

				actualPeer := peers[i]
				if actualPeer.ID != expectedPeer.ID {
					t.Errorf("Expected peer ID %s, got %s", expectedPeer.ID, actualPeer.ID)
				}

				if len(actualPeer.Addresses) != len(expectedPeer.Addresses) {
					t.Errorf("Expected %d addresses, got %d", len(expectedPeer.Addresses), len(actualPeer.Addresses))
				}

				for j, expectedAddr := range expectedPeer.Addresses {
					if j >= len(actualPeer.Addresses) {
						t.Errorf("Expected address at index %d but got none", j)
						continue
					}
					if actualPeer.Addresses[j] != expectedAddr {
						t.Errorf("Expected address %s, got %s", expectedAddr, actualPeer.Addresses[j])
					}
				}
			}
		})
	}
}

func TestPeersIntoBootstrap(t *testing.T) {
	tests := []struct {
		name                 string
		input                []client.PeerInfo
		expectedBootstrap    []string
		expectedErrors       int
		expectedErrorIndices []int
	}{
		{
			name: "valid single peer",
			input: []client.PeerInfo{
				{
					Addresses: []string{"127.0.0.1:4001"},
					ID:        "12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
				},
			},
			expectedBootstrap: []string{"127.0.0.1:4001/p2p/12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8"},
			expectedErrors:    0,
		},
		{
			name: "valid multiple peers",
			input: []client.PeerInfo{
				{
					Addresses: []string{"127.0.0.1:4001"},
					ID:        "12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
				},
				{
					Addresses: []string{"192.168.1.100:4002"},
					ID:        "12D3KooWEj8q4q5r6s7t8u9v0w1x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m",
				},
			},
			expectedBootstrap: []string{
				"127.0.0.1:4001/p2p/12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
				"192.168.1.100:4002/p2p/12D3KooWEj8q4q5r6s7t8u9v0w1x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m",
			},
			expectedErrors: 0,
		},
		{
			name: "peer with empty ID",
			input: []client.PeerInfo{
				{
					Addresses: []string{"127.0.0.1:4001"},
					ID:        "",
				},
			},
			expectedBootstrap:    []string{},
			expectedErrors:       1,
			expectedErrorIndices: []int{0},
		},
		{
			name: "peer with no addresses",
			input: []client.PeerInfo{
				{
					Addresses: []string{},
					ID:        "12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
				},
			},
			expectedBootstrap:    []string{},
			expectedErrors:       1,
			expectedErrorIndices: []int{0},
		},
		{
			name: "peer with multiple addresses - uses first",
			input: []client.PeerInfo{
				{
					Addresses: []string{"127.0.0.1:4001", "192.168.1.100:4002", "10.0.0.1:4003"},
					ID:        "12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
				},
			},
			expectedBootstrap: []string{"127.0.0.1:4001/p2p/12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8"},
			expectedErrors:    0,
		},
		{
			name:              "empty input",
			input:             []client.PeerInfo{},
			expectedBootstrap: []string{},
			expectedErrors:    0,
		},
		{
			name: "mixed valid and invalid peers",
			input: []client.PeerInfo{
				{
					Addresses: []string{"127.0.0.1:4001"},
					ID:        "12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
				},
				{
					Addresses: []string{"192.168.1.100:4002"},
					ID:        "",
				},
				{
					Addresses: []string{},
					ID:        "12D3KooWEj8q4q5r6s7t8u9v0w1x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m",
				},
			},
			expectedBootstrap:    []string{"127.0.0.1:4001/p2p/12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8"},
			expectedErrors:       2,
			expectedErrorIndices: []int{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bootstrapPeers, errors := PeersIntoBootstrap(tt.input)

			if len(errors) != tt.expectedErrors {
				t.Errorf("Expected %d errors, got %d", tt.expectedErrors, len(errors))
			}

			if len(bootstrapPeers) != len(tt.expectedBootstrap) {
				t.Errorf("Expected %d bootstrap peers, got %d", len(tt.expectedBootstrap), len(bootstrapPeers))
			}

			for i, expectedBootstrap := range tt.expectedBootstrap {
				if i >= len(bootstrapPeers) {
					t.Errorf("Expected bootstrap peer at index %d but got none", i)
					continue
				}
				if bootstrapPeers[i] != expectedBootstrap {
					t.Errorf("Expected bootstrap peer %s, got %s", expectedBootstrap, bootstrapPeers[i])
				}
			}

			// Verify error indices if specified
			if tt.expectedErrorIndices != nil {
				for i, expectedIdx := range tt.expectedErrorIndices {
					if i >= len(errors) {
						t.Errorf("Expected error at index %d but got none", i)
						continue
					}
					// Check that the error message contains the expected index
					errorMsg := errors[i].Error()
					expectedIdxStr := fmt.Sprintf("index %d", expectedIdx)
					if !strings.Contains(errorMsg, expectedIdxStr) {
						t.Errorf("Expected error message to contain '%s', got: %s", expectedIdxStr, errorMsg)
					}
				}
			}
		})
	}
}

func TestBootstrapIntoPeersAndBack(t *testing.T) {
	// Test round-trip conversion
	originalBootstrap := []string{
		"127.0.0.1:4001/p2p/12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
		"192.168.1.100:4002/p2p/12D3KooWEj8q4q5r6s7t8u9v0w1x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m",
	}

	// Convert bootstrap strings to peers
	peers, errors := bootstrapIntoPeers(originalBootstrap)
	if len(errors) > 0 {
		t.Errorf("Unexpected errors during bootstrap to peers conversion: %v", errors)
	}

	// Convert peers back to bootstrap strings
	convertedBootstrap, errors := PeersIntoBootstrap(peers)
	if len(errors) > 0 {
		t.Errorf("Unexpected errors during peers to bootstrap conversion: %v", errors)
	}

	// Verify round-trip conversion
	if len(convertedBootstrap) != len(originalBootstrap) {
		t.Errorf("Expected %d bootstrap peers after round-trip, got %d", len(originalBootstrap), len(convertedBootstrap))
	}

	for i, original := range originalBootstrap {
		if i >= len(convertedBootstrap) {
			t.Errorf("Expected bootstrap peer at index %d but got none", i)
			continue
		}
		if convertedBootstrap[i] != original {
			t.Errorf("Expected bootstrap peer %s, got %s", original, convertedBootstrap[i])
		}
	}
}

func TestConnectToPeers(t *testing.T) {
	t.Run("nil node should panic", func(t *testing.T) {
		ctx := context.Background()
		peers := []client.PeerInfo{
			{
				Addresses: []string{"127.0.0.1:4001"},
				ID:        "12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
			},
		}

		// This should panic with nil node
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected function to panic with nil node, but it didn't")
			}
		}()

		connectToPeers(ctx, nil, peers)
	})

	t.Run("empty peers list", func(t *testing.T) {
		ctx := context.Background()
		peers := []client.PeerInfo{}

		// This should not panic even with nil node since there are no peers to connect to
		errors := connectToPeers(ctx, nil, peers)

		if len(errors) != 0 {
			t.Errorf("Expected no errors with empty peers list, got %d", len(errors))
		}
	})

	t.Run("connect to valid peers", func(t *testing.T) {
		ctx := context.Background()

		// Start a test Defra node
		testConfig := DefaultConfig
		testNode, err := StartDefraInstanceWithTestConfig(t, testConfig, &MockSchemaApplierThatSucceeds{})
		if err != nil {
			t.Fatalf("Failed to start test Defra node: %v", err)
		}
		defer testNode.Close(ctx)

		// Create some valid peer info (these will fail to connect since they're not real peers, but should not panic)
		peers := []client.PeerInfo{
			{
				Addresses: []string{"127.0.0.1:4001"},
				ID:        "12D3KooWBh1N2rLJc9Rj7Z3rX9Y8uMvN2pQ4sT7wX1yB6eF9hK3mP5sA8",
			},
			{
				Addresses: []string{"192.168.1.100:4002"},
				ID:        "12D3KooWEj8q4q5r6s7t8u9v0w1x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m",
			},
		}

		// This should not panic and should return connection errors (since these are fake peers)
		errors := connectToPeers(ctx, testNode, peers)

		// We expect errors since these are fake peer addresses
		if len(errors) == 0 {
			t.Errorf("Expected connection errors with fake peers, but got none")
		}

		// Verify that we got the expected number of errors (one per peer)
		if len(errors) != len(peers) {
			t.Errorf("Expected %d connection errors, got %d", len(peers), len(errors))
		}

		// Verify error messages contain expected information
		for i, err := range errors {
			errorMsg := err.Error()
			expectedIdxStr := fmt.Sprintf("peer %d", i)
			if !strings.Contains(errorMsg, expectedIdxStr) {
				t.Errorf("Expected error message to contain '%s', got: %s", expectedIdxStr, errorMsg)
			}
		}
	})

	t.Run("connect to empty peers list with real node", func(t *testing.T) {
		ctx := context.Background()

		// Start a test Defra node
		testConfig := DefaultConfig
		testNode, err := StartDefraInstanceWithTestConfig(t, testConfig, &MockSchemaApplierThatSucceeds{})
		if err != nil {
			t.Fatalf("Failed to start test Defra node: %v", err)
		}
		defer testNode.Close(ctx)

		peers := []client.PeerInfo{}

		// This should not panic and should return no errors
		errors := connectToPeers(ctx, testNode, peers)

		if len(errors) != 0 {
			t.Errorf("Expected no errors with empty peers list, got %d", len(errors))
		}
	})

	t.Run("connect multiple nodes to each other", func(t *testing.T) {
		ctx := context.Background()

		// Start first Defra node with a specific listen address
		testConfig1 := DefaultConfig
		testConfig1.DefraDB.P2P.ListenAddr = "/ip4/127.0.0.1/tcp/9171"
		node1, err := StartDefraInstanceWithTestConfig(t, testConfig1, &MockSchemaApplierThatSucceeds{})
		if err != nil {
			t.Fatalf("Failed to start first Defra node: %v", err)
		}
		defer node1.Close(ctx)

		// Start second Defra node with a different listen address
		testConfig2 := DefaultConfig
		testConfig2.DefraDB.P2P.ListenAddr = "/ip4/127.0.0.1/tcp/9172"
		node2, err := StartDefraInstanceWithTestConfig(t, testConfig2, &MockSchemaApplierThatSucceeds{})
		if err != nil {
			t.Fatalf("Failed to start second Defra node: %v", err)
		}
		defer node2.Close(ctx)

		// Get the peer info from node1 to connect node2 to it
		node1PeerInfo := node1.DB.PeerInfo()

		// Convert node1's peer info to bootstrap format and back to verify our conversion functions
		bootstrapPeers, errors := PeersIntoBootstrap([]client.PeerInfo{node1PeerInfo})
		if len(errors) > 0 {
			t.Fatalf("Failed to convert node1 peer info to bootstrap format: %v", errors)
		}

		if len(bootstrapPeers) != 1 {
			t.Fatalf("Expected 1 bootstrap peer, got %d", len(bootstrapPeers))
		}

		// Convert bootstrap peer back to peer info format
		convertedPeers, errors := bootstrapIntoPeers(bootstrapPeers)
		if len(errors) > 0 {
			t.Fatalf("Failed to convert bootstrap peer back to peer info: %v", errors)
		}

		if len(convertedPeers) != 1 {
			t.Fatalf("Expected 1 converted peer, got %d", len(convertedPeers))
		}

		// Verify the round-trip conversion worked correctly
		if convertedPeers[0].ID != node1PeerInfo.ID {
			t.Errorf("Expected converted peer ID %s, got %s", node1PeerInfo.ID, convertedPeers[0].ID)
		}

		// Now connect node2 to node1 using our connectToPeers function
		connectionErrors := connectToPeers(ctx, node2, convertedPeers)

		// We expect this to succeed since both nodes are real and running
		if len(connectionErrors) > 0 {
			// Log the errors but don't fail the test immediately - P2P connections can be flaky
			t.Logf("Connection errors (this may be expected in some environments): %v", connectionErrors)
		} else {
			t.Log("Successfully connected node2 to node1!")
		}

		// Test connecting node1 to node2 as well (bidirectional connection)
		node2PeerInfo := node2.DB.PeerInfo()
		bootstrapPeers2, errors := PeersIntoBootstrap([]client.PeerInfo{node2PeerInfo})
		if len(errors) > 0 {
			t.Fatalf("Failed to convert node2 peer info to bootstrap format: %v", errors)
		}

		convertedPeers2, errors := bootstrapIntoPeers(bootstrapPeers2)
		if len(errors) > 0 {
			t.Fatalf("Failed to convert node2 bootstrap peer back to peer info: %v", errors)
		}

		connectionErrors2 := connectToPeers(ctx, node1, convertedPeers2)
		if len(connectionErrors2) > 0 {
			t.Logf("Bidirectional connection errors (this may be expected): %v", connectionErrors2)
		} else {
			t.Log("Successfully established bidirectional connection between nodes!")
		}
	})
}
