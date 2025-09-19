package defra

import (
	"context"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/config"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUser represents a simple user type for testing
type TestUser struct {
	Name string `json:"name"`
}

func setupTestQueryClient(t *testing.T) (*node.Node, *queryClient) {
	// Create test config
	testConfig := &config.Config{
		DefraDB: config.DefraDBConfig{
			Url:           "http://localhost:0", // Use port 0 for random available port
			KeyringSecret: "test-secret",
			P2P: config.DefraP2PConfig{
				BootstrapPeers: []string{},
				ListenAddr:     "",
			},
			Store: config.DefraStoreConfig{
				Path: t.TempDir(),
			},
		},
		Logger: config.LoggerConfig{
			Development: true,
		},
	}

	schemaApplier := NewSchemaApplierFromProvidedSchema(`
		type User {
			name: String
		}
	`)

	// Start Defra instance
	defraNode, err := StartDefraInstance(testConfig, schemaApplier)
	require.NoError(t, err)

	// Create query client
	queryClient, err := newQueryClient(defraNode)
	require.NoError(t, err)

	return defraNode, queryClient
}

func TestNewQueryClient(t *testing.T) {
	t.Run("valid node", func(t *testing.T) {
		defraNode, _ := setupTestQueryClient(t)
		defer defraNode.Close(context.Background())

		client, err := newQueryClient(defraNode)
		require.NoError(t, err)
		assert.NotNil(t, client)
		assert.Equal(t, defraNode, client.defraNode)
	})

	t.Run("nil node", func(t *testing.T) {
		client, err := newQueryClient(nil)
		require.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "defraNode parameter cannot be nil")
	})
}

func TestQueryClient_query(t *testing.T) {
	defraNode, queryClient := setupTestQueryClient(t)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	t.Run("valid introspection query", func(t *testing.T) {
		query := `
			query {
				__schema {
					types {
						name
					}
				}
			}
		`

		result, err := queryClient.query(ctx, query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// The result should be a map with __schema key
		resultMap, ok := result.(map[string]interface{})
		require.True(t, ok)
		assert.Contains(t, resultMap, "__schema")
	})

	t.Run("empty query", func(t *testing.T) {
		result, err := queryClient.query(ctx, "")
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "query parameter is empty")
	})

	t.Run("invalid query", func(t *testing.T) {
		query := `invalid graphql query`

		result, err := queryClient.query(ctx, query)
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "graphql errors")
	})
}

func TestQueryClient_queryAndUnmarshal(t *testing.T) {
	defraNode, queryClient := setupTestQueryClient(t)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	t.Run("successful unmarshal", func(t *testing.T) {
		query := `
			query {
				__schema {
					types {
						name
					}
				}
			}
		`

		var result map[string]interface{}
		err := queryClient.queryAndUnmarshal(ctx, query, &result)
		require.NoError(t, err)
		assert.Contains(t, result, "__schema")
	})

	t.Run("invalid query", func(t *testing.T) {
		query := `invalid graphql query`

		var result map[string]interface{}
		err := queryClient.queryAndUnmarshal(ctx, query, &result)
		require.Error(t, err)
	})
}

func TestQueryClient_getDataField(t *testing.T) {
	defraNode, queryClient := setupTestQueryClient(t)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	t.Run("successful data extraction", func(t *testing.T) {
		query := `
			query {
				__schema {
					types {
						name
					}
				}
			}
		`

		data, err := queryClient.getDataField(ctx, query)
		require.NoError(t, err)
		assert.Contains(t, data, "__schema")
	})

	t.Run("query with errors", func(t *testing.T) {
		query := `invalid graphql query`

		data, err := queryClient.getDataField(ctx, query)
		require.Error(t, err)
		assert.Nil(t, data)
	})
}

func TestQueryClient_queryInto(t *testing.T) {
	defraNode, queryClient := setupTestQueryClient(t)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	t.Run("successful query into struct", func(t *testing.T) {
		query := `
			query {
				__schema {
					types {
						name
					}
				}
			}
		`

		var result map[string]interface{}
		err := queryClient.queryInto(ctx, query, &result)
		require.NoError(t, err)
		assert.Contains(t, result, "__schema")
	})
}

func TestQueryClient_queryDataInto(t *testing.T) {
	defraNode, queryClient := setupTestQueryClient(t)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	// First, create some test data using PostMutation
	createUserQuery := `
		mutation {
			create_User(input: {name: "John Doe"}) {
				name
			}
		}
	`

	user, err := PostMutation[TestUser](ctx, defraNode, createUserQuery)
	require.NoError(t, err)
	require.Equal(t, "John Doe", user.Name)

	t.Run("query data into slice", func(t *testing.T) {
		query := `
			query {
				User {
					name
				}
			}
		`

		var users []TestUser
		err := queryClient.queryDataInto(ctx, query, &users)
		require.NoError(t, err)
		assert.Len(t, users, 1)
		assert.Equal(t, "John Doe", users[0].Name)
	})

	t.Run("query data into single struct", func(t *testing.T) {
		query := `
			query {
				User {
					name
				}
			}
		`

		var user TestUser
		err := queryClient.queryDataInto(ctx, query, &user)
		require.NoError(t, err)
		assert.Equal(t, "John Doe", user.Name)
	})

	t.Run("invalid result type (not pointer)", func(t *testing.T) {
		query := `
			query {
				User {
					name
				}
			}
		`

		var user TestUser
		err := queryClient.queryDataInto(ctx, query, user) // Not a pointer
		require.Error(t, err)
		assert.Contains(t, err.Error(), "result must be a pointer")
	})
}

func TestQuerySingle(t *testing.T) {
	defraNode, _ := setupTestQueryClient(t)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	// Create test data
	createUserQuery := `
		mutation {
			create_User(input: {name: "Jane Doe"}) {
				name
			}
		}
	`

	_, err := PostMutation[TestUser](ctx, defraNode, createUserQuery)
	require.NoError(t, err)

	t.Run("successful single query", func(t *testing.T) {
		query := `
			query {
				User {
					name
				}
			}
		`

		user, err := QuerySingle[TestUser](ctx, defraNode, query)
		require.NoError(t, err)
		assert.Equal(t, "Jane Doe", user.Name)
	})

	t.Run("query with no results", func(t *testing.T) {
		query := `
			query {
				User(filter: {name: {_eq: "NonExistent"}}) {
					name
				}
			}
		`

		user, err := QuerySingle[TestUser](ctx, defraNode, query)
		require.NoError(t, err) // No error when no results found, just empty result
		assert.Empty(t, user.Name)
	})
}

func TestQueryArray(t *testing.T) {
	defraNode, _ := setupTestQueryClient(t)
	defer defraNode.Close(context.Background())

	ctx := context.Background()

	// Create multiple test users
	users := []string{"Alice", "Bob", "Charlie"}
	for _, name := range users {
		createUserQuery := `
			mutation {
				create_User(input: {name: "` + name + `"}) {
					name
				}
			}
		`
		_, err := PostMutation[TestUser](ctx, defraNode, createUserQuery)
		require.NoError(t, err)
	}

	t.Run("successful array query", func(t *testing.T) {
		query := `
			query {
				User {
					name
				}
			}
		`

		userArray, err := QueryArray[TestUser](ctx, defraNode, query)
		require.NoError(t, err)
		assert.Len(t, userArray, 3) // 3 new users created in this test

		// Check that we have the expected names
		names := make(map[string]bool)
		for _, user := range userArray {
			names[user.Name] = true
		}
		assert.True(t, names["Alice"])
		assert.True(t, names["Bob"])
		assert.True(t, names["Charlie"])
	})

	t.Run("query with filter", func(t *testing.T) {
		query := `
			query {
				User(filter: {name: {_eq: "Alice"}}) {
					name
				}
			}
		`

		userArray, err := QueryArray[TestUser](ctx, defraNode, query)
		require.NoError(t, err)
		assert.Len(t, userArray, 1)
		assert.Equal(t, "Alice", userArray[0].Name)
	})
}
