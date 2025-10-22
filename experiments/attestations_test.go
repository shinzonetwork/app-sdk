package experiments

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

// This test has multiple defra nodes writing the same data to be read by another node
// This closely mimics the Shinzo setup, where multiple Indexers and Hosts will be writing the same data
// This test allows us to explore what the built-in `__version` field defra provides will look like in this case,
// Informing our design for the attestation system
// The key observation is that we receive a `[]Version` whose length is equal to the number of writers (and unique signatures)
func TestSyncFromMultipleWriters(t *testing.T) {
	ctx := context.Background()
	cfg := defra.DefaultConfig
	schema := "type User { name: String }"

	writerDefras := []*node.Node{}
	defraNodes := 5
	for i := 0; i < defraNodes; i++ {
		writerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
		require.NoError(t, err)
		defer writerDefra.Close(ctx)
		writerDefras = append(writerDefras, writerDefra)
	}

	// Create a reader instance
	readerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
	require.NoError(t, err)
	defer readerDefra.Close(ctx)

	// Connect reader to all writers
	for _, writer := range writerDefras {
		err := readerDefra.DB.Connect(ctx, writer.DB.PeerInfo())
		require.NoError(t, err)
	}

	// Write data from each writer
	for _, writer := range writerDefras {
		mutation := `mutation {
			create_User(input: { name: "Quinn" }) {
				name
			}
		}`

		type UserResult struct {
			Name string `json:"name"`
		}

		result, err := defra.PostMutation[UserResult](ctx, writer, mutation)
		require.NoError(t, err)
		require.Equal(t, "Quinn", result.Name)
	}

	// Wait for sync and verify reader has all data
	type UserWithVersion struct {
		Name    string                `json:"name"`
		Version []attestation.Version `json:"_version"`
	}

	var userWithVersion UserWithVersion
	var queryErr error
	for attempts := 1; attempts < 60; attempts++ {
		query := `query {
			User(limit: 1) {
				name
				_version {
					cid
					signature {
						type
						identity
						value
						__typename
					}
				}
			}
		}`

		userWithVersion, queryErr = defra.QuerySingle[UserWithVersion](ctx, readerDefra, query)
		if queryErr == nil && userWithVersion.Name == "Quinn" {
			break
		}
		t.Logf("Attempt %d to query username from readerDefra failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
	}

	require.NoError(t, queryErr)
	require.Equal(t, "Quinn", userWithVersion.Name)
	require.Equal(t, defraNodes, len(userWithVersion.Version))
}

// This test mimics TestSyncFromMultipleWriters and is also designed to help us explore Defra's built in `_version` attestation system
// In this test, unlike in TestSyncFromMultipleWriters, we have one of our writers write slightly different data
// We see that this gives us two separate documents in our collection, each with a version array of their own
func TestSyncFromMultipleWritersWithSomeOverlappingData(t *testing.T) {
	ctx := context.Background()
	cfg := defra.DefaultConfig
	schema := "type User { name: String, friends: [String] }"

	writerDefras := []*node.Node{}
	defraNodes := 5
	for i := 0; i < defraNodes; i++ {
		writerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
		require.NoError(t, err)
		defer writerDefra.Close(ctx)

		writerDefras = append(writerDefras, writerDefra)
	}

	// Create a reader instance
	readerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema), "User")
	require.NoError(t, err)
	defer readerDefra.Close(ctx)

	// Connect reader to all writers
	for _, writer := range writerDefras {
		err := readerDefra.DB.Connect(ctx, writer.DB.PeerInfo())
		require.NoError(t, err)
	}

	// Standard friends array for writers
	standardFriends := []string{"Alice", "Bob", "Charlie", "Diana"}
	// Modified friends array for 1 (malicious?) writer (remove "Bob", add "Eve" and "Frank")
	modifiedFriends := []string{"Alice", "Charlie", "Diana", "Eve", "Frank"}

	// Write data to each writer
	type UserResult struct {
		Name    string   `json:"name"`
		Friends []string `json:"friends"`
	}

	for i, writer := range writerDefras {
		var friendsStr string
		var friends []string
		if i == len(writerDefras)-1 {
			friends = modifiedFriends
			t.Logf("Writer %d posting with MODIFIED friends: %v", i+1, modifiedFriends)
		} else {
			friends = standardFriends
			t.Logf("Writer %d posting with STANDARD friends: %v", i+1, standardFriends)
		}

		// Format friends array for GraphQL
		friendsStr = "["
		for j, friend := range friends {
			if j > 0 {
				friendsStr += ", "
			}
			friendsStr += fmt.Sprintf(`"%s"`, friend)
		}
		friendsStr += "]"

		mutation := fmt.Sprintf(`mutation {
			create_User(input: { name: "Quinn", friends: %s }) {
				name
				friends
			}
		}`, friendsStr)

		result, err := defra.PostMutation[UserResult](ctx, writer, mutation)
		require.NoError(t, err)
		require.Equal(t, "Quinn", result.Name)
	}

	// Wait for sync
	time.Sleep(5 * time.Second)

	// Query all User entries to see how DefraDB handles the conflicting data
	query := `query {
		User {
			name
			friends
			_version {
				cid
				signature {
					identity
				}
			}
		}
	}`

	type UserWithFriendsAndVersion struct {
		Name    string                `json:"name"`
		Friends []string              `json:"friends"`
		Version []attestation.Version `json:"_version"`
	}

	users, err := defra.QueryArray[UserWithFriendsAndVersion](ctx, readerDefra, query)
	require.NoError(t, err)
	require.NotNil(t, users)
	require.Len(t, users, 2)

	for _, user := range users {
		if len(user.Friends) == 4 {
			require.Len(t, user.Version, defraNodes-1)
		} else if len(user.Friends) == 5 {
			require.Len(t, user.Version, 1)
		} else {
			t.Fatalf("Unexpected user object %+v", user)
		}
	}
}

// This test mimics TestSyncFromMultipleWritersWithSomeOverlappingData but adds in a vote_count [GCounter CRDT](https://github.com/sourcenetwork/defradb/tree/develop/internal/core/crdt#gcounter---increment-only-counter)
// Here we see that a malicious actor is able to manipulate the vote_count and increment it multiple times by themselves - giving the allusion of multiple attestations - by modifying the document in subsequent mutation queries
// We also notice that the compromised document (written by the malicious node) has the same number of `_version` instances as it was written to - giving us an avenue for detecting this, by checking the signing identities
// Simply checking the _version length is insufficient as an attestation system. We must also check the number of unique signers.
func TestSyncFromMultipleWritersWithSomeOverlappingDataAndVoteCounts(t *testing.T) {
	ctx := context.Background()
	cfg := defra.DefaultConfig
	schema := "type User { name: String, friends: [String], vote_count: Int @crdt(type: pcounter) }"

	writerDefras := []*node.Node{}
	defraNodes := 5
	for i := 0; i < defraNodes; i++ {
		writerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema))
		require.NoError(t, err)
		defer writerDefra.Close(ctx)

		err = writerDefra.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		writerDefras = append(writerDefras, writerDefra)
	}

	// Create a reader instance
	readerDefra, err := defra.StartDefraInstanceWithTestConfig(t, cfg, defra.NewSchemaApplierFromProvidedSchema(schema))
	require.NoError(t, err)
	defer readerDefra.Close(ctx)

	err = readerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	// Connect reader to all writers
	for _, writer := range writerDefras {
		err := readerDefra.DB.Connect(ctx, writer.DB.PeerInfo())
		require.NoError(t, err)
	}

	// Standard friends array for  writers
	standardFriends := []string{"Alice", "Bob", "Charlie", "Diana"}
	// Modified friends array for 1 (malicious) writer (remove "Bob", add "Eve" and "Frank")
	modifiedFriends := []string{"Alice", "Charlie", "Diana", "Eve", "Frank"}

	// Helper function to create or update user with vote count
	createOrUpdateUser := func(writer *node.Node, friends []string) error {
		// Format friends array
		friendsStr := "["
		for j, friend := range friends {
			if j > 0 {
				friendsStr += ", "
			}
			friendsStr += fmt.Sprintf(`"%s"`, friend)
		}
		friendsStr += "]"

		// Try to create
		createMutation := fmt.Sprintf(`mutation {
			create_User(input: { name: "Quinn", friends: %s, vote_count: 1 }) {
				name
				friends
				vote_count
			}
		}`, friendsStr)

		type UserResult struct {
			Name      string   `json:"name"`
			Friends   []string `json:"friends"`
			VoteCount int      `json:"vote_count"`
		}

		_, err := defra.PostMutation[UserResult](ctx, writer, createMutation)
		if err != nil {
			// Document exists, update it
			// Query for the document
			queryStr := `query {
				User(filter: {name: {_eq: "Quinn"}}) {
					_docID
					vote_count
					friends
				}
			}`

			type UserWithDocID struct {
				DocID     string   `json:"_docID"`
				VoteCount int      `json:"vote_count"`
				Friends   []string `json:"friends"`
			}

			users, err := defra.QueryArray[UserWithDocID](ctx, writer, queryStr)
			if err != nil {
				return err
			}

			// Find matching document by friends array
			var matchingDoc *UserWithDocID
			for i := range users {
				if len(users[i].Friends) == len(friends) {
					allMatch := true
					for j, friend := range friends {
						if j >= len(users[i].Friends) || users[i].Friends[j] != friend {
							allMatch = false
							break
						}
					}
					if allMatch {
						matchingDoc = &users[i]
						break
					}
				}
			}

			if matchingDoc != nil {
				// Update with increment of 1
				updateMutation := fmt.Sprintf(`mutation {
					update_User(docID: "%s", input: { vote_count: 1 }) {
						name
						friends
						vote_count
					}
				}`, matchingDoc.DocID)

				_, err = defra.PostMutation[UserResult](ctx, writer, updateMutation)
				return err
			}
		}
		return nil
	}

	// Write data to each writer
	for i, writer := range writerDefras {
		if i == len(writerDefras)-1 {
			// Last writer posts modified friends array 10 times (malicious behavior)
			t.Logf("Writer %d (MALICIOUS) posting with MODIFIED friends 10 times: %v", i+1, modifiedFriends)
			for j := 0; j < 10; j++ {
				err := createOrUpdateUser(writer, modifiedFriends)
				require.NoError(t, err)
				t.Logf("  Malicious writer post #%d complete", j+1)
			}
		} else {
			// First writers post standard friends array once
			t.Logf("Writer %d posting with STANDARD friends: %v", i+1, standardFriends)
			err := createOrUpdateUser(writer, standardFriends)
			require.NoError(t, err)
		}
	}

	// Wait for sync
	time.Sleep(5 * time.Second)

	// Query all User entries to see how DefraDB handles the conflicting data and CRDT counters
	query := `query {
		User {
			name
			friends
			vote_count
			_version {
				cid
				signature {
					identity
				}
			}
		}
	}`

	type UserWithFriendsVoteCountAndVersion struct {
		Name      string                `json:"name"`
		Friends   []string              `json:"friends"`
		VoteCount int                   `json:"vote_count"`
		Version   []attestation.Version `json:"_version"`
	}

	users, err := defra.QueryArray[UserWithFriendsVoteCountAndVersion](ctx, readerDefra, query)
	require.NoError(t, err)
	require.NotNil(t, users)
	require.Len(t, users, 2)

	for _, user := range users {
		if len(user.Friends) == 4 {
			require.Len(t, user.Version, defraNodes-1)
			require.Equal(t, defraNodes-1, user.VoteCount)
		} else if len(user.Friends) == 5 {
			require.Len(t, user.Version, 10)
			require.Equal(t, 10, user.VoteCount)
		} else {
			t.Fatalf("Unexpected user object %+v", user)
		}
	}
}
