package attestation

import (
	"context"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/stretchr/testify/require"
)

// TestDocument is a test struct with a DocID field
type TestDocument struct {
	DocID string `json:"_docID"`
	Name  string `json:"name"`
}

// TestDocumentPtr is a test struct with a DocID field (pointer version)
type TestDocumentPtr struct {
	DocID *string `json:"_docID"`
	Name  string  `json:"name"`
}

// TestDocumentNoDocID is a test struct without a DocID field
type TestDocumentNoDocID struct {
	Name string `json:"name"`
}

func TestExtractCollectionNameFromQuery(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expected    string
		expectError bool
	}{
		{
			name:     "simple query with User",
			query:    `query { User { name } }`,
			expected: "User",
		},
		{
			name:     "query without operation keyword",
			query:    `{ User { name } }`,
			expected: "User",
		},
		{
			name:     "query with alias",
			query:    `query { myAlias: User { name } }`,
			expected: "User",
		},
		{
			name:     "query with subscription",
			query:    `subscription { User { name } }`,
			expected: "User",
		},
		{
			name:     "query with filter",
			query:    `query { User(filter: {name: {_eq: "Test"}}) { name } }`,
			expected: "User",
		},
		{
			name:     "query with multiple spaces",
			query:    `query   {   User   {   name   }   }`,
			expected: "User",
		},
		{
			name:     "query with newlines",
			query:    "query {\n\tUser {\n\t\tname\n\t}\n}",
			expected: "User",
		},
		{
			name:     "query with underscore in name",
			query:    `query { AttestationRecord_ViewName { attested_doc } }`,
			expected: "AttestationRecord_ViewName",
		},
		{
			name:     "query with numbers in name",
			query:    `query { User123 { name } }`,
			expected: "User123",
		},
		{
			name:        "empty query",
			query:       "",
			expectError: true,
		},
		{
			name:        "query without opening brace",
			query:       "query User name",
			expectError: true,
		},
		{
			name:        "query with only whitespace",
			query:       "   ",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractCollectionNameFromQuery(tt.query)
			if tt.expectError {
				require.Error(t, err)
				require.Empty(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetDocID(t *testing.T) {
	t.Run("struct with string DocID", func(t *testing.T) {
		doc := TestDocument{
			DocID: "test-doc-id-123",
			Name:  "Test Document",
		}
		docID, err := getDocID(doc)
		require.NoError(t, err)
		require.Equal(t, "test-doc-id-123", docID)
	})

	t.Run("pointer to struct with string DocID", func(t *testing.T) {
		doc := &TestDocument{
			DocID: "test-doc-id-456",
			Name:  "Test Document Ptr",
		}
		docID, err := getDocID(doc)
		require.NoError(t, err)
		require.Equal(t, "test-doc-id-456", docID)
	})

	t.Run("struct without DocID field", func(t *testing.T) {
		doc := TestDocumentNoDocID{
			Name: "No DocID",
		}
		docID, err := getDocID(doc)
		require.Error(t, err)
		require.Empty(t, docID)
		require.Contains(t, err.Error(), "does not have a DocID field")
	})

	t.Run("non-struct type", func(t *testing.T) {
		var str string = "not a struct"
		docID, err := getDocID(str)
		require.Error(t, err)
		require.Empty(t, docID)
		require.Contains(t, err.Error(), "expected struct type")
	})
}

func TestfilterMinimumIndexerAttestations(t *testing.T) {
	ctx := context.Background()

	// Set up test Defra instance with schema
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(`
		type User {
			name: String
		}
		type AttestationRecord_User {
			attested_doc: String
			source_doc: String
			CIDs: [String]
		}
	`)

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, schemaApplier, "User", "AttestationRecord_User")
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// Create some test documents
	testDocs := []TestDocument{
		{DocID: "doc-1", Name: "Document 1"},
		{DocID: "doc-2", Name: "Document 2"},
		{DocID: "doc-3", Name: "Document 3"},
	}

	// Create attestation records using mutations
	// Document 1: 3 unique CIDs (should pass threshold of 2)
	createRecord1 := `mutation {
		create_AttestationRecord_User(input: {
			attested_doc: "doc-1"
			source_doc: "source-1"
			CIDs: ["cid-1", "cid-2", "cid-3"]
		}) {
			attested_doc
		}
	}`
	_, err = defra.PostMutation[AttestationRecord](ctx, defraNode, createRecord1)
	require.NoError(t, err)

	// Document 2: 2 unique CIDs initially (should pass threshold of 2)
	createRecord2 := `mutation {
		create_AttestationRecord_User(input: {
			attested_doc: "doc-2"
			source_doc: "source-2"
			CIDs: ["cid-4", "cid-5"]
		}) {
			attested_doc
		}
	}`
	_, err = defra.PostMutation[AttestationRecord](ctx, defraNode, createRecord2)
	require.NoError(t, err)

	// Document 2: Add another record with one duplicate and one new CID
	// Total unique CIDs for doc-2: cid-4, cid-5, cid-6 (3 unique)
	createRecord2b := `mutation {
		create_AttestationRecord_User(input: {
			attested_doc: "doc-2"
			source_doc: "source-2b"
			CIDs: ["cid-4", "cid-6"]
		}) {
			attested_doc
		}
	}`
	_, err = defra.PostMutation[AttestationRecord](ctx, defraNode, createRecord2b)
	require.NoError(t, err)

	// Document 3: 1 unique CID (should fail threshold of 2)
	createRecord3 := `mutation {
		create_AttestationRecord_User(input: {
			attested_doc: "doc-3"
			source_doc: "source-3"
			CIDs: ["cid-7"]
		}) {
			attested_doc
		}
	}`
	_, err = defra.PostMutation[AttestationRecord](ctx, defraNode, createRecord3)
	require.NoError(t, err)

	query := `query { User { _docID name } }`

	t.Run("threshold of 0 returns all documents", func(t *testing.T) {
		result, err := filterMinimumIndexerAttestations(ctx, defraNode, testDocs, 0, query)
		require.NoError(t, err)
		require.Len(t, result, 3)
	})

	t.Run("threshold of 2 filters correctly", func(t *testing.T) {
		result, err := filterMinimumIndexerAttestations(ctx, defraNode, testDocs, 2, query)
		require.NoError(t, err)
		// Should include doc-1 (3 CIDs) and doc-2 (3 unique CIDs: cid-4, cid-5, cid-6)
		// Should exclude doc-3 (1 CID)
		require.Len(t, result, 2, "Expected 2 documents to pass threshold of 2")

		// Verify the correct documents are included
		docIDs := make(map[string]bool)
		for _, doc := range result {
			docIDs[doc.DocID] = true
		}
		require.True(t, docIDs["doc-1"], "doc-1 should be included (3 CIDs)")
		require.True(t, docIDs["doc-2"], "doc-2 should be included (3 unique CIDs)")
		require.False(t, docIDs["doc-3"], "doc-3 should be excluded (1 CID)")
	})

	t.Run("threshold of 3 filters correctly", func(t *testing.T) {
		result, err := filterMinimumIndexerAttestations(ctx, defraNode, testDocs, 3, query)
		require.NoError(t, err)
		// Should include doc-1 (3 CIDs) and doc-2 (3 unique CIDs)
		// Should exclude doc-3 (1 CID)
		require.Len(t, result, 2, "Expected 2 documents to pass threshold of 3")

		docIDs := make(map[string]bool)
		for _, doc := range result {
			docIDs[doc.DocID] = true
		}
		require.True(t, docIDs["doc-1"], "doc-1 should be included (3 CIDs)")
		require.True(t, docIDs["doc-2"], "doc-2 should be included (3 unique CIDs)")
		require.False(t, docIDs["doc-3"], "doc-3 should be excluded (1 CID)")
	})

	t.Run("threshold of 4 filters out everything", func(t *testing.T) {
		result, err := filterMinimumIndexerAttestations(ctx, defraNode, testDocs, 4, query)
		require.NoError(t, err)
		// No documents have 4+ unique CIDs
		require.Len(t, result, 0)
	})

	t.Run("document with no attestation records is filtered out", func(t *testing.T) {
		docsWithMissing := []TestDocument{
			{DocID: "doc-1", Name: "Document 1"},
			{DocID: "doc-missing", Name: "Missing Document"},
		}
		result, err := filterMinimumIndexerAttestations(ctx, defraNode, docsWithMissing, 1, query)
		require.NoError(t, err)
		// doc-missing has 0 CIDs, so it should be filtered out with threshold of 1
		require.Len(t, result, 1)
		require.Equal(t, "doc-1", result[0].DocID)
	})

	t.Run("invalid query returns error", func(t *testing.T) {
		invalidQuery := "invalid query"
		result, err := filterMinimumIndexerAttestations(ctx, defraNode, testDocs, 1, invalidQuery)
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "extracting view name")
	})
}
