package attestation

import (
	"context"
	"fmt"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/config"
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
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

// testSetup holds common test setup data
type testSetup struct {
	ctx       context.Context
	defraNode *node.Node
	query     string
}

// setupTestDefraInstance creates a Defra instance with the standard test schema
func setupTestDefraInstance(t *testing.T, cfg *config.Config) *testSetup {
	ctx := context.Background()

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

	if cfg == nil {
		cfg = defra.DefaultConfig
	}

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, cfg, schemaApplier, "User", "AttestationRecord_User")
	require.NoError(t, err)

	return &testSetup{
		ctx:       ctx,
		defraNode: defraNode,
		query:     `query { User { _docID name } }`,
	}
}

// createTestDocument creates a User document in Defra and returns its DocID
func createTestDocument(t *testing.T, setup *testSetup, name string) string {
	createDoc := fmt.Sprintf(`mutation {
		create_User(input: {name: "%s"}) {
			_docID
		}
	}`, name)

	doc, err := defra.PostMutation[struct {
		DocID string `json:"_docID"`
	}](setup.ctx, setup.defraNode, createDoc)
	require.NoError(t, err)
	require.NotNil(t, doc)

	return doc.DocID
}

// createAttestationRecord creates an attestation record for a document
func createAttestationRecord(t *testing.T, setup *testSetup, attestedDocID, sourceDoc string, cids []string) {
	cidsList := ""
	for i, cid := range cids {
		if i > 0 {
			cidsList += ", "
		}
		cidsList += fmt.Sprintf(`"%s"`, cid)
	}

	createRecord := fmt.Sprintf(`mutation {
		create_AttestationRecord_User(input: {
			attested_doc: "%s"
			source_doc: "%s"
			CIDs: [%s]
		}) {
			attested_doc
		}
	}`, attestedDocID, sourceDoc, cidsList)

	_, err := defra.PostMutation[AttestationRecord](setup.ctx, setup.defraNode, createRecord)
	require.NoError(t, err)
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

func TestFilterMinimumIndexerAttestations(t *testing.T) {
	setup := setupTestDefraInstance(t, nil)
	defer setup.defraNode.Close(setup.ctx)

	// Create test documents in Defra
	doc1ID := createTestDocument(t, setup, "Document 1")
	doc2ID := createTestDocument(t, setup, "Document 2")
	doc3ID := createTestDocument(t, setup, "Document 3")

	// Create test documents slice for filtering
	testDocs := []TestDocument{
		{DocID: doc1ID, Name: "Document 1"},
		{DocID: doc2ID, Name: "Document 2"},
		{DocID: doc3ID, Name: "Document 3"},
	}

	// Create attestation records
	// Document 1: 3 unique CIDs (should pass threshold of 2)
	createAttestationRecord(t, setup, doc1ID, "source-1", []string{"cid-1", "cid-2", "cid-3"})

	// Document 2: 2 unique CIDs initially, then add another record with one duplicate and one new CID
	// Total unique CIDs for doc-2: cid-4, cid-5, cid-6 (3 unique)
	createAttestationRecord(t, setup, doc2ID, "source-2", []string{"cid-4", "cid-5"})
	createAttestationRecord(t, setup, doc2ID, "source-2b", []string{"cid-4", "cid-6"})

	// Document 3: 1 unique CID (should fail threshold of 2)
	createAttestationRecord(t, setup, doc3ID, "source-3", []string{"cid-7"})

	t.Run("threshold of 0 returns all documents", func(t *testing.T) {
		result, err := filterMinimumIndexerAttestations(setup.ctx, setup.defraNode, testDocs, 0, setup.query)
		require.NoError(t, err)
		require.Len(t, result, 3)
	})

	t.Run("threshold of 2 filters correctly", func(t *testing.T) {
		result, err := filterMinimumIndexerAttestations(setup.ctx, setup.defraNode, testDocs, 2, setup.query)
		require.NoError(t, err)
		// Should include doc-1 (3 CIDs) and doc-2 (3 unique CIDs: cid-4, cid-5, cid-6)
		// Should exclude doc-3 (1 CID)
		require.Len(t, result, 2, "Expected 2 documents to pass threshold of 2")

		// Verify the correct documents are included
		docIDs := make(map[string]bool)
		for _, doc := range result {
			docIDs[doc.DocID] = true
		}
		require.True(t, docIDs[doc1ID], "doc-1 should be included (3 CIDs)")
		require.True(t, docIDs[doc2ID], "doc-2 should be included (3 unique CIDs)")
		require.False(t, docIDs[doc3ID], "doc-3 should be excluded (1 CID)")
	})

	t.Run("threshold of 3 filters correctly", func(t *testing.T) {
		result, err := filterMinimumIndexerAttestations(setup.ctx, setup.defraNode, testDocs, 3, setup.query)
		require.NoError(t, err)
		// Should include doc-1 (3 CIDs) and doc-2 (3 unique CIDs)
		// Should exclude doc-3 (1 CID)
		require.Len(t, result, 2, "Expected 2 documents to pass threshold of 3")

		docIDs := make(map[string]bool)
		for _, doc := range result {
			docIDs[doc.DocID] = true
		}
		require.True(t, docIDs[doc1ID], "doc-1 should be included (3 CIDs)")
		require.True(t, docIDs[doc2ID], "doc-2 should be included (3 unique CIDs)")
		require.False(t, docIDs[doc3ID], "doc-3 should be excluded (1 CID)")
	})

	t.Run("threshold of 4 filters out everything", func(t *testing.T) {
		result, err := filterMinimumIndexerAttestations(setup.ctx, setup.defraNode, testDocs, 4, setup.query)
		require.NoError(t, err)
		// No documents have 4+ unique CIDs
		require.Len(t, result, 0)
	})

	t.Run("document with no attestation records is filtered out", func(t *testing.T) {
		missingDocID := createTestDocument(t, setup, "Missing Document")
		docsWithMissing := []TestDocument{
			{DocID: doc1ID, Name: "Document 1"},
			{DocID: missingDocID, Name: "Missing Document"},
		}
		result, err := filterMinimumIndexerAttestations(setup.ctx, setup.defraNode, docsWithMissing, 1, setup.query)
		require.NoError(t, err)
		// missingDocID has 0 CIDs, so it should be filtered out with threshold of 1
		require.Len(t, result, 1)
		require.Equal(t, doc1ID, result[0].DocID)
	})

	t.Run("invalid query returns error", func(t *testing.T) {
		invalidQuery := "invalid query"
		result, err := filterMinimumIndexerAttestations(setup.ctx, setup.defraNode, testDocs, 1, invalidQuery)
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "extracting view name")
	})
}

func TestHasLimitParameter(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{
			name:     "query with limit",
			query:    `query { User(limit: 10) { name } }`,
			expected: true,
		},
		{
			name:     "query with limit and filter",
			query:    `query { User(filter: {name: {_eq: "Test"}}, limit: 5) { name } }`,
			expected: true,
		},
		{
			name:     "query without limit",
			query:    `query { User { name } }`,
			expected: false,
		},
		{
			name:     "query with uppercase LIMIT",
			query:    `query { User(LIMIT: 10) { name } }`,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasLimitParameter(tt.query)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractLimitValue(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected int
		found    bool
	}{
		{
			name:     "query with limit 10",
			query:    `query { User(limit: 10) { name } }`,
			expected: 10,
			found:    true,
		},
		{
			name:     "query with limit 5",
			query:    `query { User(limit: 5) { name } }`,
			expected: 5,
			found:    true,
		},
		{
			name:     "query with limit and filter",
			query:    `query { User(filter: {name: {_eq: "Test"}}, limit: 20) { name } }`,
			expected: 20,
			found:    true,
		},
		{
			name:     "query without limit",
			query:    `query { User { name } }`,
			expected: 0,
			found:    false,
		},
		{
			name:     "query with limit but no value",
			query:    `query { User(limit:) { name } }`,
			expected: 0,
			found:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limit, found := extractLimitValue(tt.query)
			require.Equal(t, tt.expected, limit)
			require.Equal(t, tt.found, found)
		})
	}
}

func TestQueryArrayWithAttestationFilter(t *testing.T) {
	setup := setupTestDefraInstance(t, nil)
	defer setup.defraNode.Close(setup.ctx)

	// Create test documents
	doc1ID := createTestDocument(t, setup, "Document 1")
	doc2ID := createTestDocument(t, setup, "Document 2")
	doc3ID := createTestDocument(t, setup, "Document 3")

	createAttestationRecord(t, setup, doc1ID, "source-1", []string{"cid-1", "cid-2", "cid-3"})
	createAttestationRecord(t, setup, doc2ID, "source-2", []string{"cid-4", "cid-5"})
	createAttestationRecord(t, setup, doc3ID, "source-3", []string{"cid-7"})

	t.Run("threshold of 0 returns all documents", func(t *testing.T) {
		result, err := QueryArrayWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query, 0)
		require.NoError(t, err)
		require.Len(t, result, 3)
	})
	t.Run("threshold of 1 returns all documents", func(t *testing.T) {
		result, err := QueryArrayWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query, 1)
		require.NoError(t, err)
		require.Len(t, result, 3)
	})

	t.Run("threshold of 2 filters correctly", func(t *testing.T) {
		result, err := QueryArrayWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query, 2)
		require.NoError(t, err)
		// Should include doc-1 (3 CIDs) and doc-2 (2 CIDs)
		// Should exclude doc-3 (1 CID)
		require.Len(t, result, 2)

		docIDs := make(map[string]bool)
		for _, doc := range result {
			docIDs[doc.DocID] = true
		}
		require.True(t, docIDs[doc1ID], "doc-1 should be included (3 CIDs)")
		require.True(t, docIDs[doc2ID], "doc-2 should be included (2 CIDs)")
		require.False(t, docIDs[doc3ID], "doc-3 should be excluded (1 CID)")
	})

	t.Run("threshold of 3 filters correctly", func(t *testing.T) {
		result, err := QueryArrayWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query, 3)
		require.NoError(t, err)
		// Should include only doc-1 (3 CIDs)
		require.Len(t, result, 1)
		require.Equal(t, doc1ID, result[0].DocID)
	})

	t.Run("threshold of 4 filters everything out", func(t *testing.T) {
		result, err := QueryArrayWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query, 4)
		require.NoError(t, err)
		require.Len(t, result, 0)
	})
}

func TestQueryArrayWithConfiguredAttestationFilter(t *testing.T) {
	// Create a config with minimum attestations threshold
	cfg := defra.DefaultConfig
	cfg.Shinzo.MinimumAttestations = "2"

	setup := setupTestDefraInstance(t, cfg)
	defer setup.defraNode.Close(setup.ctx)

	// Create test documents
	doc1ID := createTestDocument(t, setup, "Document 1")
	doc2ID := createTestDocument(t, setup, "Document 2")

	// Create attestation records
	// Document 1: 3 unique CIDs (meets threshold of 2)
	createAttestationRecord(t, setup, doc1ID, "source-1", []string{"cid-1", "cid-2", "cid-3"})
	// Document 2: 1 unique CID (doesn't meet threshold of 2)
	createAttestationRecord(t, setup, doc2ID, "source-2", []string{"cid-4"})

	t.Run("uses configured threshold", func(t *testing.T) {
		result, err := QueryArrayWithConfiguredAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query)
		require.NoError(t, err)
		// Should include only doc-1 (3 CIDs, meets threshold of 2)
		// Should exclude doc-2 (1 CID, doesn't meet threshold of 2)
		require.Len(t, result, 1)
		require.Equal(t, doc1ID, result[0].DocID)
	})
}

func TestQuerySingleWithAttestationFilter(t *testing.T) {
	t.Run("query without limit returns first matching result", func(t *testing.T) {
		setup := setupTestDefraInstance(t, nil)
		defer setup.defraNode.Close(setup.ctx)

		// Create test documents
		doc1ID := createTestDocument(t, setup, "Document 1")
		doc2ID := createTestDocument(t, setup, "Document 2")

		// Create attestation records
		// Document 1: 3 unique CIDs (meets threshold of 2)
		createAttestationRecord(t, setup, doc1ID, "source-1", []string{"cid-1", "cid-2", "cid-3"})
		// Document 2: 1 unique CID (doesn't meet threshold of 2)
		createAttestationRecord(t, setup, doc2ID, "source-2", []string{"cid-4"})

		result, err := QuerySingleWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query, 2)
		require.NoError(t, err)
		require.Equal(t, doc1ID, result.DocID)
		require.Equal(t, "Document 1", result.Name)
	})

	t.Run("query with limit returns first matching result", func(t *testing.T) {
		setup := setupTestDefraInstance(t, nil)
		defer setup.defraNode.Close(setup.ctx)

		// Create test documents
		doc1ID := createTestDocument(t, setup, "Document 1")
		doc2ID := createTestDocument(t, setup, "Document 2")

		// Create attestation records
		createAttestationRecord(t, setup, doc1ID, "source-1", []string{"cid-1", "cid-2", "cid-3"})
		createAttestationRecord(t, setup, doc2ID, "source-2", []string{"cid-4"})

		query := `query { User(limit: 10) { _docID name } }`
		result, err := QuerySingleWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, query, 2)
		require.NoError(t, err)
		require.Equal(t, doc1ID, result.DocID)
	})

	t.Run("query without limit and no matching results returns error", func(t *testing.T) {
		setup := setupTestDefraInstance(t, nil)
		defer setup.defraNode.Close(setup.ctx)

		// Create test documents
		doc1ID := createTestDocument(t, setup, "Document 1")
		doc2ID := createTestDocument(t, setup, "Document 2")

		// Create attestation records with low counts
		createAttestationRecord(t, setup, doc1ID, "source-1", []string{"cid-1"})
		createAttestationRecord(t, setup, doc2ID, "source-2", []string{"cid-2"})

		result, err := QuerySingleWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query, 10)
		require.Error(t, err)
		var zero TestDocument
		require.Equal(t, zero, result)
		require.Contains(t, err.Error(), "no results found that meet the minimum attestation threshold")
	})

	t.Run("changing threshold changes which result is returned", func(t *testing.T) {
		setup := setupTestDefraInstance(t, nil)
		defer setup.defraNode.Close(setup.ctx)

		// Create documents where the first one has fewer attestations than a later one
		// Document 1: 2 CIDs (meets threshold of 2, but not 3)
		doc1ID := createTestDocument(t, setup, "Document 1")
		createAttestationRecord(t, setup, doc1ID, "source-1", []string{"cid-1", "cid-2"})

		// Document 2: 1 CID (doesn't meet threshold of 2)
		doc2ID := createTestDocument(t, setup, "Document 2")
		createAttestationRecord(t, setup, doc2ID, "source-2", []string{"cid-3"})

		// Document 3: 5 CIDs (meets threshold of 3, highest)
		doc3ID := createTestDocument(t, setup, "Document 3")
		createAttestationRecord(t, setup, doc3ID, "source-3", []string{"cid-4", "cid-5", "cid-6", "cid-7", "cid-8"})

		// With threshold of 2, should return a document that meets threshold (doc1 or doc3)
		// Order is not guaranteed, so either is valid
		result, err := QuerySingleWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query, 2)
		require.NoError(t, err)
		require.True(t, result.DocID == doc1ID || result.DocID == doc3ID, 
			"With threshold 2, should return doc1 or doc3 (both meet threshold), got: %s", result.DocID)
		require.NotEqual(t, doc2ID, result.DocID, "Should not return doc2 (doesn't meet threshold of 2)")

		// With threshold of 3, should return doc3 (only one that meets threshold)
		result, err = QuerySingleWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query, 3)
		require.NoError(t, err)
		require.Equal(t, doc3ID, result.DocID, "With threshold 3, should return doc3 (only one that meets threshold)")
	})

	t.Run("query with limit increments to find result in large collection", func(t *testing.T) {
		setup := setupTestDefraInstance(t, nil)
		defer setup.defraNode.Close(setup.ctx)

		// Create many documents with low attestation counts
		// This will require incrementing the limit multiple times
		for i := 0; i < 150; i++ {
			docID := createTestDocument(t, setup, fmt.Sprintf("Low Attestation Doc %d", i))
			// Each has only 1 CID, so they won't meet threshold of 2
			createAttestationRecord(t, setup, docID, fmt.Sprintf("source-%d", i), []string{fmt.Sprintf("cid-low-%d", i)})
		}

		// Create a document with high attestations that will be found after incrementing limit
		highAttestationDocID := createTestDocument(t, setup, "High Attestation Document")
		// Give it 5 unique CIDs to meet threshold of 2
		createAttestationRecord(t, setup, highAttestationDocID, "source-high", []string{"cid-high-1", "cid-high-2", "cid-high-3", "cid-high-4", "cid-high-5"})

		// Query with a small limit - should increment and find the high attestation document
		query := `query { User(limit: 10) { _docID name } }`
		result, err := QuerySingleWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, query, 2)
		require.NoError(t, err)
		require.Equal(t, highAttestationDocID, result.DocID, "Should find the document with high attestations after incrementing limit")
		require.Equal(t, "High Attestation Document", result.Name)
	})

	t.Run("query with limit exhausts collection when no results meet threshold", func(t *testing.T) {
		setup := setupTestDefraInstance(t, nil)
		defer setup.defraNode.Close(setup.ctx)

		// Create many documents, all with low attestation counts
		for i := 0; i < 200; i++ {
			docID := createTestDocument(t, setup, fmt.Sprintf("Low Doc %d", i))
			// Each has only 1 CID, so they won't meet threshold of 2
			createAttestationRecord(t, setup, docID, fmt.Sprintf("source-%d", i), []string{fmt.Sprintf("cid-%d", i)})
		}

		// Query with a small limit - should increment until it exhausts the collection
		query := `query { User(limit: 10) { _docID name } }`
		result, err := QuerySingleWithAttestationFilter[TestDocument](setup.ctx, setup.defraNode, query, 2)
		require.Error(t, err)
		var zero TestDocument
		require.Equal(t, zero, result)
		require.Contains(t, err.Error(), "no results found that meet the minimum attestation threshold")
		require.Contains(t, err.Error(), "after querying entire collection")
	})
}

func TestQuerySingleWithConfiguredAttestationFilter(t *testing.T) {
	// Create a config with minimum attestations threshold
	cfg := defra.DefaultConfig
	cfg.Shinzo.MinimumAttestations = "2"

	t.Run("uses configured threshold and returns matching document", func(t *testing.T) {
		setup := setupTestDefraInstance(t, cfg)
		defer setup.defraNode.Close(setup.ctx)

		// Create test documents
		doc1ID := createTestDocument(t, setup, "Document 1")

		// Create attestation records
		// Document 1: 3 unique CIDs (meets threshold of 2)
		createAttestationRecord(t, setup, doc1ID, "source-1", []string{"cid-1", "cid-2", "cid-3"})

		result, err := QuerySingleWithConfiguredAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query)
		require.NoError(t, err)
		require.Equal(t, doc1ID, result.DocID)
		require.Equal(t, "Document 1", result.Name)
	})

	t.Run("returns error when no documents meet configured threshold", func(t *testing.T) {
		setup := setupTestDefraInstance(t, cfg)
		defer setup.defraNode.Close(setup.ctx)

		// Create test documents
		doc1ID := createTestDocument(t, setup, "Document 1")

		// Create attestation records
		// Document 1: 1 unique CID (doesn't meet threshold of 2)
		createAttestationRecord(t, setup, doc1ID, "source-1", []string{"cid-1"})

		result, err := QuerySingleWithConfiguredAttestationFilter[TestDocument](setup.ctx, setup.defraNode, setup.query)
		require.Error(t, err)
		var zero TestDocument
		require.Equal(t, zero, result)
		require.Contains(t, err.Error(), "no results found that meet the minimum attestation threshold")
	})
}
