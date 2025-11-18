package attestation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
)

// filterMinimumIndexerAttestations filters a slice of items based on a minimum attestation threshold.
// T must be a struct type that has a DocID field of type string.
func filterMinimumIndexerAttestations[T any](ctx context.Context, defraNode *node.Node, response []T, minimumAttestationThreshold uint, queryUsed string) ([]T, error) {
	if minimumAttestationThreshold == 0 {
		return response, nil
	}

	documentsById := map[string]T{}
	for _, value := range response {
		docId, err := getDocID[T](value)
		if err != nil {
			return nil, fmt.Errorf("error retrieving DocID: %w", err)
		}
		documentsById[docId] = value
	}

	viewName, err := extractCollectionNameFromQuery(queryUsed)
	if err != nil {
		return nil, fmt.Errorf("error extracting view name from query %s: %w", queryUsed, err)
	}

	// Build quoted doc IDs for GraphQL query (matching the pattern from GetAttestationRecords)
	quoted := make([]string, 0, len(documentsById))
	for id := range documentsById {
		quoted = append(quoted, fmt.Sprintf("\"%s\"", id))
	}
	docIdQueryParam := strings.Join(quoted, ", ")

	query := fmt.Sprintf(`query {
		AttestationRecord_%s(filter: {attested_doc: {_in: [%s]}}) {
			attested_doc
			source_doc
			CIDs
		}
	}`, viewName, docIdQueryParam)

	attestationRecords, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query, 0)
	if err != nil {
		return nil, fmt.Errorf("error querying attestation records: %w", err)
	}

	// Count unique CIDs per attested document using a map as a set
	// map[docID]map[CID]struct{} - outer map keyed by docID, inner map is a set of CIDs
	docIDToCIDSet := make(map[string]map[string]struct{})
	for _, record := range attestationRecords {
		docID := record.AttestedDocId

		// Initialize the set for this docID if it doesn't exist
		if docIDToCIDSet[docID] == nil {
			docIDToCIDSet[docID] = make(map[string]struct{})
		}

		// Add all CIDs from this record to the set for this docID
		// Using struct{}{} as the value since it takes zero bytes
		for _, cid := range record.CIDs {
			docIDToCIDSet[docID][cid] = struct{}{}
		}
	}

	// Filter response items based on unique CID count threshold
	newResponse := []T{}
	for docId, CIDs := range docIDToCIDSet {
		attestationCount := uint(len(CIDs))
		if attestationCount >= minimumAttestationThreshold {
			newResponse = append(newResponse, documentsById[docId])
		}
	}

	return newResponse, nil
}

// getDocID retrieves the DocID field from a value of type T using reflection.
// This helper function allows you to access the DocID field from any struct T
// that has a DocID field of type string.
func getDocID[T any](item T) (string, error) {
	val := reflect.ValueOf(item)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return "", fmt.Errorf("expected struct type, got %v", val.Kind())
	}

	docIDField := val.FieldByName("DocID")
	if !docIDField.IsValid() {
		return "", errors.New("struct does not have a DocID field")
	}

	if docIDField.Kind() != reflect.String {
		return "", fmt.Errorf("DocID field is not a string, got %v", docIDField.Kind())
	}

	return docIDField.String(), nil
}

// extractCollectionNameFromQuery extracts the collection/view name from a GraphQL query.
// It handles various query formats:
//   - "query { User { ... } }" -> "User"
//   - "{ User { ... } }" -> "User"
//   - "query { myAlias: User { ... } }" -> "User" (skips alias)
//   - Handles whitespace and formatting variations
func extractCollectionNameFromQuery(query string) (string, error) {
	if query == "" {
		return "", fmt.Errorf("query string is empty")
	}

	// Remove leading/trailing whitespace
	query = strings.TrimSpace(query)

	// Find the opening brace - could be after "query", "mutation", "subscription", or standalone
	openBraceIdx := -1
	queryLower := strings.ToLower(query)

	// Check if query starts with a GraphQL operation keyword
	if strings.HasPrefix(queryLower, "query ") {
		// Find the opening brace after "query"
		rest := query[6:] // Skip "query "
		openBraceIdx = strings.Index(rest, "{")
		if openBraceIdx != -1 {
			openBraceIdx += 6 // Adjust for the "query " we skipped
		}
	} else if strings.HasPrefix(queryLower, "mutation ") {
		rest := query[9:] // Skip "mutation "
		openBraceIdx = strings.Index(rest, "{")
		if openBraceIdx != -1 {
			openBraceIdx += 9
		}
	} else if strings.HasPrefix(queryLower, "subscription ") {
		rest := query[12:] // Skip "subscription "
		openBraceIdx = strings.Index(rest, "{")
		if openBraceIdx != -1 {
			openBraceIdx += 12
		}
	} else {
		// No operation keyword, look for opening brace directly
		openBraceIdx = strings.Index(query, "{")
	}

	if openBraceIdx == -1 {
		return "", fmt.Errorf("no opening brace found in query")
	}

	// Extract content after the opening brace
	content := query[openBraceIdx+1:]
	content = strings.TrimSpace(content)

	// Find the first identifier (collection name)
	// This could be an alias like "myAlias: CollectionName" or just "CollectionName"
	var collectionName strings.Builder
	inIdentifier := false
	afterColon := false // Track if we've passed a colon (meaning we're past an alias)

	for _, r := range content {
		// If we hit a colon, we know we're in an alias, skip to after the colon
		if r == ':' {
			afterColon = true
			collectionName.Reset()
			inIdentifier = false
			continue
		}

		// Skip whitespace until we find the actual collection name
		if !inIdentifier {
			if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
				continue
			}
			// Start of identifier (letter or underscore)
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' {
				// Only start collecting if we're after a colon (past alias) or if we haven't seen a colon yet
				if afterColon || !strings.Contains(content[:strings.IndexRune(content, r)], ":") {
					inIdentifier = true
					collectionName.WriteRune(r)
				}
			} else if r == '{' {
				// Nested brace before finding identifier
				break
			}
			continue
		}

		// We're in an identifier, continue collecting until we hit a non-identifier character
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			collectionName.WriteRune(r)
		} else {
			// End of identifier (hit whitespace, brace, parenthesis, etc.)
			break
		}
	}

	result := strings.TrimSpace(collectionName.String())
	if result == "" {
		return "", fmt.Errorf("could not extract collection name from query")
	}

	return result, nil
}
