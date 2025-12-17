package attestation

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
)

func getAttestationRecordSDL() string {
	// Always create a single AttestationRecord collection with consistent schema
	// The docType field allows filtering by document type instead of separate collections
	return `type AttestationRecord {
		attested_doc: String @index
		source_doc: String @index  
		CIDs: [String]
		docType: String @index
		count: Int @crdt(type: pcounter)
	}`
}

type AttestationRecord struct {
    AttestedDocId string   `json:"attested_doc"`
    SourceDocId   string   `json:"source_doc"`
    CIDs          []string `json:"CIDs"`
    DocType       string   `json:"docType"`      // NEW
    Count         int      `json:"count"`        // NEW
}

func AddAttestationRecordCollection(ctx context.Context, defraNode *node.Node, associatedViewName string) error {
	// Ignore the associatedViewName parameter - always create single AttestationRecord collection
	collectionSDL := getAttestationRecordSDL()
	schemaApplier := defra.NewSchemaApplierFromProvidedSchema(collectionSDL)
	err := schemaApplier.ApplySchema(ctx, defraNode)
	if err != nil {
		return fmt.Errorf("Error adding attestation record schema %s: %w", collectionSDL, err)
	}

	// Always use exactly "AttestationRecord" as collection name
	attestationRecords := "AttestationRecord"
	err = defraNode.DB.AddP2PCollections(ctx, attestationRecords)
	if err != nil {
		return fmt.Errorf("Error subscribing to collection %s: %v", attestationRecords, err)
	}
	return nil
}

func GetAttestationRecords(ctx context.Context, defraNode *node.Node, associatedViewName string, viewDocIds []string) ([]AttestationRecord, error) {
	// Build a comma-separated list of quoted doc IDs for GraphQL _in filter
	quoted := make([]string, 0, len(viewDocIds))
	for _, id := range viewDocIds {
		quoted = append(quoted, fmt.Sprintf("\"%s\"", id))
	}
	inList := strings.Join(quoted, ", ")

	// Query the single AttestationRecord collection
	// Can optionally filter by docType if needed based on associatedViewName
	query := fmt.Sprintf(`query {
        AttestationRecord (filter: {attested_doc: {_in: [%s]}}) {
            attested_doc
            source_doc
            CIDs
            docType
            count
        }
    }`, inList)
	records, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	if err != nil {
		return nil, fmt.Errorf("Error fetching attestation record: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("No attestation records found with query: %s", query)
	}
	return records, nil
}

// extractSchemaTypes extracts all type names from a GraphQL SDL schema
func extractSchemaTypes(schema string) ([]string, error) {
	// Find all type definitions: type TypeName { ... }
	re := regexp.MustCompile(`type\s+(\w+)\s*@?[^{]*\{`)
	matches := re.FindAllStringSubmatch(schema, -1)

	if len(matches) == 0 {
		return nil, fmt.Errorf("no type definitions found in schema")
	}

	types := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			typeName := strings.TrimSpace(match[1])
			types = append(types, typeName)
		}
	}

	return types, nil
}
