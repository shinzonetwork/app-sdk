package attestation

import (
	"context"
	"fmt"
	"strings"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
)

func getAttestationRecordSDL(viewName string) string {
	// Omitting the IndexerSignature schema (even if included in schema/schema.graphql) causes an error because those objects are included in the AttestationRecord
	// If either AttestationRecord or IndexerSignature do not have unique names, we will get an error when trying to apply them as a schema (collection already exists error)
	// We want a separate collection of AttestationRecords for each View so that app clients don't receive all AttestationRecords, only those that are relevant to the collections/Views they care about - we can just append the View names as those must also be unique
	return fmt.Sprintf(`type AttestationRecord_%s {
		attested_doc: String
		source_doc: String
		CIDs: [String]
	}`, viewName)
}

type AttestationRecord struct {
	AttestedDocId string   `json:"attested_doc"`
	SourceDocId   string   `json:"source_doc"`
	CIDs          []string `json:"CIDs"`
}

func AddAttestationRecordCollection(ctx context.Context, defraNode *node.Node, associatedViewName string) error {
	collectionSDL := getAttestationRecordSDL(associatedViewName)
	_, err := defraNode.DB.AddSchema(ctx, collectionSDL)
	if err != nil {
		return fmt.Errorf("Error adding attestation record schema %s: %w", collectionSDL, err)
	}

	attestationRecords := fmt.Sprintf("AttestationRecord_%s", associatedViewName)
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

	query := fmt.Sprintf(`query {
        AttestationRecord_%s (filter: {attested_doc: {_in: [%s]}}) {
            attested_doc
            source_doc
            CIDs
        }
    }`, associatedViewName, inList)
	records, err := defra.QueryArray[AttestationRecord](ctx, defraNode, query)
	if err != nil {
		return nil, fmt.Errorf("Error fetching attestation record: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("No attestation records found with query: %s", query)
	}
	return records, nil
}
