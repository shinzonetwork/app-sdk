package tests

import (
	"fmt"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/attestation"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	hostAttestation "github.com/shinzonetwork/shinzo-host-client/pkg/attestation"
	"github.com/stretchr/testify/require"
)

// These tests are located here, instead of the attestation package, so that we can import the attestation posting methods from the host without an import cycle
func TestGetAttestationRecords(t *testing.T) {
	ctx := t.Context()
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema(`type AttestationRecord_SampleView {
	attested_doc: String
    source_doc: String
	CIDs: [String]
}`), "AttestationRecord_SampleView")
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	for i := 0; i < 10; i++ {
		docId := fmt.Sprintf("ArbitraryDocId: %d", i+1)
		sourceDocId := fmt.Sprintf("ArbitrarySourceDocId: %d", i+1)
		versions := []attestation.Version{
			attestation.Version{CID: "Some CID", Signature: attestation.Signature{Type: "Some Type", Identity: "Some Identity", Value: fmt.Sprintf("Some Value %d", i+1)}},
			attestation.Version{CID: "Some Other CID", Signature: attestation.Signature{Type: "Some Type", Identity: "Some Other Identity", Value: fmt.Sprintf("Some Other Value %d", i+1)}},
		}
		record, err := hostAttestation.CreateAttestationRecord(docId, sourceDocId, versions)
		require.NoError(t, err)
		err = record.PostAttestationRecord(ctx, defraNode, "SampleView")
		require.NoError(t, err)
	}

	records, err := attestation.GetAttestationRecords(ctx, defraNode, "SampleView", []string{"ArbitraryDocId: 1", "ArbitraryDocId: 7"})
	require.NoError(t, err)
	require.NotNil(t, records)
	require.Len(t, records, 2)
	for _, record := range records {
		if record.AttestedDocId != "ArbitraryDocId: 1" && record.AttestedDocId != "ArbitraryDocId: 7" {
			t.Fatalf("Encountered unexpected AttestedDocId: %s", record.AttestedDocId)
		}
		require.Len(t, record.CIDs, 2)
	}
}
