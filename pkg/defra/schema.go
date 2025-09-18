package defra

import (
	"context"
	"fmt"
	"os"

	"github.com/shinzonetwork/app-sdk/pkg/file"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
)

type SchemaApplier interface {
	ApplySchema(ctx context.Context, defraNode *node.Node) error
}

type MockSchemaApplierThatSucceeds struct{}

func (schema *MockSchemaApplierThatSucceeds) ApplySchema(ctx context.Context, defraNode *node.Node) error {
	return nil
}

const defaultPath = "schema/schema.graphql"

type DefaultSchemaApplier struct {
	DefaultPath string
}

func (schema *DefaultSchemaApplier) ApplySchema(ctx context.Context, defraNode *node.Node) error {
	logger.Sugar.Debug("Applying schema...")

	if len(schema.DefaultPath) == 0 {
		schema.DefaultPath = defaultPath
	}

	schemaPath, err := file.FindFile(schema.DefaultPath)
	if err != nil {
		return fmt.Errorf("Failed to find schema file: %v", err)
	}

	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("Failed to read schema file: %v", err)
	}

	_, err = defraNode.DB.AddSchema(ctx, string(schemaBytes))
	return err
}
