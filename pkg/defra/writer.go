package defra

import (
	"context"
	"fmt"
	"strings"

	"github.com/sourcenetwork/defradb/node"
)

func PostMutation[T any](ctx context.Context, defraNode *node.Node, query string) (*T, error) {
	if !strings.Contains(query, "mutation") {
		return nil, fmt.Errorf("Query must be a mutation, given: %s", query)
	}

	result := defraNode.DB.ExecRequest(ctx, query)
	gqlResult := result.GQL
	if gqlResult.Data == nil {
		return nil, fmt.Errorf("Encountered errors posting mutation: %v", gqlResult.Errors)
	}
	resultAsT, ok := gqlResult.Data.(T)
	if ok {
		return &resultAsT, nil
	}
	return nil, fmt.Errorf("Encountered errors posting mutation: %v", gqlResult.Errors)
}
