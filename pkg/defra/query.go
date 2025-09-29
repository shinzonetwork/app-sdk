package defra

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/sourcenetwork/defradb/node"
)

// queryClient provides a clean interface for executing GraphQL queries against DefraDB using the direct client
type queryClient struct {
	defraNode *node.Node
}

// newQueryClient creates a new GraphQL query client using the Defra node directly
func newQueryClient(defraNode *node.Node) (*queryClient, error) {
	if defraNode == nil {
		return nil, fmt.Errorf("defraNode parameter cannot be nil")
	}

	return &queryClient{
		defraNode: defraNode,
	}, nil
}

// query executes a GraphQL query using the Defra client directly and returns the raw result
func (c *queryClient) query(ctx context.Context, query string) (interface{}, error) {
	if query == "" {
		return nil, fmt.Errorf("query parameter is empty")
	}

	result := c.defraNode.DB.ExecRequest(ctx, query)
	gqlResult := result.GQL

	if len(gqlResult.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", gqlResult.Errors)
	}

	return gqlResult.Data, nil
}

// queryAndUnmarshal executes a GraphQL query and unmarshals the result into the provided interface
func (c *queryClient) queryAndUnmarshal(ctx context.Context, query string, result interface{}) error {
	data, err := c.query(ctx, query)
	if err != nil {
		return err
	}

	// Convert the data to JSON and then unmarshal into the result
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	return json.Unmarshal(dataBytes, result)
}

// getDataField extracts the data from a GraphQL response
// For the Defra client, the data is returned directly, not wrapped in a "data" field
func (c *queryClient) getDataField(ctx context.Context, query string) (map[string]interface{}, error) {
	data, err := c.query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected data format: %T", data)
	}

	return dataMap, nil
}

// queryInto executes a GraphQL query and unmarshals the result into a struct of the specified type
func (c *queryClient) queryInto(ctx context.Context, query string, result interface{}) error {
	data, err := c.query(ctx, query)
	if err != nil {
		return err
	}

	// Convert the data to JSON and then unmarshal into the result
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	return json.Unmarshal(dataBytes, result)
}

// queryDataInto executes a GraphQL query and unmarshals only the "data" field into a struct
// This function handles both single objects and arrays in the response
func (c *queryClient) queryDataInto(ctx context.Context, query string, result interface{}) error {
	data, err := c.query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Check if result is expecting a slice (array) or single object
	resultValue := reflect.ValueOf(result)
	if resultValue.Kind() != reflect.Ptr {
		return fmt.Errorf("result must be a pointer")
	}

	resultElem := resultValue.Elem()

	// If result is a slice, find the first array in data and unmarshal it
	if resultElem.Kind() == reflect.Slice {
		if dataMap, ok := data.(map[string]interface{}); ok {
			for _, value := range dataMap {
				// Try different array types
				if array, ok := value.([]interface{}); ok {
					// Convert the array to JSON and unmarshal into result
					arrayBytes, err := json.Marshal(array)
					if err != nil {
						return fmt.Errorf("failed to marshal array: %w", err)
					}
					return json.Unmarshal(arrayBytes, result)
				}

				// Try []map[string]interface{} type
				if array, ok := value.([]map[string]interface{}); ok {
					// Convert the array to JSON and unmarshal into result
					arrayBytes, err := json.Marshal(array)
					if err != nil {
						return fmt.Errorf("failed to marshal array: %w", err)
					}
					return json.Unmarshal(arrayBytes, result)
				}
			}
		}
		// Fallback: try to unmarshal the entire data object
		dataBytes, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal data: %w", err)
		}
		return json.Unmarshal(dataBytes, result)
	}

	// If result is a single struct, find the first array in data and get its first element
	if dataMap, ok := data.(map[string]interface{}); ok {
		for _, value := range dataMap {
			// Try different array types
			if array, ok := value.([]interface{}); ok && len(array) > 0 {
				// Convert the first element to JSON and unmarshal into result
				firstElementBytes, err := json.Marshal(array[0])
				if err != nil {
					return fmt.Errorf("failed to marshal first element: %w", err)
				}
				return json.Unmarshal(firstElementBytes, result)
			}

			// Try []map[string]interface{} type
			if array, ok := value.([]map[string]interface{}); ok && len(array) > 0 {
				// Convert the first element to JSON and unmarshal into result
				firstElementBytes, err := json.Marshal(array[0])
				if err != nil {
					return fmt.Errorf("failed to marshal first element: %w", err)
				}
				return json.Unmarshal(firstElementBytes, result)
			}
		}
	}

	// Fallback: try to unmarshal the entire data object
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	return json.Unmarshal(dataBytes, result)
}

// wrapQueryIfNeeded automatically wraps a query with "query { }" if it doesn't already start with "query", "mutation", or "subscription"
func wrapQueryIfNeeded(query string) string {
	// Trim whitespace to check the actual start
	trimmed := strings.TrimSpace(query)

	// Check if query already starts with GraphQL operation keywords (case insensitive)
	if len(trimmed) >= 5 {
		lowerStart := strings.ToLower(trimmed[:5])
		if lowerStart == "query" || lowerStart == "mutat" || lowerStart == "subsc" {
			return query // Return original query as-is
		}
	}

	// Check for "mutation" and "subscription" (longer keywords)
	if len(trimmed) >= 8 {
		lowerStart := strings.ToLower(trimmed[:8])
		if lowerStart == "mutation" {
			return query // Return original query as-is
		}
	}

	if len(trimmed) >= 11 {
		lowerStart := strings.ToLower(trimmed[:11])
		if lowerStart == "subscription" {
			return query // Return original query as-is
		}
	}

	// Wrap the query with "query { }"
	return fmt.Sprintf("query { %s }", query)
}

// QuerySingle executes a GraphQL query and returns a single item of the specified type
// This is useful when you expect a single object back (not an array)
func QuerySingle[T any](ctx context.Context, defraNode *node.Node, query string) (T, error) {
	var result T
	client, err := newQueryClient(defraNode)
	if err != nil {
		return result, err
	}

	// Auto-wrap query if it doesn't start with "query"
	wrappedQuery := wrapQueryIfNeeded(query)
	err = client.queryDataInto(ctx, wrappedQuery, &result)
	return result, err
}

// QueryArray executes a GraphQL query and returns an array of the specified type
// This is useful when you expect an array of objects back
func QueryArray[T any](ctx context.Context, defraNode *node.Node, query string) ([]T, error) {
	var result []T
	client, err := newQueryClient(defraNode)
	if err != nil {
		return result, err
	}

	// Auto-wrap query if it doesn't start with "query"
	wrappedQuery := wrapQueryIfNeeded(query)
	err = client.queryDataInto(ctx, wrappedQuery, &result)
	return result, err
}
