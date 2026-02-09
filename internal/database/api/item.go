/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api

// BaseItem wraps a domain object T with multi-tenancy and tagging support.
//
// T should be a complete API object that includes its own ID field
// (e.g., openai.Batch, openai.FileObject). BaseItem adds platform-level
// concerns like tenant isolation and tag-based filtering.
//
// Example usage:
//
//	// Define typed DB clients for your domain
//	type BatchDBClient = api.DBClient[openai.Batch]
//	type FileDBClient = api.DBClient[openai.FileObject]
//
//	// Create and store items
//	batch := &api.BaseItem[openai.Batch]{
//	    TenantID: "tenant-123",
//	    Tags:   api.Tags{"type": "batch"},
//	    Item:   openai.Batch{ID: "batch-456", ...},
//	}
//	dbClient.Store(ctx, batch)
type BaseItem[T any] struct {
	// TenantID is the identifier for multi-tenancy support.
	TenantID string

	// Tags are key-value pairs that enable filtering items based on their contents.
	Tags Tags

	// Item contains the domain object (e.g., openai.Batch, openai.FileObject).
	Item T
}
