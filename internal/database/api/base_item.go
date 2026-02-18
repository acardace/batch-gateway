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

// BaseIndexes contains common indexed fields for all database items.
// These fields are used for querying and filtering.
type BaseItem struct {
	ID string

	// TenantID is the identifier for multi-tenancy support.
	TenantID string

	// Expiry is the Unix timestamp (in seconds) when the item expires.
	Expiry int64

	// Tags are key-value pairs that enable filtering items based on their contents.
	Tags Tags

	// Spec contains the serialized specification/static data.
	Spec []byte

	// Status contains the serialized status/dynamic data.
	Status []byte
}
