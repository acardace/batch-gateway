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

import "fmt"

// BatchItem is the database item type
type BatchItem struct {
	BaseItem
}

// BatchDBClient is the typed database client for batch objects.
type BatchDBClient = DBClient[BatchItem]

// Validate validates a BatchItem for required fields.
func (b *BatchItem) Validate() error {
	if b == nil {
		return fmt.Errorf("item is nil")
	}
	if len(b.ID) == 0 {
		return fmt.Errorf("ID is empty")
	}
	return nil
}
