/*
 * Copyright 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package columnnames builds the TimescaleDB column names timescale-tableworker derives from
// device-type content variable paths. pkg/lib/handler's HashFieldNameIfNeeded delegates here
// instead of keeping a second copy of this logic.
//
// This is a leaf package on purpose: it imports nothing beyond the standard library, so a caller
// outside this module can compute the same column names without also pulling in the worker's
// device-type models, database driver and other service dependencies.
package columnnames

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashFieldNameIfNeeded returns name as a double-quoted SQL identifier, or, if name is longer than
// PostgreSQL's 63 byte identifier limit, a double-quoted 62 character hex-encoded SHA-256 hash of
// name instead.
func HashFieldNameIfNeeded(name string) string {
	if len(name) > 63 {
		sum := sha256.Sum256([]byte(name)) // 32 bytes
		truncated := sum[:31]              // 31 bytes -> 62 hex chars
		return "\"" + hex.EncodeToString(truncated) + "\""
	}
	return "\"" + name + "\""
}
