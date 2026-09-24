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

package columnnames

import (
	"strings"
	"testing"
)

// TestHashFieldNameIfNeededShortNameIsQuoted pins that a name within PostgreSQL's 63 byte
// identifier limit is returned unchanged apart from the surrounding double quotes, matching
// pkg/lib/handler.HashFieldNameIfNeeded's behavior before the move to this package.
func TestHashFieldNameIfNeededShortNameIsQuoted(t *testing.T) {
	actual := HashFieldNameIfNeeded("short_name")
	expected := "\"short_name\""
	if actual != expected {
		t.Error("Expected/Actual\n", expected, "\n", actual)
	}
}

// TestHashFieldNameIfNeeded63CharsIsNotHashed pins the boundary: a name of exactly 63 characters
// is still within the limit and must not be hashed.
func TestHashFieldNameIfNeeded63CharsIsNotHashed(t *testing.T) {
	name := strings.Repeat("a", 63)
	actual := HashFieldNameIfNeeded(name)
	expected := "\"" + name + "\""
	if actual != expected {
		t.Error("Expected/Actual\n", expected, "\n", actual)
	}
	if len(name) != 63 {
		t.Fatal("test setup broken: name is not 63 characters long")
	}
}

// TestHashFieldNameIfNeeded64CharsIsHashed pins the other side of the boundary: a name of 64
// characters exceeds the limit and is replaced by a quoted 62 character hex-encoded hash.
func TestHashFieldNameIfNeeded64CharsIsHashed(t *testing.T) {
	name := strings.Repeat("a", 64)
	actual := HashFieldNameIfNeeded(name)
	if !strings.HasPrefix(actual, "\"") || !strings.HasSuffix(actual, "\"") {
		t.Fatal("expected hashed name to be double-quoted, got", actual)
	}
	hash := strings.TrimSuffix(strings.TrimPrefix(actual, "\""), "\"")
	if len(hash) != 62 {
		t.Error("expected 62 hex characters, got", len(hash), "in", actual)
	}
	for _, r := range hash {
		if !strings.ContainsRune("0123456789abcdef", r) {
			t.Error("expected only lowercase hex characters, found", string(r), "in", hash)
			break
		}
	}
}

// TestHashFieldNameIfNeededIsStable pins that hashing the same over-long name twice yields the
// same result, since callers rely on the hash being deterministic per input.
func TestHashFieldNameIfNeededIsStable(t *testing.T) {
	name := strings.Repeat("b", 100)
	first := HashFieldNameIfNeeded(name)
	second := HashFieldNameIfNeeded(name)
	if first != second {
		t.Error("expected stable hash for same input\n", first, "\n", second)
	}
}
