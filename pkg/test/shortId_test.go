/*
 * Copyright 2021 InfAI (CC SES)
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

package test

import (
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/devicetypes"
	"testing"
)

func TestShortenId(t *testing.T) {
	t.Run("Test ShortenId", func(t *testing.T) {
		actual, err := devicetypes.ShortenId("urn:infai:ses:device:d42d8d24-f2a2-4dd7-8ad3-4cabfb6f8062")
		if err != nil {
			t.Error(err.Error())
		}
		expected := "1C2NJPKiTdeK00yr-2-AYg"
		if actual != expected {
			t.Error("Mismatched shortId. Expected/Actual", expected, actual)
		}

		actual, err = devicetypes.ShortenId("urn:infai:ses:device:e3a9d39f-d833-45df-81c0-e479d17c2e06")
		if err != nil {
			t.Error(err.Error())
		}
		expected = "46nTn9gzRd-BwOR50XwuBg"
		if actual != expected {
			t.Error("Mismatched shortId. Expected/Actual", expected, actual)
		}
	})
}
