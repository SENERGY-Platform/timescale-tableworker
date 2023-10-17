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

package handler

import (
	"crypto/sha256"
	"fmt"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/lib/devicetypes"
	"log"
	"sort"
	"strings"
)

func parseContentVariable(c devicetypes.ContentVariable, path string) []fieldDescription {
	s := []fieldDescription{}
	prefix := path
	if len(prefix) > 0 {
		prefix += "."
	}
	prefix += c.Name
	switch c.Type {
	case devicetypes.String:
		s = append(s, fieldDescription{
			ColumnName: "\"" + prefix + "\"",
			Nullable:   true,
			DataType:   "text",
		})
	case devicetypes.Boolean:
		s = append(s, fieldDescription{
			ColumnName: "\"" + prefix + "\"",
			Nullable:   true,
			DataType:   "bool",
		})

	case devicetypes.Float:
		s = append(s, fieldDescription{
			ColumnName: "\"" + prefix + "\"",
			Nullable:   true,
			DataType:   "double PRECISION",
		})
	case devicetypes.Integer:
		s = append(s, fieldDescription{
			ColumnName: "\"" + prefix + "\"",
			Nullable:   true,
			DataType:   "bigint",
		})
	case devicetypes.Structure:
		sort.SliceStable(c.SubContentVariables, func(i, j int) bool { // guarantees same results for different orders
			return c.SubContentVariables[i].Id < c.SubContentVariables[j].Id
		})
		for _, sub := range c.SubContentVariables {
			s = append(s, parseContentVariable(sub, prefix)...)
		}
	case devicetypes.List:
		log.Println("WARN: creating fields for list type not supported yet, skipping!")
	}
	return s
}

func hashServiceOutputs(c devicetypes.Service) string {
	outputStrings := []string{}
	for i := range c.Outputs {
		descs := parseContentVariable(c.Outputs[i].ContentVariable, "")
		for _, desc := range descs {
			outputStrings = append(outputStrings, desc.String())
		}
	}
	return hashStringSlice(outputStrings)
}

func hashStringSlice(c []string) string {
	base := strings.Join(c, ",")
	return fmt.Sprintf("%X", sha256.Sum256([]byte(base)))
}

func compareFds(base []fieldDescription, update []fieldDescription) (added []fieldDescription, removed []fieldDescription, newType []fieldDescription, setNotNull []fieldDescription, dropNotNull []fieldDescription) {
	for _, b := range base {
		found := false
		for _, u := range update {
			if b.ColumnName == u.ColumnName {
				found = true
				// check type and nullable
				if b.DataType != u.DataType {
					newType = append(newType, u)
				}
				if b.Nullable != u.Nullable {
					if u.Nullable {
						dropNotNull = append(dropNotNull, u)
					} else {
						setNotNull = append(setNotNull, u)
					}
				}
				break
			}
		}
		if !found {
			removed = append(removed, b)
		}
	}
	// find new
	for _, u := range update {
		found := false
		for _, b := range base {
			if b.ColumnName == u.ColumnName {
				found = true
				break
			}
		}
		if !found {
			added = append(added, u)
		}
	}

	return added, removed, newType, setNotNull, dropNotNull
}
