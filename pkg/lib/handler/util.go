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

func parseContentVariable(c devicetypes.ContentVariable, path string) []string {
	s := []string{}
	prefix := path
	if len(prefix) > 0 {
		prefix += "."
	}
	prefix += c.Name
	switch c.Type {
	case devicetypes.String:
		s = append(s, "\""+prefix+"\" text NULL")
	case devicetypes.Boolean:
		s = append(s, "\""+prefix+"\" bool NULL")
	case devicetypes.Float:
		s = append(s, "\""+prefix+"\" double PRECISION NULL")
	case devicetypes.Integer:
		s = append(s, "\""+prefix+"\" bigint NULL")
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
		outputStrings = append(outputStrings, parseContentVariable(c.Outputs[i].ContentVariable, "")...)
	}
	return hashStringSlice(outputStrings)
}

func hashStringSlice(c []string) string {
	base := strings.Join(c, ",")
	return fmt.Sprintf("%X", sha256.Sum256([]byte(base)))
}
