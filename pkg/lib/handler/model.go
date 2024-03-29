/*
 * Copyright 2023 InfAI (CC SES)
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

type TableEditMessage struct {
	Method string   `json:"method"` // "put" or "delete"
	Tables []string `json:"tables"`
}

type fieldDescription struct {
	ColumnName string
	Nullable   bool
	DataType   string
}

func (f *fieldDescription) String() string {
	s := f.ColumnName + " " + f.DataType + " "
	if f.Nullable {
		s += "NULL"
	} else {
		s += "NOT NULL"
	}
	return s
}
