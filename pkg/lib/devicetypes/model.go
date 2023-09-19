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

package devicetypes

import "github.com/SENERGY-Platform/models/go/models"

type DeviceTypeCommand struct {
	Command    Command    `json:"command"`
	Id         string     `json:"id"`
	Owner      string     `json:"owner"`
	DeviceType DeviceType `json:"device_type"`
}

type Command string

const (
	PutCommand    Command = "PUT"
	DeleteCommand Command = "DELETE"
	RightsCommand Command = "RIGHTS"
)

type DeviceType = models.DeviceType

type Service = models.Service

type Type = models.Type

const (
	String  = models.String
	Integer = models.Integer
	Float   = models.Float
	Boolean = models.Boolean

	List      = models.List
	Structure = models.Structure
)

type ContentVariable = models.ContentVariable
