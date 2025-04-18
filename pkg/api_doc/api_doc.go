/*
 * Copyright 2025 InfAI (CC SES)
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

package api_doc

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/SENERGY-Platform/api-docs-provider/lib/client"
	"github.com/SENERGY-Platform/timescale-tableworker/pkg/config"
)

func PublishAsyncapiDoc(conf config.Config) {
	adpClient := client.New(http.DefaultClient, conf.ApiDocsProviderBaseUrl)
	file, err := os.Open("docs/asyncapi.json")
	if err != nil {
		log.Println(err)
		return
	}
	defer file.Close()
	ctx, cf := context.WithTimeout(context.Background(), 30*time.Second)
	defer cf()
	err = adpClient.AsyncapiPutDocFromReader(ctx, "github_com_SENERGY-Platform_timescale-tableworker", file)
	if err != nil {
		log.Println(err)
		return
	}
}
