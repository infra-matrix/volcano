/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Enhanced gang scheduling validation with task-level validity checks
- Improved preemption logic to respect gang scheduling constraints
- Added support for job starving detection and enhanced pipeline state management

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

package capacitycard

import (
	"encoding/json"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
)

// QueueHasCardQuota checks whether the queue has card quota annotation.
func QueueHasCardQuota(q *scheduling.Queue) bool {
	_, ok := q.Annotations[QueueAnnotationKeyCardQuota]
	return ok
}

// GetCardResourceFromAnnotations extracts card resource from annotations.
func GetCardResourceFromAnnotations(annotations map[string]string, key string) map[v1.ResourceName]float64 {
	cardResource := map[v1.ResourceName]float64{}
	if cardJson, ok := annotations[key]; ok {
		cardMap := make(map[string]int)
		if err := json.Unmarshal([]byte(cardJson), &cardMap); err != nil {
			klog.Warningf(
				`failed to unmarshal card json: %s, %+v`,
				cardJson, err,
			)
			return cardResource
		}

		for cardName, cardCount := range cardMap {
			cardResource[v1.ResourceName(cardName)] = float64(
				cardCount * cardCountQuantityMultiplier,
			)
		}
	}
	return cardResource
}

// CheckSingleScalarResourceResult is the result of CheckSingleScalarResource.
type CheckSingleScalarResourceResult struct {
	Ok                   bool
	NoEnoughScalarName   v1.ResourceName
	NoEnoughScalarCount  float64
	ToBeUsedScalarQuant  float64
	QueueCapabilityQuant float64
}

// CheckSingleScalarResource checks whether the scalar resource is enough.
func CheckSingleScalarResource(
	scalarName v1.ResourceName,
	scalarQuant float64,
	toBeUsedResource, queueCapability *api.Resource,
) CheckSingleScalarResourceResult {
	result := CheckSingleScalarResourceResult{
		Ok: true,
	}
	// The multi-cards name is given like: NVIDIA-GTX-GeForce-4090D|NVIDIA-H200 .
	// The card name is confirmed after the task is assigned to certain node,
	// in which the card name is extracted from node's label.
	//
	// In multi-cards name, any one of the card can satisfy the request is ok.
	multiCardNames := strings.Split(scalarName.String(), MultiCardSeparator)
	for _, cardName := range multiCardNames {
		result.ToBeUsedScalarQuant += toBeUsedResource.ScalarResources[v1.ResourceName(cardName)]
		result.QueueCapabilityQuant += queueCapability.ScalarResources[v1.ResourceName(cardName)]
	}

	if scalarQuant > 0 && result.ToBeUsedScalarQuant > result.QueueCapabilityQuant {
		result.Ok = false
		result.NoEnoughScalarName = scalarName
		result.NoEnoughScalarCount = scalarQuant
		return result
	}
	result.Ok = true
	return result
}
