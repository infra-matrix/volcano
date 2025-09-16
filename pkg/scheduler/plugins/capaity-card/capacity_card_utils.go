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
	`encoding/json`
	`strings`

	v1 `k8s.io/api/core/v1`
	`k8s.io/klog/v2`
	`volcano.sh/apis/pkg/apis/scheduling`
	`volcano.sh/volcano/pkg/scheduler/api`
)

// NewQueueResourceSupportingCard creates a new resource object from resource list
func NewQueueResourceSupportingCard(q *scheduling.Queue, rl v1.ResourceList) *api.Resource {
	var (
		queueResource     = api.NewResource(rl)
		queueCardResource = GetCardResourceFromAnnotations(q.Annotations, QueueAnnotationKeyCardQuota)
	)
	if queueResource.ScalarResources == nil {
		queueResource.ScalarResources = make(map[v1.ResourceName]float64)
	}
	for cardName, cardCountMilli := range queueCardResource.ScalarResources {
		queueResource.ScalarResources[cardName] = cardCountMilli
	}
	return queueResource
}

// GetCardResourceFromAnnotations extracts card resource from annotations.
func GetCardResourceFromAnnotations(annotations map[string]string, key string) *api.Resource {
	cardResource := api.EmptyResource()
	if cardJson, ok := annotations[key]; ok {
		cardMap := make(map[string]int)
		if err := json.Unmarshal([]byte(cardJson), &cardMap); err != nil {
			klog.Warningf(
				`failed to unmarshal card json: %s, %+v`,
				cardJson, err,
			)
			return cardResource
		}
		if cardResource.ScalarResources == nil {
			cardResource.ScalarResources = make(map[v1.ResourceName]float64)
		}
		for cardName, cardCount := range cardMap {
			cardResource.ScalarResources[v1.ResourceName(cardName)] = float64(
				cardCount * cardCountQuantityMultiplier,
			)
		}
	}
	return cardResource
}

// CheckSingleScalarResourceResult is the result of CheckSingleScalarResource.
type CheckSingleScalarResourceResult struct {
	Ok                  bool
	NoEnoughScalarName  v1.ResourceName
	NoEnoughScalarCount float64
	ToBeUsedScalarQuant float64
	RealCapabilityQuant float64
}

// CheckSingleScalarResource checks whether the scalar resource is enough.
func CheckSingleScalarResource(
	scalarName v1.ResourceName,
	scalarQuant float64,
	toBeUsedResource, realCapability *api.Resource,
) CheckSingleScalarResourceResult {
	result := CheckSingleScalarResourceResult{
		Ok: true,
	}
	// The multi-cards name is given like: NVIDIA-GTX-GeForce-4090D|NVIDIA-H200 .
	// The card name is confirmed after the task is assigned to certain node,
	// in which the card name is extracted from node's label.
	//
	// In multi-cards name, any one of the card can satisfy the request is ok.
	if strings.Contains(scalarName.String(), MultiCardSeparator) {
		multiCardNames := strings.Split(scalarName.String(), MultiCardSeparator)
		for _, cardName := range multiCardNames {
			if result = CheckSingleScalarResource(
				v1.ResourceName(cardName), scalarQuant, toBeUsedResource, realCapability,
			); result.Ok {
				return result
			}
		}
		result.Ok = false
		result.NoEnoughScalarName = v1.ResourceName(multiCardNames[0])
		result.NoEnoughScalarCount = scalarQuant
		return result
	}

	result.ToBeUsedScalarQuant = toBeUsedResource.ScalarResources[scalarName]
	result.RealCapabilityQuant = realCapability.ScalarResources[scalarName]
	if scalarQuant > 0 && result.ToBeUsedScalarQuant > result.RealCapabilityQuant {
		result.Ok = false
		result.NoEnoughScalarName = scalarName
		result.NoEnoughScalarCount = scalarQuant
		return result
	}
	result.Ok = true
	return result
}
