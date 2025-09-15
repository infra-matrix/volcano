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

	v1 `k8s.io/api/core/v1`
	`k8s.io/klog/v2`
	`volcano.sh/apis/pkg/apis/scheduling`
	`volcano.sh/volcano/pkg/scheduler/api`
)

// NewTotalResourceSupportingCard creates a new resource object from resource list
func NewTotalResourceSupportingCard(rl v1.ResourceList) *api.Resource {
	r := api.EmptyResource()
	for rName, rQuant := range rl {
		switch rName {
		case v1.ResourceCPU:
			r.MilliCPU += float64(rQuant.MilliValue())
		case v1.ResourceMemory:
			r.Memory += float64(rQuant.Value())
		case v1.ResourcePods:
			r.MaxTaskNum += int(rQuant.Value())
			r.AddScalar(rName, float64(rQuant.Value()))
		case v1.ResourceEphemeralStorage:
			r.AddScalar(rName, float64(rQuant.MilliValue()))
		default:
			if api.IsCountQuota(rName) {
				continue
			}
			r.AddScalar(rName, float64(rQuant.Value()))
		}
	}
	return r
}

// NewQueueResourceSupportingCard creates a new resource object from resource list
func NewQueueResourceSupportingCard(q *scheduling.Queue, rl v1.ResourceList) *api.Resource {
	var (
		queueResource     = api.NewResource(rl)
		queueCardResource = GetCardResourceFromAnnotations(q.Annotations, QueueAnnotationKeyCardQuota)
	)
	if queueResource.ScalarResources == nil {
		queueResource.ScalarResources = make(map[v1.ResourceName]float64)
	}
	for cardName, cardCount := range queueCardResource.ScalarResources {
		queueResource.ScalarResources[cardName] = cardCount
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
			cardResource.ScalarResources[v1.ResourceName(cardName)] = float64(cardCount)
		}
	}
	return cardResource
}
