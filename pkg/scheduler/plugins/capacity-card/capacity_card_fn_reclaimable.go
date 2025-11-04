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
	`k8s.io/klog/v2`
	`volcano.sh/volcano/pkg/scheduler/api`
	`volcano.sh/volcano/pkg/scheduler/plugins/util`
)

const (
	serviceTypeAnnoKey = "volcano.sh/service.type"
)

type serviceType string

const (
	serviceTypeInference serviceType = "inference"
	serviceTypeTraining  serviceType = "training"
	serviceTypeUnknown   serviceType = "unknown"
)

// ReclaimableFn selects the reclaimable tasks under the capacity card plugin.
// Polices:
// 1. High priority inference services can preempt resources from lower priority training tasks.
// 2. Lower or equal priority inference services cannot preempt resources from high priority training tasks.
// 3. Training tasks cannot preempt each other, nor can they preempt resources from inference services.
// 4. Inference services cannot preempt each other.
func (p *Plugin) ReclaimableFn(
	reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo,
) ([]*api.TaskInfo, int) {
	var (
		victims              []*api.TaskInfo
		reclaimerServiceType = p.getTaskServiceType(reclaimer)
	)
	// Training tasks cannot preempt each other, nor can they preempt resources from inference services.
	if reclaimerServiceType == serviceTypeTraining {
		return victims, util.Permit
	}
	for _, reclaimee := range reclaimees {
		reclaimeeServiceType := p.getTaskServiceType(reclaimee)
		if reclaimeeServiceType == serviceTypeInference {
			// Inference services cannot preempt each other.
			continue
		}

		if reclaimeeServiceType == serviceTypeUnknown {
			klog.V(4).Infof("unknown service type for reclaimee task: %s, skip it", reclaimee.Name)
			continue
		}

		// Lower or equal priority inference services cannot preempt resources from high priority training tasks.
		if reclaimer.Priority <= reclaimee.Priority {
			continue
		}

		victims = append(victims, reclaimee)
	}
	klog.V(4).Infof("reclaimer: %s, victims: %+v", reclaimer, victims)
	return victims, util.Permit
}

func (p *Plugin) getTaskServiceType(ti *api.TaskInfo) serviceType {
	if ti.Pod == nil {
		return serviceTypeUnknown
	}
	st := ti.Pod.Annotations[serviceTypeAnnoKey]
	if st != "" {
		return serviceType(st)
	}
	if !p.allowServiceTypeByPodOwnerReferences {
		return serviceTypeUnknown
	}
	if ti.Pod.OwnerReferences == nil || len(ti.Pod.OwnerReferences) == 0 {
		return serviceTypeUnknown
	}
	switch ti.Pod.OwnerReferences[0].Kind {
	case "Job":
		return serviceTypeTraining

	case "ReplicaSet":
		return serviceTypeInference

	default:
		return serviceTypeUnknown
	}
}
