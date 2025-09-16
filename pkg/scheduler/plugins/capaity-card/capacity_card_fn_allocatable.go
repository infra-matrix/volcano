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
	`volcano.sh/apis/pkg/apis/scheduling`
	`volcano.sh/volcano/pkg/scheduler/api`
)

// AllocatableFn checks whether the task can be allocated, which does the queue-level capacity check.
// If it returns true, which will do the following aspects to resources:
// 1. Pod phase will be changed from Pending to Running.
func (p *Plugin) AllocatableFn(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
	if queue.Queue.Status.State != scheduling.QueueStateOpen {
		klog.V(3).Infof(
			"Queue <%s> current state: %s, cannot allocate task <%s>.",
			queue.Name, queue.Queue.Status.State, candidate.Name,
		)
		return false
	}
	return p.isTaskAllocatable(p.queueOpts[queue.UID], candidate)
}

func (p *Plugin) isTaskAllocatable(qAttr *queueAttr, ti *api.TaskInfo) bool {
	var (
		taskReqResource = ti.Resreq
		realCapability  = qAttr.realCapability
		futureUsed      = qAttr.allocated.Clone().Add(taskReqResource)
	)
	if futureUsed == nil {
		klog.V(5).Infof(
			"Task <%s/%s>, Queue <%s> futureUsed is nil, allow it to allocate",
			ti.Namespace, ti.Name, qAttr.name,
		)
		return true
	}
	if realCapability == nil {
		klog.V(5).Infof(
			"Task <%s/%s>, Queue <%s> realCapability is nil, allow it to allocate",
			ti.Namespace, ti.Name, qAttr.name,
		)
		return false
	}
	if taskReqResource == nil {
		if ok := futureUsed.LessEqual(realCapability, api.Zero); !ok {
			klog.V(5).Infof(
				"Task <%s/%s>, Queue <%s> realCapability <%s> is empty, deny it to enqueue",
				ti.Namespace, ti.Name, qAttr.name, realCapability.String(),
			)
			return false
		}
		klog.V(5).Infof(
			"Task <%s/%s>, Queue <%s> taskReqResource is nil, allow it to enqueue",
			ti.Namespace, ti.Name, qAttr.name,
		)
		return true
	}

	if taskReqResource.MilliCPU > 0 && futureUsed.MilliCPU > realCapability.MilliCPU {
		klog.V(2).Infof(
			"Task <%s/%s>, Queue <%s> has no enough CPU, taskReqResource <%v>, futureUsed <%v>, realCapability <%v>",
			ti.Namespace, ti.Name, qAttr.name,
			taskReqResource.MilliCPU, futureUsed.MilliCPU, realCapability.MilliCPU,
		)
		return false
	}
	if taskReqResource.Memory > 0 && futureUsed.Memory > realCapability.Memory {
		klog.V(2).Infof(
			"Task <%s/%s>, Queue <%s> has no enough Memory, taskReqResource <%v Mi>, futureUsed <%v Mi>, realCapability <%v Mi>",
			ti.Namespace, ti.Name, qAttr.name,
			taskReqResource.Memory/1024/1024, futureUsed.Memory/1024/1024, realCapability.Memory/1024/1024,
		)
		return false
	}

	// if r.scalar is nil, whatever rr.scalar is, r is less or equal to rr
	if futureUsed.ScalarResources == nil {
		return true
	}

	for scalarName, scalarQuant := range taskReqResource.ScalarResources {
		if api.IsIgnoredScalarResource(scalarName) {
			continue
		}
		var (
			futureUsedQuant     = futureUsed.ScalarResources[scalarName]
			realCapabilityQuant = realCapability.ScalarResources[scalarName]
		)
		if scalarQuant > 0 && futureUsedQuant > realCapabilityQuant {
			klog.V(2).Infof(
				"Task <%s/%s>, Queue <%s> has no enough %s, scalarQuant <%v>, futureUsedQuant <%v>, realCapability <%v>",
				ti.Namespace, ti.Name, qAttr.name, scalarName,
				scalarQuant, futureUsedQuant, realCapabilityQuant,
			)
			return false
		}
	}
	return true
}
