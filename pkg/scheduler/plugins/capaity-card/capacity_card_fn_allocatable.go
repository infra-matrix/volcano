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
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
)

// AllocatableFn checks whether the task can be allocated, which does the queue-level capacity check.
// If it returns true, which will do the following aspects to resources:
// 1. Pod phase will be changed from Pending to Running.
func (p *Plugin) AllocatableFn(queue *api.QueueInfo, candidate *api.TaskInfo, isCardUnlimitedCpuMemory bool) bool {
	if queue.Queue.Status.State != scheduling.QueueStateOpen {
		klog.V(3).Infof(
			"Queue <%s> current state: %s, cannot allocate task <%s/%s>.",
			queue.Name, queue.Queue.Status.State, candidate.Namespace, candidate.Name,
		)
		return false
	}
	return p.isTaskAllocatable(p.queueOpts[queue.UID], candidate, isCardUnlimitedCpuMemory)
}

// isTaskAllocatable checks whether the task can be allocated in the queue according to the queue's real capability.
func (p *Plugin) isTaskAllocatable(qAttr *queueAttr, ti *api.TaskInfo, isCardUnlimitedCpuMemory bool) bool {
	var (
		taskReqResource    = ti.Resreq
		queueCapability    = qAttr.capability
		totalToBeAllocated = qAttr.allocated.Clone().Add(taskReqResource)
	)
	// check cpu and memory if cardUnlimitedCpuMemory not set or has no card resources
	if !isCardUnlimitedCpuMemory || !p.HasCardResource(taskReqResource) {
		// check cpu and memory
		cpuMemoryReq := &api.Resource{
			MilliCPU: taskReqResource.MilliCPU,
			Memory:   taskReqResource.Memory,
		}
		if !totalToBeAllocated.LessEqualWithDimension(queueCapability, cpuMemoryReq) {
			klog.V(3).Infof("Queue <%v> has not enough CPU or memory: capability cpu: <%v>, memory: <%v>, total to be allocated cpu: <%v>, memory: <%v>; task <%v/%v>: resource request cpu: <%v>, memory: <%v>",
				qAttr.name,
				queueCapability.MilliCPU,
				queueCapability.Memory,
				totalToBeAllocated.MilliCPU,
				totalToBeAllocated.Memory,
				ti.Namespace,
				ti.Name,
				taskReqResource.MilliCPU,
				taskReqResource.Memory,
			)
			eventRecorder.Eventf(
				ti.Pod, v1.EventTypeWarning, InsufficientCPUMemoryQuota,
				"Queue <%v> has not enough CPU or memory: capability cpu: <%v>, memory: <%v>, total to be allocated cpu: <%v>, memory: <%v>, resource request cpu: <%v>, memory: <%v>",
				qAttr.name,
				queueCapability.MilliCPU,
				queueCapability.Memory,
				totalToBeAllocated.MilliCPU,
				totalToBeAllocated.Memory,
				taskReqResource.MilliCPU,
				taskReqResource.Memory,
			)
			return false
		}
	}

	// if r.scalar is nil, whatever rr.scalar is, r is less or equal to rr
	if totalToBeAllocated.ScalarResources == nil {
		return true
	}

	for scalarName, scalarQuant := range taskReqResource.ScalarResources {
		if api.IsIgnoredScalarResource(scalarName) {
			continue
		}
		checkResult := CheckSingleScalarResource(
			scalarName, scalarQuant, totalToBeAllocated, queueCapability,
		)
		if checkResult.Ok {
			continue
		}
		klog.V(2).Infof(
			"Task <%s/%s>, Queue <%s> has no enough %s, request <%v>, total would be <%v>, capability <%v>",
			ti.Namespace, ti.Name, qAttr.name,
			checkResult.NoEnoughScalarName,
			checkResult.NoEnoughScalarCount,
			checkResult.ToBeUsedScalarQuant,
			checkResult.QueueCapabilityQuant,
		)
		if ti.Pod != nil {
			eventRecorder.Eventf(
				ti.Pod, v1.EventTypeWarning, InsufficientScalarQuota,
				"Queue <%s> has insufficient <%s> quota: requested <%v>, total would be <%v>, but capability is <%v>",
				qAttr.name,
				checkResult.NoEnoughScalarName,
				checkResult.NoEnoughScalarCount,
				checkResult.ToBeUsedScalarQuant,
				checkResult.QueueCapabilityQuant,
			)
		}
		return false
	}
	return true
}
