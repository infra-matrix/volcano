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
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

// JobEnqueueableFn checks whether the job can be enqueued, which does the queue-level capacity pre-check.
// It handles pending PodGroups, and checks whether the queue has enough resource to proceed.
// If it returns util.Permit, which will do the following aspects to resources:
// 1. PogGroup phase will be changed from Pending to InQueue.
// 2. Pod will be created and in phase of Pending.
func (p *Plugin) JobEnqueueableFn(ssn *framework.Session, job *api.JobInfo) int {
	var (
		queueID = job.Queue
		queue   = ssn.Queues[queueID]
		qAttr   = p.queueOpts[queueID]
	)
	// If the queue is not open, do not enqueue
	if queue.Queue.Status.State != scheduling.QueueStateOpen {
		klog.V(3).Infof(
			"Queue <%s> current state: %s, is not open state, reject job <%s/%s>.",
			queue.Name, queue.Queue.Status.State, job.Namespace, job.Name,
		)
		return util.Reject
	}

	// it checks whether the queue has enough resource to run the job.
	if !p.jobEnqueueable(ssn, queue, job) {
		klog.V(2).Infof(
			"Queue <%s> has no enough resource for job <%s/%s>",
			queue.Name, job.Namespace, job.Name,
		)
		ssn.RecordPodGroupEvent(job.PodGroup, v1.EventTypeNormal, string(scheduling.PodGroupUnschedulableType), "queue resource quota insufficient")
		return util.Reject
	}

	// job enqueued
	deductedResources := job.DeductSchGatedResources(job.GetMinResources())
	qAttr.inqueue.Add(deductedResources)
	klog.V(5).Infof("Job <%s/%s> enqueued", job.Namespace, job.Name)
	return util.Permit
}

// isJobEnqueueable checks whether the job can be enqueued in the queue according to the queue's real capability.
func (p *Plugin) jobEnqueueable(ssn *framework.Session, queue *api.QueueInfo, job *api.JobInfo) bool {
	attr := p.queueOpts[queue.UID]
	minReq := job.GetMinResources()

	klog.V(5).Infof("job %s min resource <%s>, queue %s capability <%s> allocated <%s> inqueue <%s> elastic <%s>",
		job.Name, minReq.String(), queue.Name, attr.realCapability.String(), attr.allocated.String(), attr.inqueue.String(), attr.elastic.String())
	// The queue resource quota limit has not reached
	totalToBeUsed := minReq.Clone().Add(attr.allocated).Add(attr.inqueue).Sub(attr.elastic)

	// check cpu and memory
	cpuMemoryReq := &api.Resource{
		MilliCPU: minReq.MilliCPU,
		Memory:   minReq.Memory,
	}
	if !totalToBeUsed.LessEqualWithDimension(attr.realCapability, cpuMemoryReq) {
		return false
	}

	// if r.scalar is nil, whatever rr.scalar is, r is less or equal to rr
	if totalToBeUsed.ScalarResources == nil {
		return true
	}

	for scalarName, scalarQuant := range minReq.ScalarResources {
		if api.IsIgnoredScalarResource(scalarName) {
			continue
		}
		checkResult := CheckSingleScalarResource(
			scalarName, scalarQuant, totalToBeUsed, attr.realCapability,
		)
		if checkResult.Ok {
			continue
		}
		klog.V(2).Infof(
			"Job <%s/%s>, Queue <%s> has no enough %s, request <%v>, total would be <%v>, capability <%v>",
			job.Namespace, job.Name, queue.Name,
			checkResult.NoEnoughScalarName,
			checkResult.NoEnoughScalarCount,
			checkResult.ToBeUsedScalarQuant,
			checkResult.QueueCapabilityQuant,
		)
		ssn.RecordPodGroupEvent(
			job.PodGroup, v1.EventTypeWarning, "InsufficientScalarQuota",
			fmt.Sprintf(
				"Queue <%s> has insufficient <%s> quota: requested <%v>, total would be <%v>, but capability is <%v>",
				queue.Name, checkResult.NoEnoughScalarName, checkResult.NoEnoughScalarCount,
				checkResult.ToBeUsedScalarQuant, checkResult.QueueCapabilityQuant,
			),
		)
		return false
	}
	return true
}
