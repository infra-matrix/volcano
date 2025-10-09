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
func (p *Plugin) JobEnqueueableFn(ssn *framework.Session, jobInfo *api.JobInfo, isCardUnlimitedCpuMemory bool) int {
	job, err := p.NewJobInfo(jobInfo)
	if err != nil {
		klog.Errorf(
			"Failed to create jobInfo for job <%s/%s>: %+v",
			jobInfo.Namespace, jobInfo.Name, err,
		)
		return util.Reject
	}

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
	if !p.isJobEnqueueable(ssn, qAttr, job, isCardUnlimitedCpuMemory) {
		klog.V(2).Infof(
			"Queue <%s> has no enough resource for job <%s/%s>",
			queue.Name, job.Namespace, job.Name,
		)
		return util.Reject
	}

	// job enqueued
	deductedResources := job.DeductSchGatedResources(job.GetMinResources())
	qAttr.inqueue.Add(deductedResources)
	klog.V(5).Infof("Job <%s/%s> enqueued", job.Namespace, job.Name)
	return util.Permit
}

// isJobEnqueueable checks whether the job can be enqueued in the queue according to the queue's real capability.
func (p *Plugin) isJobEnqueueable(ssn *framework.Session, qAttr *queueAttr, job *JobInfo, isCardUnlimitedCpuMemory bool) bool {
	var (
		jobReqResource  = job.GetMinResources()
		queueCapability = qAttr.capability
		totalToBeUsed   = jobReqResource.Clone().
				Add(qAttr.allocated).
				Add(qAttr.inqueue).
				Sub(qAttr.elastic)
	)
	klog.V(5).Infof(
		"Job <%s/%s> min resource <%s>, queue %s capability <%s> allocated <%s> inqueue <%s> elastic <%s>",
		job.Namespace, job.Name, jobReqResource.String(), qAttr.name,
		queueCapability.String(),
		qAttr.allocated.String(),
		qAttr.inqueue.String(),
		qAttr.elastic.String(),
	)

	// check cpu and memory if cardUnlimitedCpuMemory not set or has no card resources
	if !isCardUnlimitedCpuMemory || !p.HasCardResource(jobReqResource) {
		// check cpu and memory
		cpuMemoryReq := &api.Resource{
			MilliCPU: jobReqResource.MilliCPU,
			Memory:   jobReqResource.Memory,
		}
		if !totalToBeUsed.LessEqualWithDimension(queueCapability, cpuMemoryReq) {
			klog.V(3).Infof("Queue <%v> has not enough CPU or memory: capability cpu: <%v>, memory: <%v>, total to be used cpu: <%v>, memory: <%v>; Job <%v/%v>: resource request cpu: <%v>, memory: <%v>",
				qAttr.name,
				queueCapability.MilliCPU,
				queueCapability.Memory,
				totalToBeUsed.MilliCPU,
				totalToBeUsed.Memory,
				job.Namespace,
				job.Name,
				jobReqResource.MilliCPU,
				jobReqResource.Memory,
			)
			ssn.RecordPodGroupEvent(
				job.PodGroup, v1.EventTypeWarning, InsufficientCPUMemoryQuota,
				fmt.Sprintf("Queue <%v> has not enough CPU or memory: capability cpu: <%v>, memory: <%v>, total to be used cpu: <%v>, memory: <%v>, resource request cpu: <%v>, memory: <%v>",
					qAttr.name,
					queueCapability.MilliCPU,
					queueCapability.Memory,
					totalToBeUsed.MilliCPU,
					totalToBeUsed.Memory,
					jobReqResource.MilliCPU,
					jobReqResource.Memory,
				),
			)
			return false
		}
	}

	// if r.scalar is nil, whatever rr.scalar is, r is less or equal to rr
	if totalToBeUsed.ScalarResources == nil {
		return true
	}

	for scalarName, scalarQuant := range jobReqResource.ScalarResources {
		if api.IsIgnoredScalarResource(scalarName) {
			continue
		}
		checkResult := CheckSingleScalarResource(
			scalarName, scalarQuant, totalToBeUsed, queueCapability,
		)
		if checkResult.Ok {
			continue
		}
		klog.V(2).Infof(
			"Job <%s/%s>, Queue <%s> has no enough %s, request <%v>, total would be <%v>, capability <%v>",
			job.Namespace, job.Name, qAttr.name,
			checkResult.NoEnoughScalarName,
			checkResult.NoEnoughScalarCount,
			checkResult.ToBeUsedScalarQuant,
			checkResult.QueueCapabilityQuant,
		)
		ssn.RecordPodGroupEvent(
			job.PodGroup, v1.EventTypeWarning, InsufficientScalarQuota,
			fmt.Sprintf(
				"Queue <%s> has insufficient <%s> quota: requested <%v>, total would be <%v>, but capability is <%v>",
				qAttr.name, checkResult.NoEnoughScalarName, checkResult.NoEnoughScalarCount,
				checkResult.ToBeUsedScalarQuant, checkResult.QueueCapabilityQuant,
			),
		)
		return false
	}
	return true
}
