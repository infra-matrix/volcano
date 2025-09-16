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
	`volcano.sh/volcano/pkg/scheduler/framework`
	`volcano.sh/volcano/pkg/scheduler/plugins/util`
)

// JobEnqueueableFn checks whether the job can be enqueued, which does the queue-level capacity pre-check.
// It handles pending PodGroups, and checks whether the queue has enough resource to proceed.
// If it returns util.Permit, which will do the following aspects to resources:
// 1. PogGroup phase will be changed from Pending to InQueue.
// 2. Pod will be created and in phase of Pending.
func (p *Plugin) JobEnqueueableFn(ssn *framework.Session, jobInfo *api.JobInfo) int {
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
	// If no capability is set, always enqueue the job.
	if qAttr.realCapability == nil {
		klog.V(4).Infof(
			"Capability of queue <%s> was not set, allow job <%s/%s> to Inqueue.",
			queue.Name, job.Namespace, job.Name,
		)
		return util.Permit
	}

	if job.PodGroup.Spec.MinResources == nil {
		klog.V(4).Infof("Job %s MinResources is null.", job.Name)
		return util.Permit
	}

	// it checks whether the queue has enough resource to run the job.
	if !p.isJobEnqueueable(qAttr, job) {
		klog.V(2).Infof(
			"Queue %s has no enough resource for job <%s/%s>",
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
func (p *Plugin) isJobEnqueueable(qAttr *queueAttr, job *JobInfo) bool {
	var (
		jobReqResource   = job.GetMinResources()
		realCapability   = qAttr.realCapability
		toBeUsedResource = jobReqResource.Clone().
			Add(qAttr.allocated).
			Add(qAttr.inqueue).
			Sub(qAttr.elastic)
	)
	klog.V(5).Infof(
		"Job %s min resource <%s>, queue %s capability <%s> allocated <%s> inqueue <%s> elastic <%s>",
		job.Name, jobReqResource.String(), qAttr.name,
		realCapability.String(),
		qAttr.allocated.String(),
		qAttr.inqueue.String(),
		qAttr.elastic.String(),
	)

	if toBeUsedResource == nil {
		klog.V(5).Infof(
			"Job <%s/%s>, Queue <%s> totalToBeUsed is nil, allow it to enqueue",
			job.Namespace, job.Name, qAttr.name,
		)
		return true
	}
	if realCapability == nil {
		klog.V(5).Infof(
			"Job <%s/%s>, Queue <%s> realCapability is nil, allow it to enqueue",
			job.Namespace, job.Name, qAttr.name,
		)
		return false
	}
	if jobReqResource == nil {
		if ok := toBeUsedResource.LessEqual(realCapability, api.Zero); !ok {
			klog.V(5).Infof(
				"Job <%s/%s>, Queue <%s> realCapability <%s> is empty, deny it to enqueue",
				job.Namespace, job.Name, qAttr.name, realCapability.String(),
			)
			return false
		}
		klog.V(5).Infof(
			"Job <%s/%s>, Queue <%s> request is nil, allow it to enqueue",
			job.Namespace, job.Name, qAttr.name,
		)
		return true
	}

	if jobReqResource.MilliCPU > 0 && toBeUsedResource.MilliCPU > realCapability.MilliCPU {
		klog.V(5).Infof(
			"Job <%s/%s>, Queue <%s> has no enough CPU, request <%v>, totalToBeUsed <%v>, realCapability <%v>",
			job.Namespace, job.Name, qAttr.name,
			jobReqResource.MilliCPU, toBeUsedResource.MilliCPU, realCapability.MilliCPU,
		)
		return false
	}
	if jobReqResource.Memory > 0 && toBeUsedResource.Memory > realCapability.Memory {
		klog.V(5).Infof(
			"Job <%s/%s>, Queue <%s> has no enough Memory, request <%v Mi>, totalToBeUsed <%v Mi>, realCapability <%v Mi>",
			job.Namespace, job.Name, qAttr.name,
			jobReqResource.Memory/1024/1024, toBeUsedResource.Memory/1024/1024, realCapability.Memory/1024/1024,
		)
		return false
	}

	// if r.scalar is nil, whatever rr.scalar is, r is less or equal to rr
	if toBeUsedResource.ScalarResources == nil {
		return true
	}

	for scalarName, scalarQuant := range jobReqResource.ScalarResources {
		if api.IsIgnoredScalarResource(scalarName) {
			continue
		}
		checkResult := CheckSingleScalarResource(
			scalarName, scalarQuant, toBeUsedResource, realCapability,
		)
		if checkResult.Ok {
			continue
		}
		klog.V(2).Infof(
			"Job <%s/%s>, Queue <%s> has no enough %s, request <%v>, totalToBeUsed <%v>, realCapability <%v>",
			job.Namespace, job.Name, qAttr.name,
			checkResult.NoEnoughScalarName,
			checkResult.NoEnoughScalarCount,
			checkResult.ToBeUsedScalarQuant,
			checkResult.RealCapabilityQuant,
		)
		return false
	}
	return true
}
