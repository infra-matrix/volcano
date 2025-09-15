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

// JobEnqueueableFn returns a function to check whether the job can be enqueued.
// It handles pending PodGroups, and checks whether the queue has enough.
// It does the following aspects:
// 1. PogGroup phase from Pending to InQueue.
// 2. Pod will be created and in phase of Pending.
// 3. Queue resource pre-check.
func (p *Plugin) JobEnqueueableFn(ssn *framework.Session) api.VoteFn {
	return func(obj interface{}) int {
		job, err := p.NewJobInfo(obj.(*api.JobInfo))
		if err != nil {
			klog.Errorf("Failed to create jobInfo for job <%s/%s>: %+v", job.Namespace, job.Name, err)
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
}

func (p *Plugin) isJobEnqueueable(qAttr *queueAttr, job *JobInfo) bool {
	jobMinResourceReq := job.GetMinResources()
	klog.V(5).Infof(
		"Job %s min resource <%s>, queue %s capability <%s> allocated <%s> inqueue <%s> elastic <%s>",
		job.Name, jobMinResourceReq.String(), qAttr.name,
		qAttr.realCapability.String(),
		qAttr.allocated.String(),
		qAttr.inqueue.String(),
		qAttr.elastic.String(),
	)
	inuseResource := jobMinResourceReq.Clone().
		Add(qAttr.allocated).
		Add(qAttr.inqueue).
		Sub(qAttr.elastic)

	if inuseResource == nil {
		klog.V(5).Infof(
			"Job <%s/%s>, Queue <%s> inuseResource is nil, allow it to enqueue",
			qAttr.name, job.Namespace, job.Name,
		)
		return true
	}
	if qAttr.realCapability == nil {
		klog.V(5).Infof(
			"Job <%s/%s>, Queue <%s> realCapability is nil, allow it to enqueue",
			qAttr.name, job.Namespace, job.Name,
		)
		return false
	}
	if jobMinResourceReq == nil {
		if ok := inuseResource.LessEqual(qAttr.realCapability, api.Zero); !ok {
			klog.V(5).Infof(
				"Job <%s/%s>, Queue <%s> jobMinResourceReq <%s> is nil, allow it to enqueue",
				qAttr.name, job.Namespace, job.Name, qAttr.realCapability.String(),
			)
			return false
		}
		return true
	}

	if jobMinResourceReq.MilliCPU > 0 && inuseResource.MilliCPU > qAttr.realCapability.MilliCPU {
		klog.V(5).Infof(
			"Job <%s/%s>, Queue <%s> has no enough CPU, jobMinResourceReq <%v>, inuseResource <%v>, realCapability <%v>",
			qAttr.name, job.Namespace, job.Name,
			jobMinResourceReq.MilliCPU, inuseResource.MilliCPU, qAttr.realCapability.MilliCPU,
		)
		return false
	}
	if jobMinResourceReq.Memory > 0 && inuseResource.Memory > qAttr.realCapability.Memory {
		klog.V(5).Infof(
			"Job <%s/%s>, Queue <%s> has no enough Memory, jobMinResourceReq <%v Mi>, inuseResource <%v Mi>, realCapability <%v Mi>",
			qAttr.name, job.Namespace, job.Name,
			jobMinResourceReq.Memory/1024/1024, inuseResource.Memory/1024/1024, qAttr.realCapability.Memory/1024/1024,
		)
		return false
	}

	// if r.scalar is nil, whatever rr.scalar is, r is less or equal to rr
	if inuseResource.ScalarResources == nil {
		return true
	}

	for scalarName, scalarQuant := range jobMinResourceReq.ScalarResources {
		if api.IsIgnoredScalarResource(scalarName) {
			continue
		}
		var (
			inuseQuant          = inuseResource.ScalarResources[scalarName]
			realCapabilityQuant = qAttr.realCapability.ScalarResources[scalarName]
		)
		if scalarQuant > 0 && inuseQuant > realCapabilityQuant {
			klog.V(5).Infof(
				"Job <%s/%s>, Queue <%s> has no enough %s, jobMinResourceReq <%v>, inuseResource <%v>, realCapability <%v>",
				qAttr.name, job.Namespace, job.Name, scalarName,
				scalarQuant, inuseQuant, realCapabilityQuant,
			)
			return false
		}
	}
	return true
}
