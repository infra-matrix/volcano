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
	`math`

	v1 `k8s.io/api/core/v1`
	`k8s.io/klog/v2`
	"volcano.sh/apis/pkg/apis/scheduling"
	`volcano.sh/volcano/pkg/scheduler/api`
	`volcano.sh/volcano/pkg/scheduler/api/helpers`
	`volcano.sh/volcano/pkg/scheduler/framework`
	`volcano.sh/volcano/pkg/scheduler/metrics`
	`volcano.sh/volcano/pkg/scheduler/plugins/util`
)

type queueAttr struct {
	queueID        api.QueueID
	name           string
	share          float64
	deserved       *api.Resource
	allocated      *api.Resource
	request        *api.Resource
	elastic        *api.Resource // elastic = job.allocated - job.minAvailable
	inqueue        *api.Resource
	capability     *api.Resource
	realCapability *api.Resource
	guarantee      *api.Resource
}

func (p *Plugin) buildQueueAttrs(ssn *framework.Session) {
	for _, queue := range ssn.Queues {
		guarantee := NewQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Guarantee.Resource)
		p.totalGuarantee.Add(guarantee)
	}

	// Build attributes for Queues.
	for _, apiJob := range ssn.Jobs {
		job := NewJobInfo(apiJob)
		klog.V(4).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
		if _, found := p.queueOpts[job.Queue]; !found {
			queue := ssn.Queues[job.Queue]
			attr := &queueAttr{
				queueID:   queue.UID,
				name:      queue.Name,
				deserved:  NewQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Deserved),
				allocated: api.EmptyResource(),
				request:   api.EmptyResource(),
				elastic:   api.EmptyResource(),
				inqueue:   api.EmptyResource(),
				guarantee: api.EmptyResource(),
			}
			if len(queue.Queue.Spec.Capability) != 0 {
				attr.capability = NewQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Capability)
				if attr.capability.MilliCPU <= 0 {
					attr.capability.MilliCPU = math.MaxFloat64
				}
				if attr.capability.Memory <= 0 {
					attr.capability.Memory = math.MaxFloat64
				}
			}
			if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
				attr.guarantee = NewQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Guarantee.Resource)
			}
			realCapability := api.ExceededPart(p.totalResource, p.totalGuarantee).Add(attr.guarantee)
			if attr.capability == nil {
				attr.capability = api.EmptyResource()
				attr.realCapability = realCapability
			} else {
				realCapability.MinDimensionResource(attr.capability, api.Infinity)
				attr.realCapability = realCapability
			}
			p.queueOpts[job.Queue] = attr
			klog.V(4).Infof("Added Queue <%s> attributes.", job.Queue)
		}

		attr := p.queueOpts[job.Queue]
		for status, tasks := range job.TaskStatusIndex {
			if api.AllocatedStatus(status) {
				for _, t := range tasks {
					attr.allocated.Add(t.Resreq)
					attr.request.Add(t.Resreq)
				}
			} else if status == api.Pending {
				for _, t := range tasks {
					attr.request.Add(t.Resreq)
				}
			}
		}

		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue {
			// deduct the resources of scheduling gated tasks in a job when calculating inqueued resources
			// so that it will not block other jobs from being inqueued.
			attr.inqueue.Add(job.DeductSchGatedResources(job.GetMinResources()))
		}

		// calculate inqueue resource for running jobs
		// the judgement 'job.PodGroup.Status.Running >= job.PodGroup.Spec.MinMember'
		// will work on cases such as the following condition:
		// Considering a Spark job is completed(driver pod is completed) while the PodGroup keeps running,
		// the allocated resource will be reserved again if without the judgement.
		if job.PodGroup.Status.Phase == scheduling.PodGroupRunning &&
			job.PodGroup.Spec.MinResources != nil &&
			int32(util.CalculateAllocatedTaskNum(job.JobInfo)) >= job.PodGroup.Spec.MinMember {
			inqueued := util.GetInqueueResource(job.JobInfo, job.Allocated)
			attr.inqueue.Add(job.DeductSchGatedResources(inqueued))
		}
		attr.elastic.Add(job.GetElasticResources())
		klog.V(5).Infof(
			"Queue %s allocated <%s> request <%s> inqueue <%s> elastic <%s>",
			attr.name, attr.allocated.String(), attr.request.String(), attr.inqueue.String(), attr.elastic.String(),
		)
	}

	for _, attr := range p.queueOpts {
		if attr.realCapability != nil {
			attr.deserved.MinDimensionResource(attr.realCapability, api.Infinity)
		}
		attr.deserved = helpers.Max(attr.deserved, attr.guarantee)
		p.updateShare(attr)
		klog.V(4).Infof(
			"The attributes of queue <%s> in capacity: deserved <%v>, realCapability <%v>, allocate <%v>, request <%v>, elastic <%v>, share <%0.2f>",
			attr.name, attr.deserved, attr.realCapability, attr.allocated, attr.request, attr.elastic, attr.share,
		)
	}

	ssn.AddQueueOrderFn(p.Name(), func(l, r interface{}) int {
		lv := l.(*api.QueueInfo)
		rv := r.(*api.QueueInfo)
		if lv.Queue.Spec.Priority != rv.Queue.Spec.Priority {
			// return negative means high priority
			return int(rv.Queue.Spec.Priority) - int(lv.Queue.Spec.Priority)
		}

		if p.queueOpts[lv.UID].share == p.queueOpts[rv.UID].share {
			return 0
		}

		if p.queueOpts[lv.UID].share < p.queueOpts[rv.UID].share {
			return -1
		}
		return 1
	})

	// Record metrics
	for queueID, queueInfo := range ssn.Queues {
		queue := ssn.Queues[queueID]
		if attr, ok := p.queueOpts[queueID]; ok {
			metrics.UpdateQueueDeserved(
				attr.name, attr.deserved.MilliCPU, attr.deserved.Memory, attr.deserved.ScalarResources,
			)
			metrics.UpdateQueueAllocated(
				attr.name, attr.allocated.MilliCPU, attr.allocated.Memory, attr.allocated.ScalarResources,
			)
			metrics.UpdateQueueRequest(
				attr.name, attr.request.MilliCPU, attr.request.Memory, attr.request.ScalarResources,
			)
			if attr.capability != nil {
				metrics.UpdateQueueCapacity(
					attr.name, attr.capability.MilliCPU, attr.capability.Memory, attr.capability.ScalarResources,
				)
			}
			metrics.UpdateQueueRealCapacity(
				attr.name,
				attr.realCapability.MilliCPU, attr.realCapability.Memory,
				attr.realCapability.ScalarResources,
			)
			continue
		}
		deservedCPU, deservedMem, scalarResources := 0.0, 0.0, map[v1.ResourceName]float64{}
		if queue.Queue.Spec.Deserved != nil {
			attr := NewQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Deserved)
			deservedCPU = attr.MilliCPU
			deservedMem = attr.Memory
			scalarResources = attr.ScalarResources
		}
		metrics.UpdateQueueDeserved(queueInfo.Name, deservedCPU, deservedMem, scalarResources)
		metrics.UpdateQueueAllocated(queueInfo.Name, 0, 0, map[v1.ResourceName]float64{})
		metrics.UpdateQueueRequest(queueInfo.Name, 0, 0, map[v1.ResourceName]float64{})
		guarantee := api.EmptyResource()
		if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
			guarantee = NewQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Guarantee.Resource)
		}
		realCapacity := api.ExceededPart(p.totalResource, p.totalGuarantee).Add(guarantee)
		if len(queue.Queue.Spec.Capability) > 0 {
			capacity := NewQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Capability)
			realCapacity.MinDimensionResource(capacity, api.Infinity)
			metrics.UpdateQueueCapacity(
				queueInfo.Name, capacity.MilliCPU, capacity.Memory, capacity.ScalarResources,
			)
		}
		metrics.UpdateQueueRealCapacity(
			queueInfo.Name, realCapacity.MilliCPU, realCapacity.Memory, realCapacity.ScalarResources,
		)
	}
}

func (p *Plugin) newQueueAttr(queue *api.QueueInfo) *queueAttr {
	attr := &queueAttr{
		queueID:    queue.UID,
		name:       queue.Name,
		deserved:   NewQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Deserved),
		allocated:  api.EmptyResource(),
		request:    api.EmptyResource(),
		elastic:    api.EmptyResource(),
		inqueue:    api.EmptyResource(),
		guarantee:  api.EmptyResource(),
		capability: api.EmptyResource(),
	}
	if len(queue.Queue.Spec.Capability) != 0 {
		attr.capability = NewQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Capability)
	}

	if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
		attr.guarantee = NewQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Guarantee.Resource)
	}
	return attr
}

func (p *Plugin) updateShare(attr *queueAttr) {
	updateQueueAttrShare(attr)
	metrics.UpdateQueueShare(attr.name, attr.share)
}

func updateQueueAttrShare(attr *queueAttr) {
	res := float64(0)

	for _, rn := range attr.deserved.ResourceNames() {
		res = max(res, helpers.Share(attr.allocated.Get(rn), attr.deserved.Get(rn)))
	}

	attr.share = res
}
