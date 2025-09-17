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
	v1 `k8s.io/api/core/v1`
	`k8s.io/klog/v2`
	"volcano.sh/apis/pkg/apis/scheduling"
	`volcano.sh/volcano/pkg/scheduler/api`
	`volcano.sh/volcano/pkg/scheduler/api/helpers`
	`volcano.sh/volcano/pkg/scheduler/framework`
	`volcano.sh/volcano/pkg/scheduler/metrics`
	`volcano.sh/volcano/pkg/scheduler/plugins/util`
)

// queueAttr is used to store the attributes of a queue.
type queueAttr struct {
	queueID        api.QueueID   // queue UID
	name           string        // queue name
	share          float64       // share = max(allocated/deserved) of all resources
	deserved       *api.Resource // deserved = min(realCapability, max(guarantee, queue.deserved))
	allocated      *api.Resource // allocated = sum(job.allocated) of all active jobs
	request        *api.Resource // request = sum(job.request) of all active jobs
	elastic        *api.Resource // elastic = job.allocated - job.minAvailable
	inqueue        *api.Resource // inqueue = sum(job.minAvailable) of all inqueue jobs
	capability     *api.Resource // the capability of a queue
	realCapability *api.Resource // the real capability of a queue = min(capability, guarantee + exceeded part of cluster)
	guarantee      *api.Resource // the guaranteed resource of a queue
}

// buildQueueAttrs builds the attributes for all queues in the session.
func (p *Plugin) buildQueueAttrs(ssn *framework.Session) bool {
	for _, queue := range ssn.Queues {
		guarantee := p.newQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Guarantee.Resource)
		p.totalGuarantee.Add(guarantee)
	}

	// Build attributes for Queues.
	for _, apiJob := range ssn.Jobs {
		job, err := p.NewJobInfo(apiJob)
		if err != nil {
			klog.Errorf(
				"Failed to create jobInfo for job <%s/%s>: %+v",
				apiJob.Namespace, apiJob.Name, err,
			)
			continue
		}
		klog.V(4).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
		if _, found := p.queueOpts[job.Queue]; !found {
			queue := ssn.Queues[job.Queue]
			qAttr := &queueAttr{
				queueID:    queue.UID,
				name:       queue.Name,
				deserved:   p.newQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Deserved),
				capability: p.newQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Capability),
				guarantee:  p.newQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Guarantee.Resource),
				allocated:  api.EmptyResource(),
				request:    api.EmptyResource(),
				elastic:    api.EmptyResource(),
				inqueue:    api.EmptyResource(),
			}
			realCapability := api.ExceededPart(p.totalResource, p.totalGuarantee).Add(qAttr.guarantee)
			qAttr.realCapability = realCapability
			if qAttr.capability.MilliCPU <= 0 {
				qAttr.capability.MilliCPU = realCapability.MilliCPU
			}
			if qAttr.capability.Memory <= 0 {
				qAttr.capability.Memory = realCapability.Memory
			}
			if !QueueHasCardQuota(queue.Queue) {
				qAttr.capability.ScalarResources = realCapability.ScalarResources
			}
			p.queueOpts[job.Queue] = qAttr
			klog.V(5).Infof("Added Queue <%s> attributes.", job.Queue)
		}

		qAttr := p.queueOpts[job.Queue]
		for status, tasks := range job.TaskStatusIndex {
			if api.AllocatedStatus(status) {
				for _, t := range tasks {
					qAttr.allocated.Add(t.Resreq)
					qAttr.request.Add(t.Resreq)
				}
			} else if status == api.Pending {
				for _, t := range tasks {
					qAttr.request.Add(t.Resreq)
				}
			}
		}

		if job.PodGroup.Status.Phase == scheduling.PodGroupInqueue {
			// deduct the resources of scheduling gated tasks in a job when calculating inqueued resources
			// so that it will not block other jobs from being inqueued.
			qAttr.inqueue.Add(job.DeductSchGatedResources(job.GetMinResources()))
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
			qAttr.inqueue.Add(job.DeductSchGatedResources(inqueued))
		}
		qAttr.elastic.Add(job.GetElasticResources())
		klog.V(5).Infof(
			"Queue %s allocated <%s> request <%s> inqueue <%s> elastic <%s>",
			qAttr.name,
			qAttr.allocated.String(),
			qAttr.request.String(),
			qAttr.inqueue.String(),
			qAttr.elastic.String(),
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
	return true
}

// newQueueResourceSupportingCard creates a new resource object from resource list
func (p *Plugin) newQueueResourceSupportingCard(q *scheduling.Queue, rl v1.ResourceList) *api.Resource {
	var (
		queueResource     = api.NewResource(rl)
		queueCardResource = GetCardResourceFromAnnotations(q.Annotations, QueueAnnotationKeyCardQuota)
	)
	if queueResource.ScalarResources == nil {
		queueResource.ScalarResources = make(map[v1.ResourceName]float64)
	}
	for cardName, cardCountMilli := range queueCardResource.ScalarResources {
		queueResource.ScalarResources[cardName] = cardCountMilli
		cardResourceName := p.cardNameToResourceName[cardName]
		queueResource.ScalarResources[cardResourceName] += cardCountMilli
	}
	return queueResource
}

func (p *Plugin) buildQueueMetrics(ssn *framework.Session) {
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
			attr := p.newQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Deserved)
			deservedCPU = attr.MilliCPU
			deservedMem = attr.Memory
			scalarResources = attr.ScalarResources
		}
		metrics.UpdateQueueDeserved(queueInfo.Name, deservedCPU, deservedMem, scalarResources)
		metrics.UpdateQueueAllocated(queueInfo.Name, 0, 0, map[v1.ResourceName]float64{})
		metrics.UpdateQueueRequest(queueInfo.Name, 0, 0, map[v1.ResourceName]float64{})
		guarantee := api.EmptyResource()
		if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
			guarantee = p.newQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Guarantee.Resource)
		}
		realCapacity := api.ExceededPart(p.totalResource, p.totalGuarantee).Add(guarantee)
		if len(queue.Queue.Spec.Capability) > 0 {
			capacity := p.newQueueResourceSupportingCard(queue.Queue, queue.Queue.Spec.Capability)
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

// updateShare updates the share of the queueAttr and records the metric.
func (p *Plugin) updateShare(attr *queueAttr) {
	updateQueueAttrShare(attr)
	metrics.UpdateQueueShare(attr.name, attr.share)
}

// updateQueueAttrShare updates the share of the queueAttr.
func updateQueueAttrShare(attr *queueAttr) {
	res := float64(0)
	for _, rn := range attr.deserved.ResourceNames() {
		res = max(res, helpers.Share(attr.allocated.Get(rn), attr.deserved.Get(rn)))
	}
	attr.share = res
}
