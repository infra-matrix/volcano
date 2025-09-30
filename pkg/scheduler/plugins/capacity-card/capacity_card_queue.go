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
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/helpers"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
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
func (p *Plugin) buildQueueAttrs(ssn *framework.Session) {
	// initialize totalGuarantee from all queues.
	for _, queue := range ssn.Queues {
		guarantee := api.EmptyResource()
		if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
			guarantee.Add(api.NewResource(queue.Queue.Spec.Guarantee.Resource))
			p.totalNormalGuarantee.Add(api.NewResource(queue.Queue.Spec.Guarantee.Resource))
		}

		cardResource := p.getCardResourcesByQueue(queue.Queue)
		setScalarResources(guarantee, cardResource)
		p.totalGuarantee.Add(guarantee)
	}

	// build attributes for Queues.
	for _, job := range ssn.Jobs {
		var err error
		job, err = p.UpdateCardResourcesForJob(job)
		if err != nil {
			klog.Errorf(
				"Failed to update card resources for job <%s/%s>: %+v",
				job.Namespace, job.Name, err,
			)
			return
		}

		p.buildQueueAttrByJob(ssn, job)
	}

	// update queue deserved and print queue info.
	for _, qAttr := range p.queueOpts {
		if qAttr.realCapability != nil {
			qAttr.deserved.MinDimensionResource(qAttr.realCapability, api.Infinity)
		}
		qAttr.deserved = helpers.Max(qAttr.deserved, qAttr.guarantee)
		p.updateShare(qAttr)
		klog.V(4).Infof(
			"The attributes of queue <%s>: capacity: <%v>, realCapability <%v>, allocate <%v>, request <%v>",
			qAttr.name, qAttr.capability, qAttr.realCapability, qAttr.allocated, qAttr.request,
		)
	}

	// add the queue comparison function according to the queue's priority and share.
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
}

// buildQueueAttrByJob builds/updates the attributes of a queue by a job.
func (p *Plugin) buildQueueAttrByJob(ssn *framework.Session, job *api.JobInfo) {
	klog.V(5).Infof("Considering Job <%s/%s>.", job.Namespace, job.Name)
	if _, found := p.queueOpts[job.Queue]; !found {
		queue := ssn.Queues[job.Queue]
		qAttr := p.newQueueAttr(queue)
		p.queueOpts[job.Queue] = qAttr
		klog.V(5).Infof("Created Queue attr for queue <%s>.", queue.Name)
	}

	// calculate allocated and request resource for running and pending tasks in a job to queue.
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

	// calculate inqueue resource for pending jobs to queue.
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
		int32(util.CalculateAllocatedTaskNum(job)) >= job.PodGroup.Spec.MinMember {
		inqueued := util.GetInqueueResource(job, job.Allocated)
		qAttr.inqueue.Add(job.DeductSchGatedResources(inqueued))
	}
	qAttr.elastic.Add(job.GetElasticResources())
	klog.V(5).Infof(
		"Built queue <%s> with job <%s/%s>: allocated <%s>, request <%s>, inqueue <%s>",
		qAttr.name, job.Namespace, job.Name,
		qAttr.allocated.String(),
		qAttr.request.String(),
		qAttr.inqueue.String(),
	)
}

// newQueueAttr creates a new queueAttr from a QueueInfo object.
func (p *Plugin) newQueueAttr(queue *api.QueueInfo) *queueAttr {
	// initialize deserved from queue.Queue.Spec.Deserved.
	attr := &queueAttr{
		queueID: queue.UID,
		name:    queue.Name,

		deserved:  api.NewResource(queue.Queue.Spec.Deserved),
		allocated: api.EmptyResource(),
		request:   api.EmptyResource(),
		elastic:   api.EmptyResource(),
		inqueue:   api.EmptyResource(),
		guarantee: api.EmptyResource(),
	}

	// initialize capability from queue.Queue.Spec.Capability.
	if len(queue.Queue.Spec.Capability) != 0 {
		attr.capability = api.NewResource(queue.Queue.Spec.Capability)
		if attr.capability.MilliCPU <= 0 {
			attr.capability.MilliCPU = math.MaxFloat64
		}
		if attr.capability.Memory <= 0 {
			attr.capability.Memory = math.MaxFloat64
		}
	}

	// initialize guarantee from queue.Queue.Spec.Guarantee.Resource.
	if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
		attr.guarantee = api.NewResource(queue.Queue.Spec.Guarantee.Resource)
	}

	// initialize realCapability from p.totalResource, p.totalGuarantee and attr.guarantee.
	realCapability := api.ExceededPart(p.totalNormalResource, p.totalNormalGuarantee).Add(attr.guarantee)
	if attr.capability == nil {
		attr.capability = api.EmptyResource()
		attr.realCapability = realCapability
	} else {
		realCapability.MinDimensionResource(attr.capability, api.Infinity)
		attr.realCapability = realCapability
	}

	// initialize card resources
	cardResources := p.getCardResourcesByQueue(queue.Queue)
	setScalarResources(attr.capability, cardResources)
	setScalarResources(attr.deserved, cardResources)
	setScalarResources(attr.guarantee, cardResources)
	setScalarResources(attr.realCapability, cardResources)

	return attr
}

// getCardResourcesByQueue returns the card resources of a queue.
func (p *Plugin) getCardResourcesByQueue(q *scheduling.Queue) map[v1.ResourceName]float64 {
	cardResource := map[v1.ResourceName]float64{}
	rawCardResource := GetCardResourceFromAnnotations(q.Annotations, QueueAnnotationKeyCardQuota)
	for cardName, cardCountMilli := range rawCardResource {
		// convert card name to real resource name, and add the card count to the resource.
		// for example, convert "NVIDIA-H200" to "nvidia.com/gpu"
		cardResourceName := p.cardNameToResourceName[cardName]
		if cardResourceName == "" {
			klog.Warningf("No resource name found for card <%s> in queue <%s>", cardName, q.Name)
			continue
		}

		cardResource[cardName] = cardCountMilli
		cardResource[cardResourceName] = cardCountMilli
	}
	return cardResource
}

// setScalarResources sets the scalar resources of a resource.
func setScalarResources(resource *api.Resource, scalar map[v1.ResourceName]float64) {
	for name, quant := range scalar {
		if resource.ScalarResources == nil {
			resource.ScalarResources = map[v1.ResourceName]float64{}
		}
		resource.ScalarResources[name] = quant
	}
}

// buildQueueMetrics builds the metrics for all queues in the session.
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
		// get card resources
		cardResources := p.getCardResourcesByQueue(queue.Queue)
		deservedCPU, deservedMem, scalarResources := 0.0, 0.0, map[v1.ResourceName]float64{}
		if queue.Queue.Spec.Deserved != nil {
			attr := api.NewResource(queue.Queue.Spec.Deserved)
			setScalarResources(attr, cardResources)
			deservedCPU = attr.MilliCPU
			deservedMem = attr.Memory
			scalarResources = attr.ScalarResources
		}
		metrics.UpdateQueueDeserved(queueInfo.Name, deservedCPU, deservedMem, scalarResources)
		metrics.UpdateQueueAllocated(queueInfo.Name, 0, 0, map[v1.ResourceName]float64{})
		metrics.UpdateQueueRequest(queueInfo.Name, 0, 0, map[v1.ResourceName]float64{})
		guarantee := api.EmptyResource()
		if len(queue.Queue.Spec.Guarantee.Resource) != 0 {
			guarantee = api.NewResource(queue.Queue.Spec.Guarantee.Resource)
			setScalarResources(guarantee, cardResources)
		}
		realCapacity := api.ExceededPart(p.totalNormalResource, p.totalNormalGuarantee).Add(guarantee)
		if len(queue.Queue.Spec.Capability) > 0 {
			capacity := api.NewResource(queue.Queue.Spec.Capability)
			setScalarResources(capacity, cardResources)
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
