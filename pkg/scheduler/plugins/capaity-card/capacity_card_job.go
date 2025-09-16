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
	`fmt`
	`strings`

	v1 `k8s.io/api/core/v1`
	resourcehelper "k8s.io/kubectl/pkg/util/resource"
	`volcano.sh/volcano/pkg/scheduler/api`
)

// JobInfo describes the job used in capacity plugin.
type JobInfo struct {
	*api.JobInfo
	jobCardResource *api.Resource
}

// NewJobInfo creates a JobInfo instance.
func (p *Plugin) NewJobInfo(job *api.JobInfo) (*JobInfo, error) {
	// read card request from Job(PodGroup).
	jobCardResource := GetCardResourceFromAnnotations(
		job.PodGroup.Annotations,
		JobAnnotationKeyCardRequest,
	)
	// reset job allocated resource, will recalculate it below
	job.Allocated = api.EmptyResource()
	// read task card request from task annotations.
	realCardRequest := api.EmptyResource()
	for taskId, ti := range job.Tasks {
		if ti.Pod != nil {
			taskCardResource, err := p.getCardResourceFromTask(ti)
			if err != nil {
				return nil, err
			}
			realCardRequest.Add(taskCardResource)
			// re-calculate the card request for task.
			// this task info resource assignment will update the queue.Status.Allocated after session close.
			for scalarName, scalarCount := range taskCardResource.ScalarResources {
				ti.Resreq.SetScalar(scalarName, scalarCount)
			}
			job.Tasks[taskId] = ti
		}
		if api.AllocatedStatus(ti.Status) {
			job.Allocated.Add(ti.Resreq)
		}
	}
	if realCardRequest.IsEmpty() {
		job.TotalRequest.Add(jobCardResource)
	} else {
		jobCardResource = realCardRequest
		job.TotalRequest.Add(realCardRequest)
	}

	return &JobInfo{
		JobInfo:         job,
		jobCardResource: jobCardResource,
	}, nil
}

// GetMinResources return the min resources of PodGroup.
func (ji *JobInfo) GetMinResources() *api.Resource {
	jobResource := ji.JobInfo.GetMinResources()
	jobResource.Add(ji.jobCardResource)
	return jobResource
}

// GetElasticResources returns those partly resources in allocated which are more than its minResource
func (ji *JobInfo) GetElasticResources() *api.Resource {
	if ji.Allocated == nil {
		return api.EmptyResource()
	}
	var (
		minResource = ji.GetMinResources()
		elastic     = api.ExceededPart(ji.Allocated, minResource)
	)
	return elastic
}

// getCardResourceFromTask retrieves the card resource from task's pod annotations and requests/limits.
func (p *Plugin) getCardResourceFromTask(ti *api.TaskInfo) (*api.Resource, error) {
	if ti.Pod == nil {
		return api.EmptyResource(), nil
	}

	cardName := ti.Pod.Annotations[TaskAnnotationKeyCardName]
	if cardName == "" {
		// no card requested, might be CPU type task.
		return api.EmptyResource(), nil
	}

	// if multi-cards are requested, retrieve the confirmed card name from bound node.
	if strings.Contains(cardName, MultiCardSeparator) {
		if ti.Pod.Spec.NodeName == "" {
			return api.EmptyResource(), nil
		}
		node, err := p.nodeLister.Get(ti.Pod.Spec.NodeName)
		if err != nil {
			return api.EmptyResource(), fmt.Errorf(
				"failed to get node <%s> for task <%s/%s>: %+v",
				ti.Pod.Spec.NodeName, ti.Namespace, ti.Name, err,
			)
		}
		cardInfo := p.getCardInfoFromNode(node)
		if cardInfo.Name == "" {
			return api.EmptyResource(), fmt.Errorf(
				"no card info found from node <%s> for task <%s/%s>",
				node.Name, ti.Namespace, ti.Name,
			)
		}
		cardName = cardInfo.Name
	}

	// retrieve resource quantity from requests/limits.
	cardResourceName, ok := p.cardNameToResourceName[v1.ResourceName(cardName)]
	if !ok {
		return api.EmptyResource(), fmt.Errorf("no resource name found for card <%s>", cardName)
	}
	podRequests, podLimits := resourcehelper.PodRequestsAndLimits(ti.Pod)
	if quantity, found := podLimits[cardResourceName]; found {
		return &api.Resource{
			ScalarResources: map[v1.ResourceName]float64{
				v1.ResourceName(cardName): float64(quantity.Value() * cardCountQuantityMultiplier),
			},
		}, nil
	}
	if quantity, found := podRequests[cardResourceName]; found {
		return &api.Resource{
			ScalarResources: map[v1.ResourceName]float64{
				v1.ResourceName(cardName): float64(quantity.Value() * cardCountQuantityMultiplier),
			},
		}, nil
	}
	return api.EmptyResource(), fmt.Errorf(
		"no resource <%s> defined in reqests/limits for card <%s>",
		cardResourceName, cardName,
	)
}
