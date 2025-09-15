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

	v1 `k8s.io/api/core/v1`
	resourcehelper "k8s.io/kubectl/pkg/util/resource"
	`volcano.sh/volcano/pkg/scheduler/api`
)

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
	if jobCardResource.IsEmpty() {
		jobCardResource = nil
	}
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
			ti.Resreq.Add(taskCardResource)
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

func (p *Plugin) getCardResourceFromTask(ti *api.TaskInfo) (*api.Resource, error) {
	if ti.Pod == nil {
		return api.EmptyResource(), nil
	}
	cardName := ti.Pod.Annotations[TaskAnnotationKeyCardName]
	if cardName == "" {
		return api.EmptyResource(), nil
	}
	cardResourceName, ok := p.cardNameToResourceName[v1.ResourceName(cardName)]
	if !ok {
		return api.EmptyResource(), fmt.Errorf("no resource name found for card <%s>", cardName)
	}
	podRequests, podLimits := resourcehelper.PodRequestsAndLimits(ti.Pod)
	if quantity, found := podLimits[cardResourceName]; found {
		return &api.Resource{
			ScalarResources: map[v1.ResourceName]float64{
				v1.ResourceName(cardName): float64(quantity.Value()),
			},
		}, nil
	}
	if quantity, found := podRequests[cardResourceName]; found {
		return &api.Resource{
			ScalarResources: map[v1.ResourceName]float64{
				v1.ResourceName(cardName): float64(quantity.Value()),
			},
		}, nil
	}
	return api.EmptyResource(), fmt.Errorf(
		"no resource <%s> defined in reqests/limits for card <%s>",
		cardResourceName, cardName,
	)
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
