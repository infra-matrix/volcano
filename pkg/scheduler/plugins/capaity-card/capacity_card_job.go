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
	`volcano.sh/volcano/pkg/scheduler/api`
)

type JobInfo struct {
	*api.JobInfo
	cardResource *api.Resource
}

// NewJobInfo creates a JobInfo instance.
func NewJobInfo(job *api.JobInfo) *JobInfo {
	// 将智算卡请求量从PodGroup的Annotations中分离出来，放到资源的ScalarResources中
	cardResource := GetCardResourceFromAnnotations(
		job.PodGroup.Annotations,
		JobAnnotationKeyCardQuota,
	)
	if cardResource.IsEmpty() {
		cardResource = nil
	}
	// add card request to job total request
	job.TotalRequest.Add(cardResource)
	// read task card request from task annotations.
	for taskId, ti := range job.Tasks {
		if ti.Pod != nil {
			taskCardResource := GetCardResourceFromAnnotations(
				ti.Pod.Annotations,
				TaskAnnotationKeyCardQuota,
			)
			ti.Resreq.Add(taskCardResource)
			job.Tasks[taskId] = ti
		}
	}
	return &JobInfo{
		JobInfo:      job,
		cardResource: cardResource,
	}
}

// GetMinResources return the min resources of PodGroup.
func (ji *JobInfo) GetMinResources() *api.Resource {
	jobResource := ji.JobInfo.GetMinResources()
	if ji.cardResource != nil {
		jobResource.Add(ji.cardResource)
	}
	return jobResource
}
