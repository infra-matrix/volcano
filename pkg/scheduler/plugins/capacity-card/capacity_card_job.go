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
	"strings"

	v1 "k8s.io/api/core/v1"
	resourcehelper "k8s.io/kubectl/pkg/util/resource"
	"volcano.sh/volcano/pkg/scheduler/api"
)

// JobInfo describes the job used in capacity plugin.
type JobInfo struct {
	*api.JobInfo

	allocated    *api.Resource
	totalRequest *api.Resource

	// preCheckCardResource is the card resource retrieved from job annotation for pre-check purpose.
	// Pre-check will prevent pending pod created(working in JobEnqueueableFn) when queue has no enough card resource.
	// It is suggested to set this annotation for jobs which need card resource, although it is optional.
	preCheckCardResource *api.Resource
}

// NewJobInfo creates a JobInfo instance.
func (p *Plugin) NewJobInfo(job *api.JobInfo) (*JobInfo, error) {
	// read card request from Job(PodGroup).
	preCheckCardResource := GetCardResourceFromAnnotations(
		job.PodGroup.Annotations,
		JobAnnotationKeyCardRequest,
	)

	// job allocated card resource,
	allocated := api.EmptyResource()
	if job.Allocated != nil {
		allocated.MilliCPU = job.Allocated.MilliCPU
		allocated.Memory = job.Allocated.Memory
	}
	// read task card request from task annotations.
	realCardRequest := api.EmptyResource()
	for _, ti := range job.Tasks {
		if ti.Pod != nil {
			taskCardResource, err := p.getCardResourceFromTask(ti)
			if err != nil {
				return nil, err
			}
			realCardRequest.Add(taskCardResource)

			if api.AllocatedStatus(ti.Status) {
				allocated.Add(taskCardResource)
			}
		}
	}

	// job total request card resource
	totalRequest := api.EmptyResource()
	if job.TotalRequest != nil {
		totalRequest.MilliCPU = job.TotalRequest.MilliCPU
		totalRequest.Memory = job.TotalRequest.Memory
	}
	if realCardRequest.IsEmpty() {
		totalRequest.Add(preCheckCardResource)
	} else {
		preCheckCardResource = realCardRequest
		totalRequest.Add(realCardRequest)
	}

	return &JobInfo{
		JobInfo:              job,
		totalRequest:         totalRequest,
		allocated:            allocated,
		preCheckCardResource: preCheckCardResource,
	}, nil
}

// GetMinResources return the min resources of PodGroup.
func (ji *JobInfo) GetMinResources() *api.Resource {
	jobResource := ji.JobInfo.GetMinResources()
	jobResource.ScalarResources = ji.preCheckCardResource.ScalarResources
	return jobResource
}

// GetElasticResources returns those partly resources in allocated which are more than its minResource
func (ji *JobInfo) GetElasticResources() *api.Resource {
	if ji.allocated == nil {
		return api.EmptyResource()
	}
	var (
		minResource = ji.GetMinResources()
		elastic     = api.ExceededPart(ji.allocated, minResource)
	)
	return elastic
}

// getCardNameFromTask retrieves the card name from task's pod annotations.
func (p *Plugin) getCardNameFromTask(ti *api.TaskInfo) string {
	if ti.Pod == nil {
		return ""
	}
	return ti.Pod.Annotations[TaskAnnotationKeyCardName]
}

// getCardResourceFromTask retrieves the card resource from task's pod annotations and requests/limits.
func (p *Plugin) getCardResourceFromTask(ti *api.TaskInfo) (*api.Resource, error) {
	if ti.Pod == nil {
		return api.EmptyResource(), nil
	}

	cardName := p.getCardNameFromTask(ti)
	if cardName == "" {
		// no card requested, might be CPU type task.
		return api.EmptyResource(), nil
	}

	// if multi-cards are requested, retrieve the confirmed card name from bound node.
	if strings.Contains(cardName, MultiCardSeparator) {
		multiCardNames := strings.Split(cardName, MultiCardSeparator)
		if ti.Pod.Spec.NodeName == "" {
			podCardResource, err := p.getCardResourceFromTaskPod(multiCardNames[0], ti.Pod)
			if err != nil {
				return api.EmptyResource(), err
			}
			for _, scalarCount := range podCardResource.ScalarResources {
				// change to multi-card resource.
				return &api.Resource{
					ScalarResources: map[v1.ResourceName]float64{
						v1.ResourceName(cardName): scalarCount,
					},
				}, nil
			}
		}
		return p.getCardResourceFromNodeNameForMultiCardTask(ti, cardName)
	}
	return p.getCardResourceFromTaskPod(cardName, ti.Pod)
}

// getCardResourceFromNodeNameForMultiCardTask retrieves the real card resource from node label if pod is
// already bound to certain node.
func (p *Plugin) getCardResourceFromNodeNameForMultiCardTask(
	ti *api.TaskInfo, multiCardName string,
) (*api.Resource, error) {
	// already bound to certain node, retrieve the real card name from node label.
	node, err := p.nodeLister.Get(ti.Pod.Spec.NodeName)
	if err != nil {
		return api.EmptyResource(), fmt.Errorf(
			"failed to get node <%s> for task <%s/%s>: %+v",
			ti.Pod.Spec.NodeName, ti.Namespace, ti.Name, err,
		)
	}
	var (
		nodeCardInfo   = p.getCardResourceFromNode(node)
		multiCardNames = strings.Split(multiCardName, MultiCardSeparator)
	)
	for _, singleCardName := range multiCardNames {
		if _, ok := nodeCardInfo.CardNameToResourceName[v1.ResourceName(singleCardName)]; ok {
			podCardResource, err := p.getCardResourceFromTaskPod(singleCardName, ti.Pod)
			if err == nil {
				return podCardResource, nil
			}
		}
	}
	return api.EmptyResource(), fmt.Errorf(
		"no valid card found on node <%s> for task <%s/%s> with multi-card name <%s>",
		node.Name, ti.Namespace, ti.Name, multiCardName,
	)
}

// getCardResourceFromTaskPod retrieves the card resource from task's pod requests/limits.
func (p *Plugin) getCardResourceFromTaskPod(cardName string, pod *v1.Pod) (*api.Resource, error) {
	if pod == nil {
		return api.EmptyResource(), fmt.Errorf("invalid parameter: pod is nil")
	}

	// retrieve card resource name from requests/limits.
	// eg: cardName is "NVIDIA-H200", the resource name in requests/limits is "nvidia.com/gpu".
	cardResourceName, ok := p.cardNameToResourceName[v1.ResourceName(cardName)]
	if !ok {
		return api.EmptyResource(), fmt.Errorf("no resource name found for card <%s>", cardName)
	}
	// retrieve card count from requests/limits by card resource name.
	// eg: card name is "NVIDIA-H200", resource name is "nvidia.com/gpu",
	// the "nvidia.com/gpu" count is 2 in pod requests/limits,
	// then the "NVIDIA-H200" card count is 2.
	podRequests, podLimits := resourcehelper.PodRequestsAndLimits(pod)
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

// GetTaskRequestResources get the task request resources
func (p *Plugin) GetTaskRequestResources(task *api.TaskInfo) (*api.Resource, error) {
	taskCardResource, err := p.getCardResourceFromTask(task)
	if err != nil {
		return nil, err
	}

	// job total request card resource
	totalRequest := api.EmptyResource()
	if task.Resreq != nil {
		totalRequest.MilliCPU = task.Resreq.MilliCPU
		totalRequest.Memory = task.Resreq.Memory
	}
	totalRequest.Add(taskCardResource)

	return totalRequest, nil
}
