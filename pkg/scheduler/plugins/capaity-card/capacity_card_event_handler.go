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
	`strings`

	`k8s.io/klog/v2`
	`volcano.sh/volcano/pkg/scheduler/framework`
	`volcano.sh/volcano/pkg/scheduler/metrics`
)

// OnAllocate is invoked when a task is allocated.
// This callback handler is very critical for capacity plugin to maintain the queue allocated resource,
// especially card resource is being allocated during current scheduling session.
func (p *Plugin) OnAllocate(ssn *framework.Session, event *framework.Event) {
	var (
		task         = event.Task
		taskJob      = ssn.Jobs[event.Task.Job]
		taskCardName = p.getCardNameFromTask(task)
		qAttr        = p.queueOpts[taskJob.Queue]
	)

	// if multi-cards requested, it converts multi-cards resource to real card resource.
	// for example, if a task requests "NVIDIA-H20|NVIDIA-H200" in its annotation,
	// and it here converts it to real card resource according to the node it is allocated on.
	if strings.Contains(taskCardName, MultiCardSeparator) {
		cardResource, err := p.getCardResourceFromNodeNameForMultiCardTask(task, taskCardName)
		if err != nil {
			klog.Errorf(
				"Failed to get card resource for multi-cards task <%s/%s>: %+v",
				task.Namespace, task.Name, err,
			)
			return
		}
		qAttr.allocated.Add(cardResource)
	} else {
		qAttr.allocated.Add(task.Resreq)
	}

	metrics.UpdateQueueAllocated(
		qAttr.name, qAttr.allocated.MilliCPU, qAttr.allocated.Memory, qAttr.allocated.ScalarResources,
	)
	p.updateShare(qAttr)
	klog.V(4).Infof(
		"Capacity AllocateFunc: task <%v/%v>, resreq <%v>, share <%v>",
		task.Namespace, task.Name, task.Resreq, qAttr.share,
	)
}

// OnDeallocate is invoked when a task is deallocated.
func (p *Plugin) OnDeallocate(ssn *framework.Session, event *framework.Event) {
	var (
		task         = event.Task
		taskJob      = ssn.Jobs[event.Task.Job]
		taskCardName = p.getCardNameFromTask(task)
		qAttr        = p.queueOpts[taskJob.Queue]
	)

	// if multi-cards requested, it converts multi-cards resource to real card resource.
	// for example, if a task requests "NVIDIA-H20|NVIDIA-H200" in its annotation,
	// and it here converts it to real card resource according to the node it is allocated on.
	if strings.Contains(taskCardName, MultiCardSeparator) {
		cardResource, err := p.getCardResourceFromNodeNameForMultiCardTask(task, taskCardName)
		if err != nil {
			klog.Errorf(
				"Failed to get card resource for multi-cards task <%s/%s>: %+v",
				task.Namespace, task.Name, err,
			)
			return
		}
		qAttr.allocated.Sub(cardResource)
	} else {
		qAttr.allocated.Sub(task.Resreq)
	}

	metrics.UpdateQueueAllocated(
		qAttr.name, qAttr.allocated.MilliCPU, qAttr.allocated.Memory, qAttr.allocated.ScalarResources,
	)
	klog.V(4).Infof(
		"Capacity EvictFunc: task <%v/%v>, resreq <%v>, share <%v>",
		task.Namespace, task.Name, task.Resreq, qAttr.share,
	)
}
