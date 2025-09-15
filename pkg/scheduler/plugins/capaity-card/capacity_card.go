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
	`github.com/gogf/gf/v2/util/gconv`
	`k8s.io/klog/v2`
	`volcano.sh/volcano/pkg/scheduler/api`
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName = "capacity-card"

	// MPSResourceName 用于抽象MPS资源类型,MPS的资源是固定的，都是这个ResourceName
	MPSResourceName = "nvidia.com/gpu.shared"
	// MpsReplicaLabel 节点上的MPS拆卡副本数量标签(一张卡拆成几分)
	MpsReplicaLabel = "nvidia.com/gpu.replicas"

	MpsSharedCardNamePattern       = "%s/mps-%dg*1/%d"
	MigSharedCardNamePattern       = "%s/mig-%s-mixed"
	MigResourceNamePrefix          = "nvidia.com/mig-"
	QueueAnnotationKeyCardQuota    = "volcano.sh/card.quota"
	JobAnnotationKeyCardQuota      = "volcano.sh/card.request"
	PodGroupAnnotationKeyCardQuota = JobAnnotationKeyCardQuota
	TaskAnnotationKeyCardQuota     = JobAnnotationKeyCardQuota
)

type Plugin struct {
	queueOpts        map[api.QueueID]*queueAttr
	totalResource    *api.Resource
	totalGuarantee   *api.Resource
	arguments        framework.Arguments
	resourcePrefixes []string
}

// New return capacity plugin.
func New(arguments framework.Arguments) framework.Plugin {
	return &Plugin{
		queueOpts:        map[api.QueueID]*queueAttr{},
		totalResource:    api.EmptyResource(),
		totalGuarantee:   api.EmptyResource(),
		arguments:        arguments,
		resourcePrefixes: gconv.Strings(arguments["resourcePrefixes"]),
	}
}

// Name returns name of the plugin.
func (p *Plugin) Name() string {
	return PluginName
}

// OnSessionOpen initializes the plugin state.
func (p *Plugin) OnSessionOpen(ssn *framework.Session) {
	p.totalResource = p.calculateTotalResourceFromSession(ssn)
	p.buildQueueAttrs(ssn)

	klog.V(4).Infof("The total cluster resource is: %v", p.totalResource)
	klog.V(4).Infof("The total guarantee resource is: %v", p.totalGuarantee)

	// PogGroup status from Pending to InQueue.
	// ssn.AddJobEnqueueableFn(p.Name(), func(obj interface{}) int {
	// 	return util.Permit
	// })

	// ssn.AddAllocatableFn(cp.Name(), func(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
	// 	if queue.Queue.Status.State != scheduling.QueueStateOpen {
	// 		klog.V(3).Infof("Queue <%s> current state: %s, cannot allocate task <%s>.", queue.Name, queue.Queue.Status.State, candidate.Name)
	// 		return false
	// 	}
	// 	if !readyToSchedule {
	// 		klog.V(3).Infof("Capacity plugin failed to check queue's hierarchical structure!")
	// 		return false
	// 	}
	// 	if hierarchyEnabled && !cp.isLeafQueue(queue.UID) {
	// 		klog.V(3).Infof("Queue <%s> is not a leaf queue, can not allocate task <%s>.", queue.Name, candidate.Name)
	// 		return false
	// 	}
	//
	// 	return cp.checkQueueAllocatableHierarchically(ssn, queue, candidate)
	// })
}

// OnSessionClose cleans up the plugin state.
func (p *Plugin) OnSessionClose(ssn *framework.Session) {
	p.totalResource = nil
	p.totalGuarantee = nil
	p.queueOpts = nil
}
