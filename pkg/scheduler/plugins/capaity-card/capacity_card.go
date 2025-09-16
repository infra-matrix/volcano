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
	corev1 `k8s.io/api/core/v1`
	v1 `k8s.io/client-go/listers/core/v1`
	`k8s.io/klog/v2`
	`volcano.sh/volcano/pkg/scheduler/api`
	"volcano.sh/volcano/pkg/scheduler/framework"
	`volcano.sh/volcano/pkg/scheduler/plugins/util`
)

const (
	PluginName                  = "capacity-card"
	MPSResourceName             = "nvidia.com/gpu.shared"
	MpsReplicaLabel             = "nvidia.com/gpu.replicas"
	MpsSharedCardNamePattern    = "%s/mps-%dg*1/%d"
	MigSharedCardNamePattern    = "%s/mig-%s-mixed"
	MigResourceNamePrefix       = "nvidia.com/mig-"
	QueueAnnotationKeyCardQuota = "volcano.sh/card.quota"
	JobAnnotationKeyCardRequest = "volcano.sh/card.request"
	TaskAnnotationKeyCardName   = "volcano.sh/card.name"
	MultiCardSeparator          = "|"
	configResourcePrefixesName  = "resourcePrefixes"
	cardCountQuantityMultiplier = 1000
)

// Plugin implements the capacity plugin.
type Plugin struct {
	queueOpts              map[api.QueueID]*queueAttr
	totalResource          *api.Resource
	totalGuarantee         *api.Resource
	cardNameToResourceName map[corev1.ResourceName]corev1.ResourceName
	nodeLister             v1.NodeLister
	arguments              framework.Arguments
	resourcePrefixes       []string
}

// New return capacity plugin.
func New(arguments framework.Arguments) framework.Plugin {
	return &Plugin{
		queueOpts:              map[api.QueueID]*queueAttr{},
		totalResource:          api.EmptyResource(),
		totalGuarantee:         api.EmptyResource(),
		cardNameToResourceName: map[corev1.ResourceName]corev1.ResourceName{},
		arguments:              arguments,
		resourcePrefixes:       gconv.Strings(arguments[configResourcePrefixesName]),
	}
}

// Name returns name of the plugin.
func (p *Plugin) Name() string {
	return PluginName
}

// OnSessionOpen initializes the plugin state.
func (p *Plugin) OnSessionOpen(ssn *framework.Session) {
	readyToSchedule := p.buildTotalResource(ssn)
	if readyToSchedule {
		readyToSchedule = p.buildQueueAttrs(ssn)
	}
	if readyToSchedule {
		p.buildQueueMetrics(ssn)
	}

	klog.V(4).Infof("Total resource is: %v", p.totalResource)
	klog.V(4).Infof("Total guarantee is: %v", p.totalGuarantee)

	// Job enqueueable check.
	ssn.AddJobEnqueueableFn(p.Name(), func(obj any) int {
		jobInfo := obj.(*api.JobInfo)
		if !readyToSchedule {
			klog.V(2).Infof(
				"Plugin <%s> is not ready to schedule, reject job <%s/%s>.",
				p.Name(), jobInfo.Namespace, jobInfo.Name,
			)
			return util.Reject
		}
		return p.JobEnqueueableFn(ssn, jobInfo)
	})

	// Task allocatable check.
	ssn.AddAllocatableFn(p.Name(), func(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
		if !readyToSchedule {
			klog.V(2).Infof(
				"Plugin <%s> is not ready to schedule, reject task <%s/%s>.",
				p.Name(), candidate.Namespace, candidate.Name,
			)
			return false
		}
		return p.AllocatableFn(queue, candidate)
	})
}

// OnSessionClose cleans up the plugin state.
func (p *Plugin) OnSessionClose(_ *framework.Session) {
	p.queueOpts = nil
	p.totalResource = nil
	p.totalGuarantee = nil
	p.cardNameToResourceName = nil
}
