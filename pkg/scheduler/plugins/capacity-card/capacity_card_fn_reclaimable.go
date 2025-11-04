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
	`k8s.io/klog/v2`
	`volcano.sh/volcano/pkg/scheduler/api`
	`volcano.sh/volcano/pkg/scheduler/framework`
	`volcano.sh/volcano/pkg/scheduler/plugins/util`
)

func (p *Plugin) ReclaimableFn(
	ssn *framework.Session, reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo,
) ([]*api.TaskInfo, int) {
	var victims []*api.TaskInfo
	allocations := map[api.QueueID]*api.Resource{}
	for _, reclaimee := range reclaimees {
		job := ssn.Jobs[reclaimee.Job]
		attr := p.queueOpts[job.Queue]

		if _, found := allocations[job.Queue]; !found {
			allocations[job.Queue] = attr.allocated.Clone()
		}
		allocated := allocations[job.Queue]

		exceptReclaimee := allocated.Clone().Sub(reclaimee.Resreq)
		// When scalar resource not specified in deserved such as "pods", we should skip it and consider it as infinity,
		// so the following first condition will be true and the current queue will not be reclaimed.
		if allocated.LessEqual(attr.capability, api.Infinity) || !attr.guarantee.LessEqual(exceptReclaimee, api.Zero) {
			continue
		}
		allocated.Sub(reclaimee.Resreq)
		victims = append(victims, reclaimee)
	}
	klog.V(4).Infof("Victims from capacity plugin, victims=%+v reclaimer=%s", victims, reclaimer)
	return victims, util.Permit
}
