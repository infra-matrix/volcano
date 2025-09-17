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

	corev1 `k8s.io/api/core/v1`
	`volcano.sh/volcano/pkg/scheduler/api`
	`volcano.sh/volcano/pkg/scheduler/framework`
)

// PredicateFn checks if a task can be scheduled on the node.
func (p *Plugin) PredicateFn(ssn *framework.Session, ti *api.TaskInfo, ni *api.NodeInfo) error {
	taskCardName := p.getCardNameFromTask(ti)
	if taskCardName == "" {
		// no card name requested, might be CPU type task.
		return nil
	}
	// if no multi-cards requested, no need to check here.
	if !strings.Contains(taskCardName, MultiCardSeparator) {
		return nil
	}

	var (
		taskJob            = ssn.Jobs[ti.Job]
		qAttr              = p.queueOpts[taskJob.Queue]
		queueCapability    = qAttr.capability
		taskMultiCardNames = strings.Split(taskCardName, MultiCardSeparator)
		availableCardNames = make([]string, 0)
	)

	// filter card names by node.
	nodeCardInfo := p.getCardResourceFromNode(ni.Node)
	for _, cardName := range taskMultiCardNames {
		if _, ok := nodeCardInfo.CardNameToResourceName[corev1.ResourceName(cardName)]; ok {
			availableCardNames = append(availableCardNames, cardName)
		}
	}
	if len(availableCardNames) == 0 {
		return fmt.Errorf(
			`task <%s/%s> in queue <%s> cannot be assigned to node <%s>, no available card %v found on node`,
			ti.Namespace, ti.Name, qAttr.name, ni.Name, availableCardNames,
		)
	}

	// queue quota check for the card name.
	var (
		logMsg           = ""
		taskReqCardCount = ti.Resreq.ScalarResources[corev1.ResourceName(taskCardName)]
	)
	if taskReqCardCount == 0 {
		// when bound to node, the multi-card name task resource name will be changed to the real card name.
		for _, cardName := range taskMultiCardNames {
			if count, ok := ti.Resreq.ScalarResources[corev1.ResourceName(cardName)]; ok && count > 0 {
				taskReqCardCount = count
				break
			}
		}
	}
	for _, availableCardName := range availableCardNames {
		var (
			cardQuotaInQueue  = queueCapability.ScalarResources[corev1.ResourceName(availableCardName)]
			toBeUsedCardCount = qAttr.allocated.ScalarResources[corev1.ResourceName(availableCardName)] +
				taskReqCardCount
		)
		if cardQuotaInQueue >= toBeUsedCardCount {
			return nil
		}
		if logMsg != "" {
			logMsg += "; "
		}
		logMsg += fmt.Sprintf(
			"%s: totalToBeUsedCardCount <%v>, cardQuotaInQueue <%v>",
			availableCardName, toBeUsedCardCount, cardQuotaInQueue,
		)
	}
	return fmt.Errorf(
		`task <%s/%s> in queue <%s> cannot be assigned to node <%s>, no available card quota %s`,
		ti.Namespace, ti.Name, qAttr.name, ni.Name, logMsg,
	)
}
