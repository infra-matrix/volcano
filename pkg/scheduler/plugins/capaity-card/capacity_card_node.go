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
	`math`
	`strings`

	`github.com/gogf/gf/v2/util/gconv`
	corev1 `k8s.io/api/core/v1`
	`k8s.io/apimachinery/pkg/labels`
	`k8s.io/klog/v2`
	`volcano.sh/volcano/pkg/scheduler/api`
	`volcano.sh/volcano/pkg/scheduler/framework`
)

// CardInfo defines the basic information of a card.
// One node only has one type of card.
type CardInfo struct {
	Name   string // card name
	Memory int64  // card memory in bytes
}

func (p *Plugin) buildTotalResource(ssn *framework.Session) bool {
	p.nodeLister = ssn.InformerFactory().Core().V1().Nodes().Lister()
	nodes, err := p.nodeLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("Failed to list nodes: %+v", err)
		return false
	}
	p.buildTotalResourceFromNodes(nodes)
	return true
}

func (p *Plugin) buildTotalResourceFromNodes(nodes []*corev1.Node, ) {
	var (
		totalNormalResource = make(corev1.ResourceList) // CPU, Memory, EphemeralStorage, etc.
		totalCardResource   = make(corev1.ResourceList) // GPU/NPU/PPU cards, etc.
	)
	for _, node := range nodes {
		addResourceList(
			totalNormalResource, node.Status.Capacity.DeepCopy(),
		)
		p.buildCardResourceFromNode(totalCardResource, node)
	}
	p.totalResource = api.NewResource(totalNormalResource)
	for resName, quantity := range totalCardResource {
		p.totalResource.AddScalar(resName, float64(quantity.Value()*cardCountQuantityMultiplier))
	}
}

func (p *Plugin) buildCardResourceFromNode(totalCardResource corev1.ResourceList, node *corev1.Node) {
	cardInfo := p.getCardInfoFromNode(node)
	for resName, cardCapacity := range node.Status.Capacity {
		// special MPS resource.
		if isMpsResourceName(resName) {
			if cardCapacity.Value() <= 0 {
				continue
			}
			mpsReplicas := gconv.Int(node.Labels[MpsReplicaLabel])
			if mpsReplicas > 0 {
				cardName := fmt.Sprintf(
					MpsSharedCardNamePattern,
					cardInfo.Name,
					int(math.Round(float64(cardInfo.Memory)/1024)), gconv.Int(mpsReplicas),
				)
				totalCardResource[corev1.ResourceName(cardName)] = cardCapacity.DeepCopy()
				p.cardNameToResourceName[corev1.ResourceName(cardName)] = resName
			}
			continue
		}

		// special MIG resource.
		if isMigResourceName(resName) {
			if cardCapacity.Value() <= 0 {
				continue
			}
			var (
				migSpec  = strings.TrimPrefix(string(resName), MigResourceNamePrefix)
				cardName = fmt.Sprintf(MigSharedCardNamePattern, cardInfo.Name, migSpec)
			)
			totalCardResource[corev1.ResourceName(cardName)] = cardCapacity.DeepCopy()
			p.cardNameToResourceName[corev1.ResourceName(cardName)] = resName
			continue
		}

		// 1. whole card resource
		// 2. parts are shared card resource, parts are whole card resource
		for _, resourcePrefix := range p.resourcePrefixes {
			if strings.HasPrefix(string(resName), resourcePrefix) && cardCapacity.Value() > 0 {
				totalCardResource[corev1.ResourceName(cardInfo.Name)] = cardCapacity.DeepCopy()
				p.cardNameToResourceName[corev1.ResourceName(cardInfo.Name)] = resName
				break
			}
		}
	}
}

func (p *Plugin) getCardInfoFromNode(node *corev1.Node) CardInfo {
	return CardInfo{
		Name:   p.getCardNameFromNode(node),
		Memory: p.getCardMemoryFromNode(node),
	}
}

func (p *Plugin) getCardNameFromNode(node *corev1.Node) string {
	for k, v := range node.Labels {
		for _, resourcePrefix := range p.resourcePrefixes {
			if strings.HasPrefix(k, resourcePrefix) && strings.HasSuffix(k, ".product") {
				return v
			}
		}
	}
	return ""
}

func (p *Plugin) getCardMemoryFromNode(node *corev1.Node) int64 {
	for k, v := range node.Labels {
		for _, resourcePrefix := range p.resourcePrefixes {
			if strings.HasPrefix(k, resourcePrefix) && strings.HasSuffix(k, ".memory") {
				return gconv.Int64(v) * 1024 * 1024
			}
		}
	}
	return 0
}

// isMpsResourceName checks if the resource name is MPS resource name.
func isMpsResourceName(resourceName corev1.ResourceName) bool {
	return resourceName == MPSResourceName
}

// isMigResourceName checks if the resource name is MIG resource name.
func isMigResourceName(resourceName corev1.ResourceName) bool {
	return strings.HasPrefix(string(resourceName), MigResourceNamePrefix)
}

func addResourceList(total corev1.ResourceList, add corev1.ResourceList) {
	for resourceName, quantity := range add {
		if val, ok := total[resourceName]; ok {
			val.Add(quantity)
			total[resourceName] = val
		} else {
			total[resourceName] = quantity.DeepCopy()
		}
	}
}
