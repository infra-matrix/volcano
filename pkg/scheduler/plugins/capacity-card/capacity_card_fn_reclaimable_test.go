package capacitycard

import (
	"os"
	"testing"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/actions/reclaim"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/conformance"
	"volcano.sh/volcano/pkg/scheduler/plugins/drf"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/plugins/priority"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func TestMain(m *testing.M) {
	options.Default()
	os.Exit(m.Run())
}

func TestDoesReclaimeeContainReclaimerResource(t *testing.T) {

	cases := []struct {
		name             string
		reclaimerRes     *api.Resource
		reclaimeeRes     *api.Resource
		expectedContains bool
	}{
		{
			name: "Reclaimee contains reclaimer resources",
			reclaimerRes: func() *api.Resource {
				res := api.EmptyResource()
				res.ScalarResources = map[v1.ResourceName]float64{
					"cardA": 4,
					"cardB": 2,
				}
				return res
			}(),
			reclaimeeRes: func() *api.Resource {
				res := api.EmptyResource()
				res.ScalarResources = map[v1.ResourceName]float64{
					"cardA": 4,
					"cardB": 2,
				}
				return res
			}(),

			expectedContains: true,
		},
		{
			name: "Reclaimee contains reclaimer resources",
			reclaimerRes: func() *api.Resource {
				res := api.EmptyResource()
				res.ScalarResources = map[v1.ResourceName]float64{
					"cardA": 4,
					"cardB": 2,
				}
				return res
			}(),
			reclaimeeRes: func() *api.Resource {
				res := api.EmptyResource()
				res.ScalarResources = map[v1.ResourceName]float64{
					"cardA": 1,
					"cardB": 1,
				}
				return res
			}(),

			expectedContains: true,
		},
		{
			name: "Reclaimee contains reclaimer resources",
			reclaimerRes: func() *api.Resource {
				res := api.EmptyResource()
				res.ScalarResources = map[v1.ResourceName]float64{
					"cardA": 4,
					"cardB": 2,
				}
				return res
			}(),
			reclaimeeRes: func() *api.Resource {
				res := api.EmptyResource()
				res.ScalarResources = map[v1.ResourceName]float64{
					"cardA": 1,
				}
				return res
			}(),

			expectedContains: true,
		},
		{
			name: "Reclaimee not contains reclaimer resources",
			reclaimerRes: func() *api.Resource {
				res := api.EmptyResource()
				res.ScalarResources = map[v1.ResourceName]float64{
					"cardA": 4,
					"cardB": 2,
				}
				return res
			}(),
			reclaimeeRes: func() *api.Resource {
				res := api.EmptyResource()
				res.ScalarResources = map[v1.ResourceName]float64{
					"cardC": 4,
					"cardD": 2,
				}
				return res
			}(),

			expectedContains: false,
		},
		{
			name: "Reclaimee contains reclaimer resources cpu mem",
			reclaimerRes: func() *api.Resource {
				res := api.EmptyResource()
				res.MilliCPU = 2000
				res.Memory = 4096

				return res
			}(),
			reclaimeeRes: func() *api.Resource {
				res := api.EmptyResource()
				res.MilliCPU = 1000
				res.Memory = 2048
				return res
			}(),

			expectedContains: true,
		},
		{
			name: "Reclaimee not contains reclaimer resources cpu mem",
			reclaimerRes: func() *api.Resource {
				res := api.EmptyResource()
				res.MilliCPU = 2000
				res.Memory = 4096

				return res
			}(),
			reclaimeeRes: func() *api.Resource {
				res := api.EmptyResource()

				return res
			}(),

			expectedContains: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := DoesReclaimeeContainReclaimerResource(c.reclaimerRes, c.reclaimeeRes)
			if result != c.expectedContains {
				t.Errorf("Expected %v, but got %v", c.expectedContains, result)
			}
		})
	}
}

func TestReclaim(t *testing.T) {
	// queues
	cpuMemRes := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("10"),
		v1.ResourceMemory: resource.MustParse("10Gi"),
	}
	cardRes := map[string]string{
		"volcano.sh/card.quota": `{"NVIDIA-H200":8, "NVIDIA-H20":8}`,
	}
	falseVal, trueVal := false, true

	queueInference := util.BuildQueueWithAnnos("q-i", 1, cpuMemRes, cardRes)
	queueInference.Spec.Reclaimable = &falseVal
	queueTraining := util.BuildQueueWithAnnos("q-t", 1, cpuMemRes, cardRes)
	queueTraining.Spec.Reclaimable = &falseVal
	queueHybrid := util.BuildQueueWithAnnos("q-h", 1, cpuMemRes, cardRes)
	queueHybrid.Spec.Reclaimable = &trueVal

	// pods

	p1 := util.BuildPod("c1", "preemptee1-1", "n1", v1.PodRunning,
		api.BuildResourceList("1", "1G", []api.ScalarResource{{Name: "nvidia.com/gpu", Value: "2"}}...), "pg1",
		map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string))
	p1.Annotations["volcano.sh/service.type"] = "inference"
	p1.Annotations["volcano.sh/preemptable"] = "false"
	p1.Annotations["volcano.sh/card.name"] = "NVIDIA-H200"
	p1.Annotations["scheduling.volcano.sh/queue-name"] = "q-i"

	p2 := util.BuildPod("c1", "preemptee1-2", "n1", v1.PodRunning,
		api.BuildResourceList("1", "1G", []api.ScalarResource{{Name: "nvidia.com/gpu", Value: "4"}}...), "pg2",
		map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string))
	p2.Annotations["volcano.sh/service.type"] = "training"
	p2.Annotations["volcano.sh/preemptable"] = "false"
	p2.Annotations["volcano.sh/card.name"] = "NVIDIA-H200"
	p2.Annotations["scheduling.volcano.sh/queue-name"] = "q-t"

	p3 := util.BuildPod("c1", "preemptee2-1", "n1", v1.PodRunning,
		api.BuildResourceList("1", "1G", []api.ScalarResource{{Name: "nvidia.com/gpu", Value: "2"}}...), "pg3",
		map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string))
	p3.Annotations["volcano.sh/service.type"] = "training"
	p3.Annotations["volcano.sh/preemptable"] = "true"
	p3.Annotations["volcano.sh/card.name"] = "NVIDIA-H200"
	p3.Annotations["scheduling.volcano.sh/queue-name"] = "q-h"

	p4 := util.BuildPod("c1", "preemptor1", "", v1.PodPending,
		api.BuildResourceList("1", "1G", []api.ScalarResource{{Name: "nvidia.com/gpu", Value: "2"}}...), "pg4",
		make(map[string]string), make(map[string]string))
	p4.Annotations["volcano.sh/service.type"] = "inference"
	p4.Annotations["volcano.sh/preemptable"] = "false"
	p4.Annotations["volcano.sh/card.name"] = "NVIDIA-H200"
	p4.Annotations["scheduling.volcano.sh/queue-name"] = "q-i"

	tests := []uthelper.TestCommonStruct{
		{
			Name: "can reclaim when capacity is enough",
			Plugins: map[string]framework.PluginBuilder{
				priority.PluginName:    priority.New,
				gang.PluginName:        gang.New,
				conformance.PluginName: conformance.New,
				drf.PluginName:         drf.New,
				predicates.PluginName:  predicates.New,
				PluginName:             New,
			},
			PriClass: []*schedulingv1.PriorityClass{
				util.BuildPriorityClass("low-priority", 100),
				util.BuildPriorityClass("mid-priority", 500),
				util.BuildPriorityClass("high-priority", 1000),
			},
			PodGroups: []*schedulingv1beta1.PodGroup{
				func() *schedulingv1beta1.PodGroup {
					pg := util.BuildPodGroupWithAnno("pg1", "c1", "q-i", 1, nil, schedulingv1beta1.PodGroupInqueue, map[string]string{"volcano.sh/service.type": "inference"})
					pg.Spec.PriorityClassName = "mid-priority"
					return pg
				}(),
				func() *schedulingv1beta1.PodGroup {
					pg := util.BuildPodGroupWithAnno("pg2", "c1", "q-t", 1, nil, schedulingv1beta1.PodGroupInqueue, map[string]string{"volcano.sh/service.type": "training"})
					pg.Spec.PriorityClassName = "low-priority"
					return pg
				}(),
				func() *schedulingv1beta1.PodGroup {
					pg := util.BuildPodGroupWithAnno("pg3", "c1", "q-h", 1, nil, schedulingv1beta1.PodGroupInqueue, map[string]string{"volcano.sh/service.type": "training"})
					pg.Spec.PriorityClassName = "high-priority"
					return pg
				}(),
				func() *schedulingv1beta1.PodGroup {
					pg := util.BuildPodGroupWithAnno("pg4", "c1", "q-i", 1, nil, schedulingv1beta1.PodGroupInqueue, map[string]string{"volcano.sh/service.type": "inference"})
					pg.Spec.PriorityClassName = "high-priority"
					return pg
				}(),
			},
			Pods: []*v1.Pod{
				p1, p2, p3, p4,
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("40", "40Gi",
					// scalar resources
					[]api.ScalarResource{{Name: "pods", Value: "10"},
						{Name: "nvidia.com/gpu", Value: "8"}}...),
					// labels
					map[string]string{
						"nvidia.com/gpu.product": "NVIDIA-H200",
						"nvidia.com/gpu.count":   "8",
						"nvidia.com/gpu.memory":  "81920", // 80GB in MB
					}),
			},
			Queues: []*schedulingv1beta1.Queue{
				queueInference,
				queueTraining,
				queueHybrid,
			},
			ExpectEvictNum: 1,
			ExpectEvicted:  []string{"c1/preemptee2-1"},
		},
	}

	reclaimAction := reclaim.New()

	trueValue := true
	falseValue := false
	tiers := []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:             priority.PluginName,
					EnabledJobOrder:  &trueValue,
					EnabledTaskOrder: &trueValue,
				},
				{
					Name:               gang.PluginName,
					EnabledJobStarving: &trueValue,
					EnablePreemptive:   &falseValue,
				},
				{
					Name: conformance.PluginName,
					// EnabledReclaimable: &trueValue,
				},

				{
					Name: drf.PluginName,
					// EnabledReclaimable: &trueValue,
					// EnabledQueueOrder:  &trueValue,
					EnablePreemptive: &falseValue,
				},
				{
					Name: predicates.PluginName,
					// EnabledReclaimable: &trueValue,
					// EnabledQueueOrder:  &trueValue,
					// EnablePreemptive:   &trueValue,
				},
				{
					Name:               PluginName,
					EnabledReclaimable: &trueValue,
					Arguments: map[string]interface{}{
						cardUnlimitedCpuMemory: true,
						"podOwnerReferenceToServiceType": map[string]string{
							"ReplicaSet": "inference",
							"Deployment": "inference",
							"Job":        "training",
						},
					},
				},
			},
		},
	}
	for i, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			test.RegisterSession(tiers, nil)
			defer test.Close()
			test.Run([]framework.Action{reclaimAction})
			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}
