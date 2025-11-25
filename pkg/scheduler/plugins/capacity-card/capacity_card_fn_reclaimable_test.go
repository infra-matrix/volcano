package capacitycard

import (
	"os"
	"testing"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
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
				util.BuildPodGroupWithPrio("pg1", "c1", "q1", 1, nil, schedulingv1beta1.PodGroupInqueue, "mid-priority"),
				util.BuildPodGroupWithPrio("pg2", "c1", "q2", 1, nil, schedulingv1beta1.PodGroupInqueue, "low-priority"), // reclaimed first
				util.BuildPodGroupWithPrio("pg3", "c1", "q3", 1, nil, schedulingv1beta1.PodGroupInqueue, "high-priority"),
			},
			Pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1-1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee1-2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg1", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2-1", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg2", map[string]string{schedulingv1beta1.PodPreemptable: "true"}, make(map[string]string)),
				util.BuildPod("c1", "preemptee2-2", "n1", v1.PodRunning, api.BuildResourceList("1", "1G"), "pg2", map[string]string{schedulingv1beta1.PodPreemptable: "false"}, make(map[string]string)),
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, api.BuildResourceList("1", "1G"), "pg3", make(map[string]string), make(map[string]string)),
			},
			Nodes: []*v1.Node{
				util.BuildNode("n1", api.BuildResourceList("4", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), make(map[string]string)),
			},
			Queues: []*schedulingv1beta1.Queue{
				util.BuildQueue("q1", 1, nil),
				util.BuildQueue("q2", 1, nil),
				util.BuildQueue("q3", 1, nil),
			},
			ExpectEvictNum: 1,
			ExpectEvicted:  []string{"c1/preemptee2-1"}, // low priority job's preemptable pod is evicted
		},
	}

	reclaim := reclaim.New()
	trueValue := true
	falseValue := false
	tiers := []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name: priority.PluginName,
					// EnabledJobOrder:  &trueValue,
					// EnabledTaskOrder: &trueValue,
				},
				{
					Name: gang.PluginName,
					// EnabledReclaimable: &trueValue,
					// EnabledJobStarving: &trueValue,
					EnablePreemptive: &falseValue,
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
					},
				},
			},
		},
	}
	for i, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			test.RegisterSession(tiers, nil)
			defer test.Close()
			test.Run([]framework.Action{reclaim})
			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}
