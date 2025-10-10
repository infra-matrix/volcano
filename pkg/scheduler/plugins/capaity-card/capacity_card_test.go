/*
Copyright 2024 The Volcano Authors.

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
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNewMilliQuantity(t *testing.T) {
	q1 := resource.NewMilliQuantity(int64(1), resource.DecimalSI)
	q2 := resource.NewMilliQuantity(int64(1000), resource.DecimalSI)
	fmt.Println(q1.String())
	fmt.Println(q2.String())
}
