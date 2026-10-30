// Copyright Dynatrace LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package measurements

import "github.com/google/uuid"

// InstanceID identifies this collector process. It is generated once per
// process start so a Bindplane server can separate time series reported by
// collectors that share an agent ID.
var InstanceID = uuid.Must(uuid.NewV7()).String()

// InstanceIDAttribute is the data point attribute that carries InstanceID on
// every reported throughput metric. The name matches the label Bindplane
// allowlists.
const InstanceIDAttribute = "bindplane_instance_id"
