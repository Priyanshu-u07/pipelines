// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lifecycle

import (
	"testing"

	"github.com/kubeflow/pipelines/backend/src/common/util"
)

// Every message below was captured verbatim from a workflow running on Argo
// Workflows 4.0.4 and Kubernetes 1.35.0. They are recorded engine output, not
// strings invented to fit the classifier.
const (
	msgImagePullBackOff = `ImagePullBackOff: Back-off pulling image "python:99.99.99-nonexistent": ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image "docker.io/library/python:99.99.99-nonexistent": failed to resolve reference "docker.io/library/python:99.99.99-nonexistent": docker.io/library/python:99.99.99-nonexistent: not found`

	// The same failure, sampled twenty seconds later. The reason prefix
	// alternates on a cause that never changed.
	msgErrImagePull = `ErrImagePull: rpc error: code = NotFound desc = failed to pull and unpack image "docker.io/library/python:99.99.99-nonexistent": failed to resolve reference "docker.io/library/python:99.99.99-nonexistent": docker.io/library/python:99.99.99-nonexistent: not found`

	// Two pods reporting the identical reason with opposite prospects.
	msgUnschedulableCPU = `Unschedulable: 0/1 nodes are available: 1 Insufficient cpu. no new claims to deallocate, preemption: 0/1 nodes are available: 1 Preemption is not helpful for scheduling.`
	msgUnschedulablePVC = `Unschedulable: 0/1 nodes are available: pod has unbound immediate PersistentVolumeClaims. not found`

	msgOOMKilled      = `main: OOMKilled (exit code 137)`
	msgNonZeroExit    = `main: Error (exit code 1)`
	msgConfigError    = `CreateContainerConfigError: secret "does-not-exist" not found`
	msgInvalidImage   = `InvalidImageName: Failed to apply default image tag "THIS IS NOT A VALID IMAGE REFERENCE!!": couldn't parse image name "THIS IS NOT A VALID IMAGE REFERENCE!!": invalid reference format: repository name (library/THIS IS NOT A VALID IMAGE REFERENCE!!) must be lowercase`
	msgAdmissionQuota = `pods "repro-imagepull-5zflx" is forbidden: exceeded quota: no-pods, requested: pods=1, used: pods=0, limited: pods=0`
)

// Synthetic. Normal startup clears long before a capture runs, so unlike the
// messages above this one is not recorded engine output.
const msgPodInitializing = `PodInitializing`

func pending(message string) util.NodeStatus {
	return util.NodeStatus{Type: "Pod", State: "Pending", Message: message}
}

func failed(message string) util.NodeStatus {
	return util.NodeStatus{Type: "Pod", State: "Failed", Message: message}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		node util.NodeStatus
		want Classification
	}{
		{
			name: "image pull backoff is recoverable",
			node: pending(msgImagePullBackOff),
			want: Classification{Provisioning, "ImagePullBackOff", Deadline},
		},
		{
			name: "err image pull is the same failure under another name",
			node: pending(msgErrImagePull),
			want: Classification{Provisioning, "ErrImagePull", Deadline},
		},
		{
			name: "unschedulable on capacity is recoverable",
			node: pending(msgUnschedulableCPU),
			want: Classification{Provisioning, "Unschedulable", Deadline},
		},
		{
			name: "unschedulable on an unbound claim is not",
			node: pending(msgUnschedulablePVC),
			want: Classification{Provisioning, "Unschedulable", Immediate},
		},
		{
			name: "missing secret is recoverable",
			node: pending(msgConfigError),
			want: Classification{Provisioning, "CreateContainerConfigError", Deadline},
		},
		{
			name: "malformed image reference can never resolve",
			node: pending(msgInvalidImage),
			want: Classification{Provisioning, "InvalidImageName", Immediate},
		},
		{
			name: "admission rejection carries no reason constant",
			node: pending(msgAdmissionQuota),
			want: Classification{Provisioning, ReasonAdmissionRejected, Deadline},
		},
		{
			name: "terminated state names the container before the reason",
			node: failed(msgOOMKilled),
			want: Classification{Runtime, "OOMKilled", Immediate},
		},
		{
			name: "non-zero exit is a runtime failure",
			node: failed(msgNonZeroExit),
			want: Classification{Runtime, "Error", Immediate},
		},
		{
			name: "normal startup is not a failure",
			node: pending(msgPodInitializing),
			want: Classification{},
		},
		{
			name: "a healthy node carries no message",
			node: pending(""),
			want: Classification{},
		},
		{
			name: "parent nodes are skipped even when text is present",
			node: util.NodeStatus{Type: "Steps", State: "Running", Message: msgImagePullBackOff},
			want: Classification{},
		},
		{
			name: "a message on a succeeded node is not a live failure",
			node: util.NodeStatus{Type: "Pod", State: "Succeeded", Message: msgUnschedulableCPU},
			want: Classification{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.node)
			if got != tt.want {
				t.Errorf("Classify() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// One reason, two terminalities. Both pods report Unschedulable, the same pod
// condition and the same event reason, and neither has any container statuses,
// so nothing but the detail text separates them. A classifier keyed on the
// reason alone would have to give both the same answer, and one of them would
// be wrong.
func TestUnschedulableTerminalityComesFromTheDetail(t *testing.T) {
	capacity := Classify(pending(msgUnschedulableCPU))
	unbound := Classify(pending(msgUnschedulablePVC))

	if capacity.Reason != unbound.Reason {
		t.Fatalf("expected both to report the same reason, got %q and %q",
			capacity.Reason, unbound.Reason)
	}
	if capacity.Terminality != Deadline {
		t.Errorf("a capacity shortfall should be worth waiting for, got %v", capacity.Terminality)
	}
	if unbound.Terminality != Immediate {
		t.Errorf("an unbound claim should not be worth waiting for, got %v", unbound.Terminality)
	}
}

// The reason prefix alternates between ImagePullBackOff and ErrImagePull every
// twenty seconds or so on a failure that never changes. Anything keying a clock
// on the reason would reset it on every flip and never fire, so the two must
// agree on category and terminality even though the reason differs.
func TestReasonOscillationDoesNotChangeTheOutcome(t *testing.T) {
	backoff := Classify(pending(msgImagePullBackOff))
	errPull := Classify(pending(msgErrImagePull))

	if backoff.Reason == errPull.Reason {
		t.Fatalf("expected the reasons to differ, both were %q", backoff.Reason)
	}
	if backoff.Category != errPull.Category {
		t.Errorf("category changed across the flip: %q then %q", backoff.Category, errPull.Category)
	}
	if backoff.Terminality != errPull.Terminality {
		t.Errorf("terminality changed across the flip: %v then %v",
			backoff.Terminality, errPull.Terminality)
	}
}

// Only the waiting-state shape carries a reason as its prefix. A parser taking
// the token before the first colon would return "main" for a terminated
// container and a fragment of prose for an admission rejection.
func TestAllThreeMessageShapesAreReached(t *testing.T) {
	shapes := map[string]util.NodeStatus{
		"waiting":    pending(msgImagePullBackOff),
		"terminated": failed(msgOOMKilled),
		"admission":  pending(msgAdmissionQuota),
	}

	for shape, node := range shapes {
		got := Classify(node)
		if got.Reason == "" {
			t.Errorf("%s shape produced no reason", shape)
		}
		if got.Reason == "main" {
			t.Errorf("%s shape returned a container name as the reason", shape)
		}
	}
}
