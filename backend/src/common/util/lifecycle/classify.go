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

// Package lifecycle classifies pod lifecycle failures from the execution engine's
// node status into a category, a reason and a terminality. The message shapes it
// matches are observed behaviour, not an API contract.
package lifecycle

import (
	"strings"

	"github.com/kubeflow/pipelines/backend/src/common/util"
)

// Category is where in a pod's lifecycle the failure occurred.
type Category string

const (
	Provisioning Category = "Provisioning" // before the container runs
	Runtime      Category = "Runtime"      // container ran, then broke
	NodeLevel    Category = "NodeLevel"    // the machine went away
)

// Terminality is whether waiting can help.
type Terminality int

const (
	NotAFailure Terminality = iota // PodInitializing, ContainerCreating: normal startup
	Deadline                       // may recover; fail once past its limit
	Immediate                      // cannot recover; fail now
)

// Classification is the structured form of a lifecycle failure.
type Classification struct {
	Category    Category
	Reason      string
	Terminality Terminality
}

const nodeTypePod = "Pod"

const unschedulableUnbound = "unbound immediate PersistentVolumeClaims"

var admissionMarkers = []string{"is forbidden:", "exceeded quota:"}

// ReasonAdmissionRejected stands in for the reason constant an admission
// rejection does not carry.
const ReasonAdmissionRejected = "AdmissionRejected"

var startupReasons = map[string]bool{
	"PodInitializing":    true,
	"ContainerCreating":  true,
	"ContainerCreated":   true,
	"PodScheduled":       true,
	"Initialized":        true,
	"ContainersNotReady": true,
}

// Terminality here is the default; see terminalityFor for the exceptions.
var knownReasons = map[string]Classification{
	"ImagePullBackOff":           {Category: Provisioning, Terminality: Deadline},
	"ErrImagePull":               {Category: Provisioning, Terminality: Deadline},
	"CreateContainerConfigError": {Category: Provisioning, Terminality: Deadline},
	"CreateContainerError":       {Category: Provisioning, Terminality: Deadline},
	"RunContainerError":          {Category: Provisioning, Terminality: Deadline},
	"Unschedulable":              {Category: Provisioning, Terminality: Deadline},
	"InvalidImageName":           {Category: Provisioning, Terminality: Immediate},
	"OOMKilled":                  {Category: Runtime, Terminality: Immediate},
	"Error":                      {Category: Runtime, Terminality: Immediate},
	"NodeLost":                   {Category: NodeLevel, Terminality: Immediate},
	"Preempted":                  {Category: NodeLevel, Terminality: Immediate},
	"DisruptionTarget":           {Category: NodeLevel, Terminality: Immediate},
	ReasonAdmissionRejected:      {Category: Provisioning, Terminality: Deadline},
}

var resolvedStates = map[string]bool{
	"Succeeded": true,
	"Skipped":   true,
	"Omitted":   true,
}

// Classify reports the lifecycle condition described by a node status.
// The zero value, with Terminality NotAFailure, means nothing is wrong.
// Pure: no I/O, no clock, no Kubernetes client.
func Classify(node util.NodeStatus) Classification {
	// Parent nodes aggregate their children's text; only pod nodes carry their own.
	if node.Type != "" && node.Type != nodeTypePod {
		return Classification{}
	}
	if resolvedStates[node.State] {
		return Classification{}
	}

	message := strings.TrimSpace(node.Message)
	if message == "" {
		return Classification{}
	}

	reason, detail := parse(message)
	if reason == "" || startupReasons[reason] {
		return Classification{}
	}

	known, ok := knownReasons[reason]
	if !ok {
		// An unrecognised reason still gets a classification so the run is
		// bounded. The category is left empty and the default limit applies.
		return Classification{Reason: reason, Terminality: Deadline}
	}

	return Classification{
		Category:    known.Category,
		Reason:      reason,
		Terminality: terminalityFor(reason, detail, known.Terminality),
	}
}

// parse extracts a reason and its detail text from a node message.
//
// Three shapes occur, and only the first carries a reason as its prefix:
//
//	waiting-state     <Reason>: <detail>              ImagePullBackOff: Back-off pulling image "…"
//	terminated-state  <container>: <Reason> (exit N)  main: OOMKilled (exit code 137)
//	admission         pods "<name>" is forbidden: …   pods "x" is forbidden: exceeded quota: …
//
// A parser that always took the token before the first colon would return the
// container name for the second shape and a fragment of prose for the third.
func parse(message string) (reason string, detail string) {
	for _, marker := range admissionMarkers {
		if strings.Contains(message, marker) {
			return ReasonAdmissionRejected, message
		}
	}

	head, tail, found := strings.Cut(message, ":")
	if !found {
		// A bare reason with no detail, for example "PodInitializing".
		return strings.TrimSpace(message), ""
	}

	head = strings.TrimSpace(head)
	tail = strings.TrimSpace(tail)

	// Waiting-state shape: the prefix is itself a reason.
	if _, ok := knownReasons[head]; ok {
		return head, tail
	}
	if startupReasons[head] {
		return head, tail
	}

	// Terminated-state shape: the prefix was a container name, and the reason
	// is the first token of what follows.
	if candidate, _, _ := strings.Cut(tail, " "); candidate != "" {
		if _, ok := knownReasons[candidate]; ok {
			return candidate, tail
		}
	}

	// Unrecognised, but the prefix is the engine's best guess at a reason.
	return head, tail
}

// terminalityFor resolves terminality from the reason and its detail text.
// Where the detail adds nothing, the reason decides.
func terminalityFor(reason, detail string, fallback Terminality) Terminality {
	if reason == "Unschedulable" && strings.Contains(detail, unschedulableUnbound) {
		// A claim naming a storage class that does not exist reports the same
		// reason as a pod merely waiting for capacity, but nothing plausible
		// will fix it. The detail text is the only discriminator.
		return Immediate
	}
	return fallback
}
