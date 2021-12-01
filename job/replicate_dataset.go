// Copyright 2020 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package job

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/tosca"
)

const (
	replicationSitesEnvVar         = "REPLICATION_SITES"
	requestIDStatusConsulAttribute = "requestid_status"
)

// ReplicateDatasetExecution holds HPC to DDI data transfer job Execution properties
type ReplicateDatasetExecution struct {
	*DDIJobExecution
}

// Execute executes a synchronous operation
func (r *ReplicateDatasetExecution) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(r.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
			"Creating Job %q", r.NodeName)
		var locationName string
		locationName, err = r.SetLocationFromAssociatedDataTransferJob(ctx)
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
			"Location for %s is %s", r.NodeName, locationName)
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
			"Deleting Job %q", r.NodeName)
		// Nothing to do here
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
			"Submitting replication request for node %q", r.NodeName)
		err = r.submitReplicationRequest(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
				"Failed to submit replication request for node %q, error %s", r.NodeName, err.Error())

		}
	case tosca.RunnableCancelOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
			"Canceling Job %q", r.NodeName)
		/*
			err = r.cancelJob(ctx)
			if err != nil {
				events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
					"Failed to cancel Job %q, error %s", r.NodeName, err.Error())

			}
		*/
		err = errors.Errorf("Unsupported operation %q", r.Operation.Name)

	default:
		err = errors.Errorf("Unsupported operation %q", r.Operation.Name)
	}

	return err
}

func (r *ReplicateDatasetExecution) submitReplicationRequest(ctx context.Context) error {

	requestIDStatus := make(map[string]string)

	// Store a dummy request id needed by common code managing jobs
	_ = deployments.SetAttributeForAllInstances(ctx, r.DeploymentID, r.NodeName,
		requestIDConsulAttribute, "replicationRequest")

	// Get list of requested sites where the dataset should be available
	requestedSitesStr := r.GetValueFromEnvInputs(replicationSitesEnvVar)
	var requestedSites []string
	if requestedSitesStr != "" {
		err := json.Unmarshal([]byte(requestedSitesStr), &requestedSites)
		if err != nil {
			return errors.Wrapf(err, "Wrong format for files patterns %s for node %s", requestedSitesStr, r.NodeName)
		}
	}

	if len(requestedSites) == 0 {
		// No replication to perform
		err := deployments.SetAttributeComplexForAllInstances(ctx, r.DeploymentID, r.NodeName,
			requestIDStatusConsulAttribute, requestIDStatus)
		return err
	}

	ddiClient, err := getDDIClient(ctx, r.Cfg, r.DeploymentID, r.NodeName)
	if err != nil {
		return err
	}

	fullPath := r.GetValueFromEnvInputs(ddiDatasetPathEnvVar)
	if fullPath == "" {
		return errors.Errorf("Failed to get path of dataset for which to get info")
	}

	datasetPath := fullPath
	if strings.HasPrefix(datasetPath, "/") {
		// This is an absolute path of the form /zone/project/projectID/datasetID
		// Remocing the zone
		splitPath := strings.SplitN(datasetPath, "/", 3)
		if len(splitPath) == 3 {
			datasetPath = splitPath[2]
		}
	}

	// The input path could be the path of a file in a dataset
	splitPath := strings.SplitN(datasetPath, "/", 4)
	if len(splitPath) > 3 {
		datasetPath = path.Join(splitPath[0], splitPath[1], splitPath[2])
	}

	// Get locations where this dataset is available
	ddiAreaNames, err := r.getAreasForDDIDataset(ctx, ddiClient, datasetPath, "")
	if err != nil {
		return err
	}

	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
		"Dataset %s is available at: %v", datasetPath, ddiAreaNames)

	if len(ddiAreaNames) == 0 {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, r.DeploymentID).Registerf(
			"Failed to find a DDI location where dataset %s is available", datasetPath)
		err = errors.Errorf("Found no DDI location for dataset %s", datasetPath)
		return err
	}

	var missingSites []string
	for _, requestedSite := range requestedSites {
		found := false
		for _, ddiAreaName := range ddiAreaNames {
			if strings.HasPrefix(ddiAreaName, requestedSite+"_") {
				found = true
				break
			}
		}
		if !found {
			missingSites = append(missingSites, requestedSite)
		}
	}

	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
		"Sites where %s must do the replication: %v", r.NodeName, missingSites)

	if len(missingSites) == 0 {
		// No replication request, nothing to submit
		err = deployments.SetAttributeComplexForAllInstances(ctx, r.DeploymentID, r.NodeName,
			requestIDStatusConsulAttribute, requestIDStatus)
		return err
	}

	allDDIAreaNames, err := r.getDDIAreaNames(ctx)
	if err != nil {
		return err
	}

	// Find DDI areas where the dataset should be replicated
	var neededDDIAreas []string
	for _, missingSite := range missingSites {
		for _, areaName := range allDDIAreaNames {
			if strings.HasPrefix(areaName, missingSite+"_") {
				neededDDIAreas = append(neededDDIAreas, areaName)
				break
			}
		}
	}

	token, err := r.AAIClient.GetAccessToken()
	if err != nil {
		return err
	}

	// Submit replication requests for missing DDI areas
	var submitErr error
	for _, areaName := range neededDDIAreas {

		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
			"Submitting replication from %s to %s of dataset %s", ddiAreaNames[0], areaName, datasetPath)
		requestID, err := ddiClient.SubmitDDIReplicationRequest(token, ddiAreaNames[0],
			datasetPath, areaName)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, r.DeploymentID).RegisterAsString(
				fmt.Sprintf("Failed to submit replication of %s to %s, error %s", datasetPath, areaName, err.Error()))
			// Continuing with replications to other areas if any
			submitErr = err
		} else {
			requestIDStatus[requestID] = requestStatusPending
		}
	}

	// Store the request ids
	err = deployments.SetAttributeComplexForAllInstances(ctx, r.DeploymentID, r.NodeName,
		requestIDStatusConsulAttribute, requestIDStatus)
	if err != nil {
		err = errors.Wrapf(err, "Requests submitted, but failed to store the request ID - status map %v", requestIDStatus)
	}

	if submitErr != nil && len(requestIDStatus) == 0 {
		// No submit success, returning an error
		err = submitErr
	}
	return err
}
