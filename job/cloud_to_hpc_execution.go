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
	"path/filepath"
	"strconv"
	"strings"

	"github.com/lexis-project/yorc-ddi-plugin/ddi"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/tosca"
)

// CloudToHPCJobExecution holds Cloud staging area to HPC data transfer job Execution properties
type CloudToHPCJobExecution struct {
	*DDIJobExecution
}

// Execute executes a synchronous operation
func (e *CloudToHPCJobExecution) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(e.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating Job %q", e.NodeName)
		var locationName string
		locationName, err = e.setLocationFromAssociatedCloudAreaDirectoryProvider(ctx)
		if err != nil {
			return err
		}
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Location for %s is %s", e.NodeName, locationName)
		err = e.setCloudStagingAreaAccessDetails(ctx)
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting Job %q", e.NodeName)
		// Nothing to do here
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Submitting data transfer request %q", e.NodeName)
		err = e.submitDataTransferRequest(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to submit data transfer for node %q, error %s", e.NodeName, err.Error())

		}
	case tosca.RunnableCancelOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Canceling Job %q", e.NodeName)
		/*
			err = e.cancelJob(ctx)
			if err != nil {
				events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
					"Failed to cancel Job %q, error %s", e.NodeName, err.Error())

			}
		*/
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)

	default:
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)
	}

	return err
}

func (e *CloudToHPCJobExecution) submitDataTransferRequest(ctx context.Context) error {

	ddiClient, err := getDDIClient(ctx, e.Cfg, e.DeploymentID, e.NodeName)
	if err != nil {
		return err
	}

	sourcePath := e.GetValueFromEnvInputs(cloudStagingAreaDatasetPathEnvVar)
	if sourcePath == "" {
		return errors.Errorf("Failed to get path of dataset to transfer from Cloud staging area")
	}

	sourceSubDirPath := e.GetValueFromEnvInputs(sourceSubDirEnvVar)
	if sourceSubDirPath != "" {
		sourcePath = filepath.Join(sourcePath, sourceSubDirPath)
	}

	sourceFileName := e.GetValueFromEnvInputs(sourceFileNameEnvVar)
	if sourceFileName != "" {
		sourcePath = filepath.Join(sourcePath, sourceFileName)
	}

	destPath := e.GetValueFromEnvInputs(hpcDirectoryPathEnvVar)
	if destPath == "" {
		return errors.Errorf("Failed to get HPC directory path")
	}
	serverFQDN := e.GetValueFromEnvInputs(hpcServerEnvVar)
	if serverFQDN == "" {
		return errors.Errorf("Failed to get HPC server")
	}

	res := strings.SplitN(serverFQDN, ".", 2)
	targetSystem := res[0] + "_home"

	heappeJobIDStr := e.GetValueFromEnvInputs(heappeJobIDEnvVar)
	if heappeJobIDStr == "" {
		return errors.Errorf("Failed to get ID of associated job")
	}
	heappeJobID, err := strconv.ParseInt(heappeJobIDStr, 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "Unexpected Job ID value %q for deployment %s node %s",
			heappeJobIDStr, e.DeploymentID, e.NodeName)
		return err
	}

	heappeURL := e.GetValueFromEnvInputs(heappeURLEnvVar)
	if heappeURL == "" {
		return errors.Errorf("Failed to get HEAppE URL of job %d", heappeJobID)
	}

	taskID, taskDirPath, err := e.GetFirstCreatedTaskDetails(ctx, destPath)
	if err != nil {
		return err
	}

	var metadata ddi.Metadata

	token, err := e.AAIClient.GetAccessToken()
	if err != nil {
		return err
	}

	// Check encryption/compression settings
	encrypt := "no"
	compress := "no"

	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
		"Submitting data transfer request for %s source %s path %s, destination %s path %s, encrypt %s, compress %s, URL %s, job %d, task %d",
		e.NodeName, ddiClient.GetCloudStagingAreaName(), sourcePath, targetSystem, taskDirPath, encrypt, compress, heappeURL, heappeJobID, taskID)

	requestID, err := ddiClient.SubmiCloudToHPCDataTransfer(metadata, token, sourcePath, targetSystem, taskDirPath, encrypt, compress, heappeURL, heappeJobID, taskID)

	if err != nil {
		return err
	}

	// Store the request id
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		requestIDConsulAttribute, requestID)
	if err != nil {
		return errors.Wrapf(err, "Request %s submitted, but failed to store this request id", requestID)
	}

	return err
}
