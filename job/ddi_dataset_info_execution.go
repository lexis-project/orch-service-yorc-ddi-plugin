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
	"path"
	"strings"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/tosca"
)

// DDIDatasetInfoExecution holds DDI dataset info job Execution properties
type DDIDatasetInfoExecution struct {
	*DDIJobExecution
}

// Execute executes a synchronous operation
func (e *DDIDatasetInfoExecution) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(e.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating Job %q", e.NodeName)
		// Nothing to do here
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting Job %q", e.NodeName)
		// Nothing to do here
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Submitting data transfer request %q", e.NodeName)
		err = e.submitDatasetInfoRequest(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to submit dataset info request for node %q, error %s", e.NodeName, err.Error())

		}
	case tosca.RunnableCancelOperationName:
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)
	default:
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)
	}

	return err
}

func (e *DDIDatasetInfoExecution) submitDatasetInfoRequest(ctx context.Context) error {

	ddiClient, locationName, err := getDDIClientAlive(ctx, e.Cfg, e.DeploymentID, e.NodeName)
	if err != nil {
		return err
	}

	// Store the location of this DDI client in node metadata to re-use the same client
	// when monitoring the request
	err = setNodeMetadataLocation(ctx, e.Cfg, e.DeploymentID, e.NodeName, locationName)
	if err != nil {
		return err
	}

	datasetPath := e.GetValueFromEnvInputs(ddiDatasetPathEnvVar)
	if datasetPath == "" {
		return errors.Errorf("Failed to get path of dataset for which to get info")
	}

	// The input path could be the path of a file in a dataset
	splitPath := strings.SplitN(datasetPath, "/", 4)
	if len(splitPath) > 3 {
		datasetPath = path.Join(splitPath[0], splitPath[1], splitPath[2])
	}

	// Get locations where this dataset is available
	ddiAreaNames, err := e.getAreasForDDIDataset(ctx, ddiClient, datasetPath, "")
	if err != nil {
		return err
	}

	if len(ddiAreaNames) == 0 {
		return errors.Errorf("Found no DDI area having dataset path %s", datasetPath)
	}

	token, err := e.AAIClient.GetAccessToken()
	if err != nil {
		return err
	}

	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
		"Submitting data info request for %s source %s path %s",
		e.NodeName, ddiAreaNames[0], datasetPath)

	requestID, err := ddiClient.SubmitDDIDatasetInfoRequest(token, ddiAreaNames[0], datasetPath)
	if err != nil {
		return err
	}

	// Store the request id
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		requestIDConsulAttribute, requestID)
	if err != nil {
		return errors.Wrapf(err, "Request %s submitted, but failed to store this request id", requestID)
	}

	// Store the ddi Areas
	err = e.SetDatasetInfoCapabilityLocationsAttribute(ctx, ddiAreaNames)
	if err != nil {
		return err
	}

	return err
}
