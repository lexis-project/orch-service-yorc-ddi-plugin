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

package main

import (
	"github.com/lexis-project/yorc-ddi-plugin/job"

	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/plugin"
	"github.com/ystia/yorc/v4/prov"
)

func main() {
	// Create configuration that defines the type of plugins to be served.
	// In servConfig can be set :
	// - TOSCA definitions for an extended Yorc
	// - A DelegateExecutor for some TOSCA component types
	// - An OperationExecutor for some TOSCA artifacts types
	// - An InfrastructureUsageCollector for specific infrastructures to be monitored
	servConfig := new(plugin.ServeOpts)

	// Add TOSCA Definitions contained in the def variable.
	// These defintions are provided in a yaml file ddi-types.yaml
	// bundled in this binary (see Makefile)
	// The ddi-types.yaml key can be then by used (imported) by applications
	// deployed to the extended Yorc, as the example tosca/topology.yaml is doing
	var err error
	servConfig.Definitions, err = getToscaResources()
	if err != nil {
		log.Printf("Error getting bundle TOSCA resources: %+v\n", err)
		return
	}

	// Set DelegateFunc that implements a DelegateExecutor for the TOSCA component types specified in DelegateSupportedTypes
	// The delegateExecutor is defined in delegate.go
	servConfig.DelegateSupportedTypes = []string{`org\.ddi\.nodes\..*`}
	servConfig.DelegateFunc = func() prov.DelegateExecutor {
		return new(delegateExecutor)
	}

	// Set OperationFunc that implements an OperationExecutor for the TOSCA artifacts specified in OperationSupportedArtifactTypes
	// Temporarily using an artifact existing in Alien4Cloud until the A4C yorc provider
	// is able to be extended to support new artifact types
	servConfig.OperationSupportedArtifactTypes = []string{"yorc.artifacts.Deployment.SlurmJob"}
	servConfig.OperationFunc = func() prov.OperationExecutor {
		return new(operationExecutor)
	}

	// Set ActionFunc that implements an ActionOperator for DDI jobs
	servConfig.ActionTypes = []string{
		job.EnableCloudAccessAction,
		job.DisableCloudAccessAction,
		job.DataTransferAction,
		job.CloudDataDeleteAction,
		job.ReplicateDatasetAction,
		job.GetDDIDatasetInfoAction,
		job.WaitForDatasetAction,
		job.StoreRunningHPCJobFilesToDDIAction,
		job.StoreRunningHPCJobFilesGroupByDatasetAction}
	servConfig.ActionFunc = func() prov.ActionOperator {
		return new(job.ActionOperator)
	}

	plugin.Serve(servConfig)
}
