# Yorc DDI plugin

<a href="https://doi.org/10.5281/zenodo.6080482"><img src="https://zenodo.org/badge/DOI/10.5281/zenodo.6080482.svg" alt="DOI"></a>

The Yorc DDI plugin implements a Yorc ([Ystia orchestrator](https://github.com/ystia/yorc/)) plugin as described in [Yorc documentation](https://yorc.readthedocs.io/en/latest/plugins.html), allowing the orchestrator to use 
[LEXIS DDI (Distributed Data Infrastructure)](https://lexis-project.eu/web/lexis-platform/data-management-layer/) API to manage asynchronous data transfers requests.

## Acknowledgement
This code repository is a result / contains results of the LEXIS project. The project has received funding from the European Union’s Horizon 2020 Research and Innovation programme (2014-2020) under grant agreement No. 825532.

## TOSCA components

This plugin provides the following TOSCA components defined in the TOSCA file [a4c/ddi-types-a4c.yaml](a4c/ddi-types-a4c.yaml)
that can be uploaded in [Alien4Cloud](https://alien4cloud.github.io/) catalog of TOSCA components:

### org.lexis.common.ddi.nodes.DDIToCloudJob
Job executing a transfer of dataset from DDI to Cloud staging area
### org.lexis.common.ddi.nodes.CloudToDDIJob
Job executing a transfer of dataset from Cloud staging area to DDI
### org.lexis.common.ddi.nodes.DeleteCloudDataJob
Job deleting a dataset from Cloud staging area
### org.lexis.common.ddi.nodes.DDIToHPCTaskJob
Job executing a transfer of dataset from DDI to HPC in a directory for a given
task in the job
### org.lexis.common.ddi.nodes.DDIRuntimeToHPCTaskJob
Job executing a transfer of dataset from DDI to HPC in a directory for a given
task in the job, the dataset path being computed at runtime
### org.lexis.common.ddi.nodes.HPCToDDIJob
Job executing a transfer of data from a HPC job directory to DDI
### org.lexis.common.ddi.nodes.CloudToHPCJob
Job executing a transfer of dataset from Cloud staging area to HPC in a directory for a given
task in the job
### org.lexis.common.ddi.nodes.HPCToCloudIJob
Job executing a transfer of dataset from a HPC job directory to Cloud Staging area
### org.lexis.common.ddi.nodes.StoreRunningHPCJobFilesToDDIJob
Job monitoring a HEApPE job and transferring new files produced by this HEAppE job
to DDI, until this HEAppE job ends
### org.lexis.common.ddi.nodes.StoreRunningHPCJobFilesToDDIGroupByDatasetJob
Job monitoring a HEApPE job and transferring new files produced by this HEAppE job
to DDI until this HEAppE job ends, and grouping these files in datasets according
to a pattern.
### org.lexis.common.ddi.nodes.WaitForDDIDatasetJob
Job waiting for a dataset to appear in DDI, and optionally waiting for files of
a given pattern to appear in this dataset
### org.lexis.common.ddi.nodes.DDIRuntimeFilesToCloudJob
Job executing a transfer of files in dataset from DDI to Cloud staging area, the dataset
being provided at runtime by an associated component
### org.lexis.common.ddi.nodes.DDIRuntimeFilesToHPCTaskJob
Job executing a transfer of files in a dataset from DDI to HPC in a directory for a given
task in the job
### org.lexis.common.ddi.nodes.GetDDIDatasetInfoJob
Job executing a request to get a DDI dataset info (size, number of files, number
of small files of size <= 32MB)
### org.lexis.common.ddi.nodes.ReplicateDatasetJob
Job executing requests to replicated a dataset to different sites
### org.lexis.common.ddi.nodes.GetComputeInstanceDatasetInfo
Component whose start operation provide info on a directory in a compute
instance (size, number of files, number of small files of size <= 32MB)
### org.lexis.common.ddi.nodes.GetHPCJobTaskDatasetInfo
Component whose start operation provide info on files produced by a HEAppE job
(size, number of files, number of small files of size <= 32MB)
### org.lexis.common.ddi.nodes.SSHFSMountStagingAreaDataset
SSHFS mount a dataset in the Cloud staging area on a compute instance directory.

## To build this plugin

You need first to have a working [Go environment](https://golang.org/doc/install).
Then to build, execute the following instructions:

```
mkdir -p $GOPATH/src/github.com/lexis-project
cd $GOPATH/src/github.com/lexis-project
git clone https://github.com/lexis-project/yorc-ddi-plugin
cd yorc-ddi-plugin
make
```

The plugin is then available at `bin/ddi-plugin`.

## Licensing

This plugin is licensed under the [Apache 2.0 License](LICENSE).
