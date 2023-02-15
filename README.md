# Azure Machine Learning

This repo covers Azure Machine Learning, using the v1 Python SDK.

## Usage

Some of the code uses the .from_config() authentication method for the Workspace object in the AzureML SDK, for simplicity reasons.
In order for this to work, download the config.json from your AzureML workspace and place it in the root folder of your project.
Some of the other code, like the pipeline deployment template, requires this info to be set as environment variables.

The datastore and pipeline config will need to be reconfigured to match your specific storage service names and pipeline steps respectively.

## Local

The data, notebooks and models folder are meant for local-only development/prototyping.

## Remote

The datastore deployment, pipelines and model deployment are integrated with AzureML Python SDK to orchestrate datastore/dataset setup, machine learning pipelines and model deployments (ACI deployments) respectively.

## Misc

The requirements.txt file holds all dependencies required to run the code. If the local notebooks are not used, scikit-learn can be removed from this file.
The env.dockerfile is currently unused and holds the configuration for the curated AzureML environment that was used in the pipelines. 

For any further questions, reach out to olivier.mertens@microsoft.com
