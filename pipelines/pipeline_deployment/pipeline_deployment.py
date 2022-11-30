from venv import create
from azureml.core.environment import Environment
from azureml.core.runconfig import RunConfiguration, DockerConfiguration
from azureml.core import Workspace, Experiment, Dataset
from azureml.core.conda_dependencies import CondaDependencies
from azureml.core.compute import AmlCompute, ComputeTarget
from azureml.core.authentication import ServicePrincipalAuthentication, MsiAuthentication
from azureml.data.file_dataset import FileDataset
from azureml.exceptions import ComputeTargetException
from azureml.pipeline.core.run import PipelineRun
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.core import Pipeline, PipelineEndpoint
from azureml.pipeline.core.graph import PipelineParameter
from azureml.pipeline.core import StepSequence
from typing import Optional, List, Tuple, Dict, Set, Any, Union
from datetime import datetime
import argparse
import json
import os


parser = argparse.ArgumentParser()
parser.add_argument('--config_path', dest='config_path', required=True)
args = parser.parse_args()

with open(args.config_path) as f:
    CONFIG: Dict[str, Any] = json.load(f)


def connect_to_aml_ws(config):
    ## WORKSPACE AUTHENTICATION
    assert config["WORKSPACE_AUTH"] in ["from_config", "interactive", "service_principal", "managed_identity"]

    if config["WORKSPACE_AUTH"] == "from_config":
        # when running on azureml compute instance
        ws = Workspace.from_config()
        print(f"[{datetime.now()}] Authentication from config successful.")

    elif config["WORKSPACE_AUTH"] == "interactive":
        ws = Workspace(
            subscription_id=os.environ["SUBSCRIPTION_ID"],
            resource_group=os.environ["RESOURCE_GROUP_NAME"],
            workspace_name=os.environ["WORKSPACE_NAME"])

    elif config["WORKSPACE_AUTH"] == "service_principal":
        sp = ServicePrincipalAuthentication(
            tenant_id=os.environ["TENANT_ID"], 
            service_principal_id=os.environ["SP_CLIENT_ID"], 
            service_principal_password=os.environ["SP_SECRET"])
        ws = Workspace(
            subscription_id=os.environ["SUBSCRIPTION_ID"],
            resource_group=os.environ["RESOURCE_GROUP_NAME"],
            workspace_name=os.environ["WORKSPACE_NAME"],
            auth=sp)
        print(f"[{datetime.now()}] Service Principal Authentication successful.")
        
    elif config["WORKSPACE_AUTH"] == "managed_identity":
        # when running on Azure VM
        msi_auth = MsiAuthentication()
        ws = Workspace(
            subscription_id=os.environ["SUBSCRIPTION_ID"],
            resource_group=os.environ["RESOURCE_GROUP_NAME"],
            workspace_name=os.environ["WORKSPACE_NAME"],
            auth=msi_auth)
        print(f"[{datetime.now()}] Managed Identity Authentication successful.")

    return ws


def add_datetime_as_param(pipeline_params):
    ## ADD DATETIME ARGUMENT FOR EACH STEP
    dt: datetime = datetime.now()
    run_datetime: str = f"{dt.year}{dt.month:02d}{dt.day:02d}_{dt.hour:02d}{dt.minute:02d}{dt.second:02d}"

    # datetime string is passed to each step for dynamic paths
    # format: yyyymmdd_HHMMSS
    for step in pipeline_params:
        step['run_datetime'] = run_datetime

    return pipeline_params


def get_compute_targets(ws, config, compute_configs):
    ## COMPUTE TARGET
    compute_targets: Dict[str, ComputeTarget] = {}
    vm_sizes: Dict[str, str] = {"cpu": "STANDARD_DS3_V2", "gpu": "STANDARD_NC6"}
    required_computes: List[str] = [step["COMPUTE"] for step in config["PIPELINE_STEPS"]]
    for compute_type, compute_config in compute_configs.items():
        for compute_name, num_nodes in compute_config.items():
            try:
                compute_targets[compute_name] = ComputeTarget(workspace=ws, name=compute_name)
            except ComputeTargetException:
                if compute_name in required_computes:            
                    aml_compute_config = AmlCompute.provisioning_configuration(vm_size=vm_sizes[compute_type],
                                                vm_priority="lowpriority", 
                                                min_nodes=num_nodes["min"],
                                                max_nodes=num_nodes["max"])
                    compute_targets[compute_name] = ComputeTarget.create(workspace=ws, name=compute_name, provisioning_configuration=aml_compute_config)
                    
    for compute_config in compute_configs.values():
        for compute_name in compute_config:
            compute_targets[compute_name].wait_for_completion(show_output=False, min_node_count=None, timeout_in_minutes=180)
            print(f"[{datetime.now()}] Compute target ready: {compute_name}")

    print(f"[{datetime.now()}] Finished compute target setup.")

    return compute_targets


def create_pipeline_arguments(pipeline_params):
    ## CREATE PIPELINE ARGUMENTS
    pipeline_args: List[List[str, PipelineParameter]] = []
    for step in pipeline_params:
        pipeline_args_per_step: List[str, PipelineParameter] = []
        for key in step:
            pipeline_args_per_step.append(f"--{key}")
            pipeline_args_per_step.append(PipelineParameter(
                name=key,
                default_value=step[key]
            ))
        pipeline_args.append(pipeline_args_per_step)
    return pipeline_args


def create_run_config(env):
    ## ASSIGN COMPUTE TARGET AND/OR ENVIRONMENT
    aml_run_config = RunConfiguration()
    if "DOCKER_IMAGE" in CONFIG:
        docker_config = DockerConfiguration(use_docker=True)
        aml_run_config.docker=docker_config
    aml_run_config.environment = env

    return aml_run_config


def main(CONFIG):
    
    ws = connect_to_aml_ws(CONFIG)

    ## UNPACK CONFIG
    experiment_name: str = CONFIG["EXPERIMENT_NAME"]

    script_names: List[str] = [step["SCRIPT"] for step in CONFIG["PIPELINE_STEPS"]]

    run_with_previous_script: List[bool] = [step["RUN_WITH_PREVIOUS"] for step in CONFIG["PIPELINE_STEPS"]]
    
    source_dir_names: List[str] = [step["SOURCE_DIR"] for step in CONFIG["PIPELINE_STEPS"]]
    source_directories: List[str] = [f"{CONFIG['SOURCE_DIR_PREFIX']}/{src_dir}" for src_dir in source_dir_names]
    
    step_targets: List[str] = [step["COMPUTE"] for step in CONFIG["PIPELINE_STEPS"]]
    
    input_dataset_dicts: List[Dict[str, str]] = [step["INPUT_DATASETS"] for step in CONFIG["PIPELINE_STEPS"]]
    
    pipeline_name: str = CONFIG["PIPELINE_NAME"]
    pipeline_description: str = CONFIG["PIPELINE_DESCRIPTION"]
    pipeline_params: List[Dict[str, str]] = [step["PARAMS"] for step in CONFIG["PIPELINE_STEPS"]]
    
    endpoint_name: str = CONFIG["ENDPOINT_NAME"]
    endpoint_description: str = CONFIG["ENDPOINT_DESCRIPTION"]

    compute_configs: Dict[str, Dict[str, Dict[str, int]]] = {
        "cpu": CONFIG["CPU_CLUSTERS"],
        "gpu": CONFIG["GPU_CLUSTERS"]
    }

    pipeline_params = add_datetime_as_param(pipeline_params)

    ## ENVIRONMENT
    if "DOCKER_IMAGE" in CONFIG:
        env: Environment = Environment(CONFIG["ENV_NAME"])
        env.docker.base_image = CONFIG["DOCKER_IMAGE"]
        env.docker.base_image_registry.address = CONFIG["DOCKER_REGISTRY"]
        if os.environ.get("DOCKER_USERNAME") is not None:
            env.docker.base_image_registry.username = os.environ["DOCKER_USERNAME"]
            env.docker.base_image_registry.password = os.environ["DOCKER_PASSWORD"]
        env.python.user_managed_dependencies = True
    else:
        if "CURATED_ENV" in CONFIG:
            cur_env_name: str = CONFIG["CURATED_ENV"]
            cur_env: Environment = Environment.get(ws, cur_env_name)
            env: Environment = cur_env.clone(CONFIG["ENV_NAME"])
        else:
            env = Environment(CONFIG["ENV_NAME"])

        conda_dep = CondaDependencies()
        conda_dep.add_pip_package("azureml-defaults")
        conda_dep.add_pip_package("azureml-core")
        conda_dep.add_pip_package("azureml-dataprep[fuse]")
        if os.path.isfile('deployment/pipeline_requirements.txt'):
            with open(f"deployment/pipeline_requirements.txt", 'r') as f:
                lines: List[str] = f.readlines()
            for line in lines:
                conda_dep.add_pip_package(line.split("\n")[0])
        env.python.conda_dependencies = conda_dep
    print(f"[{datetime.now()}] Finished environment setup.")

    compute_targets = get_compute_targets(ws, CONFIG, compute_configs)

    pipeline_args = create_pipeline_arguments(pipeline_params)

    aml_run_config = create_run_config(env)

    # CREATE PIPELINE STEPS
    pipeline_steps: List[PythonScriptStep] = []
    parallel_runs_count = 0
    for step_nr, step in enumerate(CONFIG["PIPELINE_STEPS"]):
        # tabular datasets are not mounted
        input_datasets: List[FileDataset] = [Dataset.get_by_name(ws, name=key).as_named_input(value).as_mount(path_on_compute=value) 
            for (key, value) in input_dataset_dicts[step_nr].items()]
    
        script_step = PythonScriptStep(
        script_name=script_names[step_nr],
        source_directory=source_directories[step_nr],
        inputs=input_datasets,
        arguments=pipeline_args[step_nr],
        compute_target=compute_targets[step_targets[step_nr]],
        runconfig=aml_run_config,
        allow_reuse=False)

        if run_with_previous_script[step_nr]:
            parallel_runs_count += 1
            if isinstance(pipeline_steps[step_nr - parallel_runs_count], list):
                pipeline_steps[step_nr - parallel_runs_count].append(script_step)
            else:
                pipeline_steps[step_nr - parallel_runs_count] = [pipeline_steps[step_nr - parallel_runs_count], script_step]
        else:
            pipeline_steps.append(script_step)
        print(f"[{datetime.now()}] Created step: {step['NAME']}")


    ## CREATE PIPELINE 
    pipeline = Pipeline(workspace=ws, steps=StepSequence(pipeline_steps))

    published_pipeline: Any = pipeline.publish(
        name=pipeline_name,
        description=pipeline_description,
        version="1.0"
    )
    print(f"[{datetime.now()}] Pipeline published.")


    ## DEPLOY PIPELINE
    if CONFIG["DEPLOY_PIPELINE_ENDPOINT"]:
        # disable and archive the published endpoint
        endpoints_available: List[PipelineEndpoint] = PipelineEndpoint.list(ws, active_only=False)
        for i in endpoints_available:
            if i.name == endpoint_name:
                i.disable()
                i.archive()
                print(f"[{datetime.now()}] Archived previous endpoint.")

        # publish the endpoint with the published pipeline
        pipeline_endpoint = PipelineEndpoint.publish(workspace=ws,
                                                    name=endpoint_name,
                                                    pipeline=published_pipeline,
                                                    description=endpoint_description)
        print(f"[{datetime.now()}] Pipeline endpoint published.")

        if CONFIG["RUN_INSTANTLY"]:
            # start pipeline run after deploying endpoint
            pipeline_params_dict = {}

            pipeline_endpoint_by_name = PipelineEndpoint.get(workspace=ws, name=endpoint_name)
            print(f"[{datetime.now()}] Submitting run to {endpoint_name}...")

            run_id: PipelineRun = pipeline_endpoint_by_name.submit(
                experiment_name=experiment_name,
                pipeline_parameters=pipeline_params_dict)
            print(f"[{datetime.now()}] Run submitted successfully.")
    else:
        # start pipeline run without deploying endpoint
        exp = Experiment(ws, experiment_name)
        active_run: PipelineRun = exp.submit(published_pipeline)
        print(f"[{datetime.now()}] Run submitted successfully.")


if __name__ == '__main__':
    main(CONFIG)