from azureml.core import Workspace, Environment
from azureml.core.webservice import AciWebservice, Webservice, LocalWebservice
from azureml.core.model import Model, InferenceConfig

ws = Workspace.from_config()

# Get Model
model = Model(ws, "titanic_model")

# Create inference config
entry_script = "model_deployments/entry_scripts/titanic_entry.py"
env_name = "AzureML-sklearn-1.0-ubuntu20.04-py38-cpu"
env = Environment.get(ws, env_name)
inference_config = InferenceConfig(entry_script, environment=env)

# Create deployment config
deployment_config = AciWebservice.deploy_configuration(cpu_cores = 1, memory_gb = 1)

# Deploy model
service = Model.deploy(
    workspace=ws, 
    name="titanic-aci-model", 
    models=[model], 
    inference_config=inference_config, 
    deployment_config=deployment_config,
    overwrite=True)

service.wait_for_deployment(show_output = True)
print(service.state)