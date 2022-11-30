import os

path_to_pipeline_deployment = "pipelines/pipeline_deployment/pipeline_deployment.py"
path_to_config = "pipelines/train_pipeline/pipeline_config.json"

if __name__ == '__main__':
    os.system(f"python {path_to_pipeline_deployment} --config_path {path_to_config}")