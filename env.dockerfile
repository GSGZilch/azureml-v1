FROM mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:20221010.v1

ENV AZUREML_CONDA_ENVIRONMENT_PATH /azureml-envs/sklearn-1.0
# Create conda environment
RUN conda create -p $AZUREML_CONDA_ENVIRONMENT_PATH \
    python=3.8 pip=21.3.1 -c anaconda -c conda-forge

# Prepend path to AzureML conda environment
ENV PATH $AZUREML_CONDA_ENVIRONMENT_PATH/bin:$PATH

# Install pip dependencies
RUN pip install 'matplotlib~=3.5.0' \
                'psutil~=5.8.0' \
                'tqdm~=4.62.0' \
                'pandas~=1.3.0' \
                'scipy~=1.7.0' \
                'numpy~=1.21.0' \
                'ipykernel~=6.0' \
                'azureml-core==1.47.0' \
                'azureml-defaults==1.47.0' \
                'azureml-mlflow==1.47.0' \
                'azureml-telemetry==1.47.0' \
                'scikit-learn~=1.0.0' \
                'debugpy~=1.6.3'

# This is needed for mpi to locate libpython
ENV LD_LIBRARY_PATH $AZUREML_CONDA_ENVIRONMENT_PATH/lib:$LD_LIBRARY_PATH
