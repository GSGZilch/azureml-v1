from azureml import data
from azureml.core import Workspace, Datastore, Dataset, Keyvault
from azureml.core.authentication import ServicePrincipalAuthentication
from typing import Optional, List, Tuple, Dict, Set, Any, Union
from azureml.data.azure_data_lake_datastore import AzureDataLakeGen2Datastore
from azureml.data.azure_sql_database_datastore import AzureSqlDatabaseDatastore
from azureml.data.azure_storage_datastore import AzureBlobDatastore
from azureml.exceptions import UserErrorException

from azureml.data import FileDataset, TabularDataset
from datastore_config import *
from datetime import datetime as dt
import os


def main():
    # AzureML Workspace
    ws = Workspace(
            subscription_id=os.environ["SUBSCRIPTION_ID"],
            resource_group=os.environ["RESOURCE_GROUP_NAME"],
            workspace_name=os.environ["WORKSPACE_NAME"],
        )

    print(f"[{dt.now()}] Interactive authentication successful.")

    # Keyvault linked to AzureML Workspace
    kv = ws.get_default_keyvault()
    
    DATASTORE_TYPES = ["BLOB", "ADLS2", "SQL"]
    DATASET_TYPES = ["file", "tabular"]

    # datastore assertions
    for datastore_dict in DATASTORE_CONFIG:
        # assert that datastore types are filled in correctly
        assert datastore_dict["TYPE"] in DATASTORE_TYPES

        if datastore_dict["TYPE"] == "BLOB":
            assert "ACCOUNT_KEY_SECRET" in datastore_dict["AUTH"]
        elif datastore_dict["TYPE"] == "ADLS2":
            for secret in ["TENANT_ID_SECRET", "SP_CLIENT_ID_SECRET", "SP_SECRET_SECRET"]:
                assert secret in datastore_dict["AUTH"]
        elif datastore_dict["TYPE"] == "SQL":
            for secret in ["SERVER", "DATABASE", "USERNAME", "PASSWORD_SECRET"]:
                assert secret in datastore_dict["AUTH"]

        # dataset assertions
        for dataset_dict in datastore_dict["DATASETS"].values():
            # assert that dataset types are filled in correctly
            assert dataset_dict["TYPE"] in DATASET_TYPES

            # assert that sql datasets use queries, other datasets use paths
            if datastore_dict["TYPE"] == "SQL":
                assert "QUERY" in dataset_dict
            else:
                assert "PATH" in dataset_dict

    # get/register all datastores and datasets
    for datastore_dict in DATASTORE_CONFIG:
        # try to get datastore
        try:
            datastore = Datastore.get(ws, datastore_name=datastore_dict["DATASTORE_NAME"])
            print(f"[{dt.now()}] Found {datastore_dict['TYPE']} Datastore '{datastore_dict['DATASTORE_NAME']}' in the workspace.")
        except UserErrorException:
            # create and register datastore
            auth_dict: Dict[str, str] = datastore_dict["AUTH"]

            if datastore_dict["TYPE"] == "BLOB":
                datastore: AzureBlobDatastore = Datastore.register_azure_blob_container(
                    workspace=ws,
                    datastore_name=datastore_dict["DATASTORE_NAME"],
                    account_name=datastore_dict["STORAGE_NAME"], 
                    container_name=datastore_dict["CONTAINER"], 
                    account_key=kv.get_secret(name=auth_dict["ACCOUNT_KEY_SECRET"])
                )
            elif datastore_dict["TYPE"] == "ADLS2":
                datastore: AzureDataLakeGen2Datastore = Datastore.register_azure_data_lake_gen2(
                    workspace=ws,
                    datastore_name=datastore_dict["DATASTORE_NAME"],
                    #subscription_id=os.environ["SUBSCRIPTION_ID"],
                    #resource_group=os.environ["RESOURCE_GROUP_NAME"], 
                    account_name=datastore_dict["STORAGE_NAME"],
                    filesystem=datastore_dict["CONTAINER"], 
                    tenant_id=kv.get_secret(name=auth_dict["TENANT_ID_SECRET"]), 
                    client_id=kv.get_secret(name=auth_dict["SP_CLIENT_ID_SECRET"]), 
                    client_secret=kv.get_secret(name=auth_dict["SP_SECRET_SECRET"])
                )
            elif datastore_dict["TYPE"] == "SQL":
                datastore: AzureSqlDatabaseDatastore = Datastore.register_azure_sql_database(
                    workspace=ws,
                    datastore_name=datastore_dict["DATASTORE_NAME"],
                    server_name=auth_dict["SERVER"],
                    database_name=auth_dict["DATABASE"],
                    username=auth_dict["USERNAME"],
                    password=kv.get_secret(auth_dict["PASSWORD_SECRET"])
                )
            print(f"[{dt.now()}] Registered '{datastore_dict['DATASTORE_NAME']}' as {datastore_dict['TYPE']} Datastore in the workspace.")

        # register/get all datasets within the datastore
        for dataset_name, dataset_dict in datastore_dict["DATASETS"].items():     
            # try to get dataset, otherwise create and register dataset           
            try:
                dataset: Union[TabularDataset, FileDataset] = Dataset.get_by_name(ws, name=dataset_name)
                print(f"[{dt.now()}] Found {dataset_dict['TYPE'].capitalize()}Dataset '{dataset_name}' in the workspace.")
            except UserErrorException:
                if dataset_dict["TYPE"] == "file":
                    # create File dataset
                    dataset: FileDataset = Dataset.File.from_files((datastore, dataset_dict["PATH"]))
                    dataset: FileDataset = dataset.register(ws, name=dataset_name)
                else:
                    # create Tabular dataset
                    if datastore_dict["TYPE"] != "SQL":
                        # if datastore type is not SQL, use Path
                        dataset: TabularDataset = Dataset.Tabular.from_delimited_files((datastore, dataset_dict["PATH"]))
                    else:
                        # if datastore type is SQL, use Query
                        dataset: TabularDataset = Dataset.Tabular.from_sql_query((datastore, dataset_dict["QUERY"]))
                    dataset: TabularDataset = dataset.register(ws, name=dataset_name)
                print(f"[{dt.now()}] Registered '{dataset_name}' as {dataset_dict['TYPE'].capitalize()}Dataset in the workspace.")
                

if __name__ == "__main__":
    main()