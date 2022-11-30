import os

ENV = os.environ["DTAP_ENVIRONMENT"]

DATASTORE_CONFIG = [
    {
        "DATASTORE_NAME": "bc_blob",
        "STORAGE_NAME": f"bcadlsweu001",
        "TYPE": "BLOB",
        "CONTAINER": "dataplatform",
        "AUTH": {
            "ACCOUNT_KEY_SECRET": "bc-adls-account-key",
        },
        "DATASETS": {
            "ds-titanic-raw": {
                "TYPE": "file",
                "PATH": "bronze/titanic/**"
            },
            "ds-titanic-cleaned": {
                "TYPE": "file",
                "PATH": "ml/cleaned/**"
            },
            "ds-titanic-preprocessed": {
                "TYPE": "file",
                "PATH": "ml/preprocessed/**"
            },
            "ds-titanic-preprocessed-tabular": {
                "TYPE": "tabular",
                "PATH": "ml/preprocessed/titanic_dataset.csv"
            }
        } 
    }
]