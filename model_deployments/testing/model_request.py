import pandas as pd
from azureml.core import Workspace
from azureml.core.webservice import Webservice
import urllib.request
import json
import os
import ssl


def allowSelfSignedHttps(allowed):
    # bypass the server certificate verification on client side
    if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

allowSelfSignedHttps(True)

ws = Workspace.from_config()
service = Webservice(ws, 'titanic-aci-model')
url = service.scoring_uri

df = pd.read_csv('data/003_preprocessed/titanic_dataset.csv')

data = {
    "data": df.loc[20, df.columns != 'Survived'].values.flatten().tolist()
}

body = str.encode(json.dumps(data))
headers = {'Content-Type':'application/json'}
req = urllib.request.Request(url, body, headers)

try:
    response = urllib.request.urlopen(req)

    result = response.read()
    print(result)
except urllib.error.HTTPError as error:
    print("The request failed with status code: " + str(error.code))

    # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
    print(error.info())
    print(error.read().decode("utf8", 'ignore'))
