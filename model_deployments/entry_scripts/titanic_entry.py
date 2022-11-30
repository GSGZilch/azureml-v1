import os
import pickle
import json
import numpy as np


def init():
    global model
    
    model_path = os.path.join(os.getenv('AZUREML_MODEL_DIR'), 'model', 'rf.pkl')

    model = pickle.load(open(model_path, 'rb'))


def run(data):
    try:
        body = json.loads(data)
        
        sample = body['data']
        sample = np.array(sample).reshape(1, -1)

        pred = model.predict(sample)

        return str(pred)

    except Exception as e:
        return str(e)