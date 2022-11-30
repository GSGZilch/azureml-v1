import os
import argparse
import pandas as pd
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from azureml.core import Run, Experiment, Workspace, Datastore, Model

# Read dataset, code specific for ML pipelines
parser = argparse.ArgumentParser()
# run_datetime is not actually used in current demo
parser.add_argument('--run_datetime', dest='run_datetime', required=True)
args = parser.parse_args()

run = Run.get_context(allow_offline=False)
experiment = run.experiment
ws = experiment.workspace 

mounted_path = run.input_datasets['titanic_input_dataset']
csv_path = os.path.join(mounted_path, 'titanic_dataset.csv')

# From here on, we can reuse the code from the notebooks
df = pd.read_csv(csv_path)

target = 'Survived'

X = df.loc[:, df.columns != target]
y = df[target]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=123
)

rf = RandomForestClassifier(
    n_estimators=50,
    random_state=123
)

rf.fit(X_train, y_train)

pred = rf.predict(X_test)

print(f"Accuracy: {accuracy_score(y_test, pred)}")

# Save the model as pickle file
if not os.path.exists('model/'):
    os.makedirs('model/')

pickle.dump(rf, open('model/rf.pkl', 'wb'))

# Register model in AzureML Model Registry
model = Model.register(ws, 'model', 'titanic_model')