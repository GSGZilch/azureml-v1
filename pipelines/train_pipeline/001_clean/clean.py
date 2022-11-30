import os
import argparse
import pandas as pd
from azureml.core import Run, Experiment, Workspace, Datastore

# Read dataset, code specific for ML pipelines
parser = argparse.ArgumentParser()
# run_datetime is not actually used in current demo
parser.add_argument('--run_datetime', dest='run_datetime', required=True)
parser.add_argument('--cleaned_output_path', dest='output_path', required=True)
args = parser.parse_args()

run = Run.get_context(allow_offline=False)
experiment = run.experiment
ws = experiment.workspace 

mounted_path = run.input_datasets['titanic_input_dataset']
csv_path = os.path.join(mounted_path, 'titanic_dataset.csv')

# From here on, we can reuse the code from the notebooks
df = pd.read_csv(csv_path)

df['Age'] = df['Age'].fillna(round(df['Age'].mean()))
df['Embarked'] = df['Embarked'].fillna('S')

if not os.path.exists('data/'):
    os.makedirs('data/')

df.to_csv('data/titanic_dataset.csv', index=False)

# Write dataset, code specific for ML pipelines
datastore = Datastore.get(ws, datastore_name='bc_blob')

datastore.upload(
    src_dir='data/',
    target_path=args.output_path,
    overwrite=True)