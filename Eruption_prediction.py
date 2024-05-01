#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!pip install kaggle
#!pip install --upgrade pandas


# In[ ]:


import os
import pathlib
import zipfile
import glob

import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from lightgbm import LGBMRegressor

import pickle


# In[ ]:


pathlib.Path("./predict-volcanic-eruptions-ingv-oe").mkdir(parents=True, exist_ok=True)


# In[ ]:


os.environ['KAGGLE_USERNAME'] = "rubbenliu"
os.environ['KAGGLE_KEY'] = "4193b9c51b3a2626398f17079aaeab3f"

#!kaggle competitions download -c predict-volcanic-eruptions-ingv-oe


# In[ ]:


if not pathlib.Path("./predict-volcanic-eruptions-ingv-oe/sample_submission.csv").exists():
    zip_path = "/home/ec2-user/SageMaker/predict-volcanic-eruptions-ingv-oe.zip"

    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall("./predict-volcanic-eruptions-ingv-oe")


# In[ ]:


def build_features(signal, ts, sensor_id):
    X = pd.DataFrame()
    f = np.fft.fft(signal)
    f_real = np.real(f)
    X.loc[ts, f'{sensor_id}_sum']       = signal.sum()
    X.loc[ts, f'{sensor_id}_mean']      = signal.mean()
    X.loc[ts, f'{sensor_id}_std']       = signal.std()
    X.loc[ts, f'{sensor_id}_var']       = signal.var() 
    X.loc[ts, f'{sensor_id}_max']       = signal.max()
    X.loc[ts, f'{sensor_id}_min']       = signal.min()
    X.loc[ts, f'{sensor_id}_skew']      = signal.skew()
    #X.loc[ts, f'{sensor_id}_mad']       = signal.mad()
    X.loc[ts, f'{sensor_id}_kurtosis']  = signal.kurtosis()
    X.loc[ts, f'{sensor_id}_quantile99']= np.quantile(signal, 0.99)
    X.loc[ts, f'{sensor_id}_quantile95']= np.quantile(signal, 0.95)
    X.loc[ts, f'{sensor_id}_quantile85']= np.quantile(signal, 0.85)
    X.loc[ts, f'{sensor_id}_quantile75']= np.quantile(signal, 0.75)
    X.loc[ts, f'{sensor_id}_quantile55']= np.quantile(signal, 0.55)
    X.loc[ts, f'{sensor_id}_quantile45']= np.quantile(signal, 0.45) 
    X.loc[ts, f'{sensor_id}_quantile25']= np.quantile(signal, 0.25) 
    X.loc[ts, f'{sensor_id}_quantile15']= np.quantile(signal, 0.15) 
    X.loc[ts, f'{sensor_id}_quantile05']= np.quantile(signal, 0.05)
    X.loc[ts, f'{sensor_id}_quantile01']= np.quantile(signal, 0.01)
    X.loc[ts, f'{sensor_id}_fft_real_mean']= f_real.mean()
    X.loc[ts, f'{sensor_id}_fft_real_std'] = f_real.std()
    X.loc[ts, f'{sensor_id}_fft_real_max'] = f_real.max()
    X.loc[ts, f'{sensor_id}_fft_real_min'] = f_real.min()

    return X


# In[ ]:


train = pd.read_csv("./predict-volcanic-eruptions-ingv-oe/train.csv")
sample_submission = pd.read_csv("./predict-volcanic-eruptions-ingv-oe/sample_submission.csv")


# In[ ]:


test_frags = glob.glob("./predict-volcanic-eruptions-ingv-oe/test/*")


# In[ ]:


sensors = set()
observations = set()
nan_columns = list()
missed_groups = list()
for_test_df = list()

for item in test_frags:
    name = int(item.split('.')[-2].split('/')[-1])
    at_least_one_missed = 0
    frag = pd.read_csv(item)
    missed_group = list()
    missed_percents = list()
    for col in frag.columns:
        missed_percents.append(frag[col].isnull().sum() / len(frag))
        if pd.isnull(frag[col]).all() == True:
            at_least_one_missed = 1
            nan_columns.append(col)
            missed_group.append(col)
    if len(missed_group) > 0:
        missed_groups.append(missed_group)
    sensors.add(len(frag.columns))
    observations.add(len(frag))
    for_test_df.append([name, at_least_one_missed] + missed_percents)


# In[ ]:


for_test_df = pd.DataFrame(
    for_test_df, 
    columns=[
        'segment_id', 'has_missed_sensors', 'missed_percent_sensor1', 'missed_percent_sensor2', 'missed_percent_sensor3', 
        'missed_percent_sensor4', 'missed_percent_sensor5', 'missed_percent_sensor6', 'missed_percent_sensor7', 
        'missed_percent_sensor8', 'missed_percent_sensor9', 'missed_percent_sensor10'
    ]
)

for_test_df


# In[ ]:


train_set = list()
j=0
for seg in train.segment_id:
    signals = pd.read_csv(f'./predict-volcanic-eruptions-ingv-oe/train/{seg}.csv')
    train_row = []
    if j%500 == 0:
        print(j)
    for i in range(0, 10):
        sensor_id = f'sensor_{i+1}'
        train_row.append(build_features(signals[sensor_id].fillna(0), seg, sensor_id))
    train_row = pd.concat(train_row, axis=1)
    train_set.append(train_row)
    j+=1

train_set = pd.concat(train_set)


# In[ ]:


train_set = train_set.reset_index()
train_set = train_set.rename(columns={'index': 'segment_id'})
train_set = pd.merge(train_set, train, on='segment_id')
train_set


# In[ ]:


drop_cols = list()
for col in train_set.columns:
    if col == 'segment_id':
        continue
    if abs(train_set[col].corr(train_set['time_to_eruption'])) < 0.01:
        drop_cols.append(col)


# In[ ]:


not_to_drop_cols = list()

for col1 in train_set.columns:
    for col2 in train_set.columns:
        if col1 == col2:
            continue
        if col1 == 'segment_id' or col2 == 'segment_id': 
            continue
        if col1 == 'time_to_eruption' or col2 == 'time_to_eruption':
            continue
        if abs(train_set[col1].corr(train_set[col2])) > 0.98:
            if col2 not in drop_cols and col1 not in not_to_drop_cols:
                drop_cols.append(col2)
                not_to_drop_cols.append(col1)


# In[ ]:


train = train_set.drop(['segment_id', 'time_to_eruption'], axis=1)
y = train_set['time_to_eruption']


# In[ ]:


train.to_csv('preprocessed_train.csv', sep='\t', encoding='utf-8')


# In[ ]:


reduced_y = y.copy()
reduced_train = train.copy()
reduced_train = reduced_train.drop(drop_cols, axis=1)
reduced_train


# In[ ]:


train, val, y, y_val = train_test_split(train, y, random_state=42, test_size=0.2, shuffle=True)
reduced_train, reduced_val, reduced_y, reduced_y_val = train_test_split(reduced_train, reduced_y, random_state=42, test_size=0.2, shuffle=True)


# In[ ]:


lgb = LGBMRegressor(
    random_state=666, 
    max_depth=7, 
    n_estimators=250, 
    learning_rate=0.12
)

lgb.fit(train, y)
preds = lgb.predict(val)


# In[ ]:


def rmse(y_true, y_pred):
    return math.sqrt(mse(y_true, y_pred))


# In[ ]:


print('Simple LGB model rmse: ', rmse(y_val, preds))


# In[ ]:





# In[ ]:


test_set = list()
j=0
for seg in sample_submission.segment_id:
    signals = pd.read_csv(f'./predict-volcanic-eruptions-ingv-oe/test/{seg}.csv')
    test_row = []
    if j%500 == 0:
        print(j)
    for i in range(0, 10):
        sensor_id = f'sensor_{i+1}'
        test_row.append(build_features(signals[sensor_id].fillna(0), seg, sensor_id))
    test_row = pd.concat(test_row, axis=1)
    test_set.append(test_row)
    j+=1
test_set = pd.concat(test_set)


# In[ ]:


test_set = test_set.reset_index()
test_set = test_set.rename(columns={'index': 'segment_id'})
test_set = pd.merge(test_set, for_test_df, how='left', on='segment_id')
test = test_set.drop(['segment_id'], axis=1)
test


# In[ ]:


reduced_test = test.copy()
reduced_test = reduced_test.drop(drop_cols, axis=1)
reduced_test


# In[ ]:


test.to_csv('preprocessed_test.csv', sep='\t', encoding='utf-8')


# In[ ]:


preds1 = lgb.predict(test)
preds1


# In[ ]:


lgb.booster_.save_model('lgbr_base.txt')


# In[ ]:





# In[ ]:


# from sagemaker import image_uris, model_uris, script_uris

# train_model_id, train_model_version, train_scope = "lightgbm-classification-model", "*", "training"
# training_instance_type = "ml.m5.xlarge"

# # Retrieve the docker image
# train_image_uri = image_uris.retrieve(
#     region=None,
#     framework=None,
#     model_id=train_model_id,
#     model_version=train_model_version,
#     image_scope=train_scope,
#     instance_type=training_instance_type
# )

# # Retrieve the training script
# train_source_uri = script_uris.retrieve(
#     model_id=train_model_id, model_version=train_model_version, script_scope=train_scope
# )

# train_model_uri = model_uris.retrieve(
#     model_id=train_model_id, model_version=train_model_version, model_scope=train_scope
# )

# # Sample training data is available in this bucket

# training_dataset_s3_path = "./predict-volcanic-eruptions-ingv-oe/train" 
# validation_dataset_s3_path = "./predict-volcanic-eruptions-ingv-oe/test" 


# s3_output_location = f"./predict-volcanic-eruptions-ingv-oe/output"
# pathlib.Path("s3_output_location").mkdir(parents=True, exist_ok=True)

# from sagemaker import hyperparameters

# # Retrieve the default hyperparameters for training the model
# hyperparameters = hyperparameters.retrieve_default(
#     model_id=train_model_id, model_version=train_model_version
# )

# # [Optional] Override default hyperparameters with custom values
# hyperparameters[
#     "num_boost_round"
# ] = "500"
# print(hyperparameters)

# from sagemaker.estimator import Estimator
# from sagemaker.utils import name_from_base

# training_job_name = name_from_base(f"built-in-algo-{train_model_id}-training")

# # Create SageMaker Estimator instance
# tabular_estimator = Estimator(
#     role=aws_role,
#     image_uri=train_image_uri,
#     source_dir=train_source_uri,
#     model_uri=train_model_uri,
#     entry_point="transfer_learning.py",
#     instance_count=1, # for distributed training, specify an instance_count greater than 1
#     instance_type=training_instance_type,
#     max_run=360000,
#     hyperparameters=hyperparameters,
#     output_path=s3_output_location
# )

# # Launch a SageMaker Training job by passing the S3 path of the training data
# tabular_estimator.fit(
#     {
#         "train": training_dataset_s3_path,
#         "validation": validation_dataset_s3_path,
#     }, logs=True, job_name=training_job_name
# )

