--- Create the tables to store experimental inputs and results into

--- Schema: rws_experiment

--- Tables
---		results_table: Stores the results for each model run, including precision/recall curves
---		experiment_table: stores parameters for each run such as feature lists and test/train splits.  The inputs for this table are generated using a Python script.

CREATE SCHEMA IF NOT EXISTS rws_experiment;

create table rws_experiment.experiment_table(
    experiment_id serial primary key,
    
    experiment_datetime timestamp,
    time_granularity int not null,
    segmentation_table varchar not null,
    time_lag_minutes int not null,
    weather_station_mapping_table varchar not null,
    
    sample_train boolean not null,
    sample_val boolean not null,
    sample_test boolean not null,
    train_sample_method_params jsonb not null,
    val_sample_method_params jsonb not null,
    
    test_sample_method_params jsonb not null,
    features_knmi_config jsonb not null,
    features_hectopunten_config jsonb not null,
    features_flow_config jsonb not null,
    features_temporal_config jsonb not null,
    
    features_ongevallen_config jsonb not null,
    validation_config jsonb not null,
    classifiers jsonb not null,
    grid_parameters jsonb not null,
    random_seed int not null,
    
    exp_remarks text not null);

create table rws_experiment.result_table(
    result_id serial primary key,
    experiment_id int not null,
    val_set_id int not null,
    result_datetime timestamp not null,
    model text not null,
    parameters_id text not null,
    parameters jsonb not null,
    split_info jsonb not null,
    train_size int not null,
    val_size int not null,
    actual_features_size int not null,
    accident_pct_train_pre_sample float not null,
    accident_pct_val float not null,
    p_8_dayshift float,
    p_3_nightshift float,
    r_8_dayshift float,
    r_3_nightshift float,
    auc_roc float,
    pickle_path text not null,
    remarks text);

alter table  rws_experiment.result_table
  rename column p_3_nightshift to p_1_nightshift;

alter table  rws_experiment.result_table
  rename column r_3_nightshift to r_1_nightshift;
   
create table rws_experiment.raw_y_score(
   result_id int not null unique,
   experiment_id int not null,
   val_set_id int not null,
   y_true  int[] not null,
   y_scores float[] not null,
   space_time json not null,
   roc jsonb, 
   precision_recall_curve jsonb);
