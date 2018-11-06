# Utils
----
## write\_exp\_utils.py

### ExperimentConfig class
This class contains modules needed to insert experiment details (e.g. models, hyperparameters, time and space resolution, etc.) to the database. Please do the following steps to add a new experiment to the database:

1. Create an experiment configuration file (yaml) in src/experiment_config directory which describes the details of a new experiment (e.g. models, hyperparameters, lag, time and space resolution, etc.)
2. Create a feature configuration yaml file which includes a list of features that will be included in the experiment as well as the names of feature tables.  A feature config file for top k features can generated through a feature selection process, please see [this wiki page](https://github.com/dssg/rws_accident_prediction/wiki/Feature-Selection) for more information.
3. run "python create_experiment.py #experiment\_config\_path #feature\_config\_path"


### Experiment class
This class  reads experiment details from the database and instantiates an  object which encapsulates  all the experiment details needed to run an experiment.  

### ResultConfig class
This class contains modules needed to insert basic experimental results details (e.g. raw prediction scores) and other house keeping information.
    

----
## read\_exp\_util.py
This file contains modules needed to read experimental results and other experimental details from the database.


