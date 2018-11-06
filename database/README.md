
# SQL and ETL pipeline
The following steps need to be completed in the order listed to build the database and generate features. Files should be run in SQL unless specified otherwise
## ETL pipeline
  - 01_create_postgres_tables.sql
      - Objective: Create the schemas and tables to copy the data, currently stored in the directory, into Postgres
      - Note: This script is set to create speed and flow tables that were downloaded from NDW at 15 min resolution.  Change the name of the tables  for speed and flow within the script from '15min' to '1hr' if running 1 hour flow and speed data.
      </br>
  - 02_copy_data_to_postgres.sh
      - Objective: Copy data from EC2 to Database
      - Note: Run as a bash shell script
      - Note: This script is set to create speed and flow tables that were downloaded from NDW at 15 min resolution.  Change the name of the tables  for speed and flow within the script from '15min' to '1hr' if running 1 hour flow and speed data.
  
## Clean raw data
  - 03_create_cleaned_tables.sql
    - Objective: Clean the raw tables (e.g. rename columns to English, change column types). Filter the data to only include the Utrecht region
    </br>
  - For step 4, run the script corresponding to the resolution of the downloaded speed and flow data
    - Objective:  Clean the flow and speed data.  Speed and flow were provided as two separate cvs's.  This code joins the two datasets and preprocesses the speed and flow data
    - 04_clean_speed_flow_1hr_resolution.sql
    - 04_clean_speed_flow_15min_resolution.sql
    </br>
  - 05_link_loops_to_hectopoints
    - Objective: This script links each loop to the nearest hectopunten, using a datset provided by CSResearch
    </br>
  - 06_create_loop_network.sql
    - Objective: This script links each flow loop to both the loop behind it, and the loop in front of it in terms of traffic flow
    </br>
  - (Note currently in use) 07_create_hectopunten_network.sql
    - Objective: This script links each hectopoint to the hectokey in front and behind it 
    - Note: This table is not currently being used, but may be useful in the future to generate features across linked hectopunten
    </br>
  - 08_segment_hectopunten.sql
    - Objective: This script clusters the hectopunten that are located on the main highways (also called main carriageways) 

## Generate Features (Not including flow)
Note: Step 9 creates the schemas for hectopunten and weather features.  It should be run prior to running the HectopuntenFeatureFactory and WeatherFeatureFactory Python scripts in 'src/data'.
- 09_generate_features.sql
  - Objective: This script generates the schema that will be used to store feature tables that do not include speed and flow
  - Note: This script should be run prior to running the Python scripts to generate Hectopunten and weather features in 'src/data'
  </br>
- 11_generate_ongeval_features.sql
  - Objective: Generate features related to past incidents
  - Note: The ordering of this step and step 10 (generate flow features) does not matter
  
## Generate Flow Features
- (Step 10)
- Flow features are generated separately through a Python script  
- To run flow features at 15 min resolution, a Python script located in 'src/data/SpeedFlowFeatureFactory.py' has been created that automatically updates the version number and runs through the 3 steps (10a, 10b, 10c) to generate flow features. Update the config parameters at the beginning of the script as needed.

## Create Experiment database
- 12_create_model_tables.sql
  - Objective: Create the tables to store experimental inputs and results into
