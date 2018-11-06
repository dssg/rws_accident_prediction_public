
## Steps to Run Full Pipeline
1. ETL and feature generation (for speed/flow and ongevallen) : Run the steps listed in README in the ../database folder <a href="https://github.com/dssg/rws_accident_prediction/tree/dev/database">here</a>

2. Generate hectopunten features
  - Update configuration settings in the top of data/generateHectopuntenFeatures.py 
        e.g. set the appropriate segmentation table to use to cluster hectopunten
  - Run generateHectopuntenFeatures.py
  
3. Generate weather features
  - Run generateWeatherFeatures.py
  
4. Generate temporal features
  - Run generateTemporalFeatures.py 
  
5. Create configuration files and run experiments: Run the steps described <a href="https://github.com/dssg/rws_accident_prediction/wiki/Experimentation">here</a>

6. Generate PR curves using the steps described <a href="https://github.com/dssg/rws_accident_prediction/wiki/Evaluation">here</a>

7. Plot results on heatmap using the instructions described <a href="https://github.com/dssg/rws_accident_prediction/wiki/Visualization-using-Heatmaps">here</a>
