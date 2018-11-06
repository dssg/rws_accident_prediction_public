
## Generate Heatmap of Risk Levels for Hectopunten
To visualize the results of the prediction model, run the Create_Accident_Prediction_Map.ipynb file.
The resulting map will be saved in the visualization directory as a Accident_Prediction_Map.html file. 
To change configurations, such as the id of the result which you which to visualize, and the time period over which you wish to visualize accidents, edit the config settings at the beginning of the notebook.

The resulting map will include a layer for each timestamp.  Press the layers icon in the top right corner of the screen to see the checkboxes for each timestamp.  Uncheck all of the boxes and check one by one the timestamps which you wish to see.  The timestamp refers to the start of the prediction time period. Each hectopunten segment is colored based on the risk level assignment for the segment by the model that was run (which was specified by the experiment and result id set in config).  You can click on each hectopunten to see the exact number that was assigned to the segment.  In addition, if an accident actually occurred over the time period, the accident is shown using a marker. 
