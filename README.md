# UWO data analysis

A collection of scripts for analyzing data from the UWO.

## Data access

1. decentlab_client.py

    Connects to decentlab api and downloads data to a local folder.

2. example_queries.jl

    Sample queries to illustrate access to observation data in Julia.

3. example_queries.m

    Sample queries to illustrate access to observation data in Octave/Matlab.

4. example_queries.py

    Sample queries to illustrate access to observation data in Python.

5. query_datapool.py

    Sample usage of the datapool_client package.

## Field observation

1. check_sqlite_consistency.py

    Check how to merge files, check if data is consitent (read source data from life system and
    compare to sqlite), check if only the data is in there that we want to publish, is there
    unnecessary data, export some example images and check them, ...

## Maintenance
    
1. check_sensors.py
    
    First attempt of script to automatically detect faulty sensors. The script should be run once a week. 
        
    - PSR check: 
        - Detection of PSR drop should work (more or less)
        - Finetuning of threshold (wheter or not a sensor is suspicious) migth be necessary.
        - Script generates a report that lists all suspicious sensors.
            
    - Signal validation
         - first attempt to add another check, that validates the actual measurement
         - only have "proof of concept". Must be further developed before it has any use
         - Is not included on report
 
2. comparefilteredsignals.ipynb

    Interactive notebook to compare the signal of multiple sensors. It has some filtering options to filter out noise which increases abitlity to compare sensors.

3. comparegroups.ipynb

    Interactive notebook to create boxplots to compare signal of multiple sensors.

4. config_concept.json
    
    This file links similiar measuring sensors. Such a file is needed if signal validation (at least the one that ive started) should be outmated.
    The file is completely incomplete and only shows the necessary structure.

5. datasliceconsistency.py

    With this script the data of the exported slices can be checked for their consistency with respect to the "dataslices_content_overview.csv" overview and the measured variables runoff and rainfall. The results are output in a log file.

    Specifically, the number of data points for the sources and variable combinations for each year are output.
    The amount of rain should be between 1000 and 2000 mm/a. Negative values are set to 0.
    The amount of runoff should be less than the amount in the inflow of the ARA (bf_plsZUL1100_inflow_ara). Negative values are set to 0.
    
5.  helper_functions.py

    most of the helper functions are found in this file

6.  plot_psr.py

    script to create PSR Heatmaps.

7. qh_relation.py:

    script to find QH ([l/s] and [mm]) relation for sensors that do measure both parameters.

## Paper

Scripts whose output is used directly in the paper.

1. figure_4_right.py

    Produces the right part of Figure 4 with an overview of the available data in the form of a heat map.

2. table_1.py

    Produces the content of Table 1.
