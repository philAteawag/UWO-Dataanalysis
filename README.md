# UWO-Dataanalysis

Some scritps to analyze the UWO Data

## Overview Scripts

### Python Scripts

1. QH_relation.py:

    script to find QH ([l/s] and [mm]) relation for sensors that do measure both parameters. 
    
2. check_sensors.py
    
    First attempt of script to automatically detect faulty sensors. The script should be run once a week. 
        
    - PSR check: 
        - Detection of PSR drop should work (more or less)
        - Finetuning of threshold (wheter or not a sensor is suspicious) migth be necessary.
        - Script generates a report that lists all suspicious sensors.
            
    - Signal validation
         - first attempt to add another check, that validates the actual measurement
         - only have "proof of concept". Must be further developed before it has any use
         - Is not included on report
 
 3.  plot_PSR.py
  
     script to create PSR Heatmaps.
     
 4.  libs/helper_functions.py
 
     most of the helper functions are found in this file
    
 5. create_psr_pp.py
  
     Not really useful anymore... was used to automate a pp 
     
 ### Notebooks
 
 1. CompareFilteredSignal.ipynb
    
    Interactive notebook to compare the signal of multiple sensors. It has some filtering options to filter out noise which increases abitlity to compare sensors.
 
 2. Compare_groups.ipynb
 
    Interactive notebook to create boxplots to compare signal of multiple sensors.
    
 3. dataanalyisis2.ipynb
    
    can be deleted...
  
 4. explore_data_da.ipynb

    can be deleted...
    
 5. main.ipynb

    can be deleted...
    
 ### other files
 
 1. config_concept.json
     
    This file links similiar measuring sensors. Such a file is needed if signal validation (at least the one that ive started) should be outmated.
    The file is completely incomplete and only shows the necessary structure.
    
 2. requirements.txt
 
    Things should run if these packages are installed
 
    
  

  
  
