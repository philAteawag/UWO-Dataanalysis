import sys
from time import strftime
sys.path.append(r'C:\Users\steineph\DataAnalysis\UWO-Dataanalysis\libs')
from UWO_DataAnalysis import helper_functions 
from datapool_client import DataPool
import datetime
import numpy as np
from scipy import stats
import pandas as pd
import tqdm
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os

# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__

class PSR_Sensor():
    
    def __init__(self, source_name, start_date, end_date):
        self.old_mean = 0
        self.source_name = source_name
        self.loaded_data_since = start_date
        self.loaded_data_upto = end_date
        self.previous_PSR = self.calculate_previous_weekly_PSR(source_name=source_name, start_date=start_date, end_date=end_date)
        self.data = self.previous_PSR

    def calculate_previous_weekly_PSR(self, source_name, start_date, end_date):
        '''
        Calculate previous wekkly PSR
        '''
        blockPrint()
        previous_PSR=helper_functions.calculate_PSR(source_name=source_name, start_date=start_date, end_date=end_date, resolution='W', allow_higher_samplingrates=False)
        enablePrint()
        return previous_PSR

    def calculate_z_score_PSR(self):
        
        psr_df=self.data
        
        #replace inf values with nan
        psr_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        #drop nan values 
        psr_df=psr_df.dropna()

        #make two tables. First stores all psr of previous weeks, second one stores psr of current (last recorded ) week
        psr_previous=psr_df.iloc[0:-1]
        psr_current = psr_df.iloc[-1]

        #we drop the psr values of previous weeks if they were strong outliers
        psr_previous['z_score']=stats.zscore(psr_previous['normalized_count'])
        psr_df_filtered=psr_previous[psr_previous['z_score']>-3]
        
        #calculate the mean value of PSR over the last couple of weeks (disregarding the current week) and update variable
        self.old_mean=np.mean(psr_df_filtered['normalized_count'])

        #now add the current week to the table again
        #psr_df_filtered=pd.concat([psr_df_filtered,psr_current])
        psr_df_filtered=psr_df_filtered.append(psr_current)
        
        #calculate the z score
        psr_df_filtered['z_score']=stats.zscore(psr_df_filtered['normalized_count'])
        self.data = psr_df_filtered
        return psr_df_filtered

        

class Sensor_Report(PdfPages):
    def __init__(self, filename):
        self.current_page = 0    
        super().__init__(filename=filename)
        
    def df_to_pdf(self, df, table_info): 
        '''
        converts df into matplotlib table which can than be converted into a pdf

        Parameters
        ----------
        df : pandas DataFrame
            the dataframe that should be converted into a pdf
        table_info : str
            Table info that will be plotted on top of the table
        numb_pages : int
            Number at which the report starts. 
            Need this as we want to repeatedly add pages and tables to the report
        '''

        total_rows, total_cols = df.shape;

        rows_per_page = 30; # Number of rows per page
        rows_printed = 0
        page_number = self.current_page;
        while (total_rows >0):
            fig=plt.figure(figsize=(8.5, 11))
            plt.gca().axis('off')
            matplotlib_tab = pd.plotting.table(plt.gca(),df.iloc[rows_printed:rows_printed+rows_per_page],
                loc='upper center', colWidths=[0.15]*total_cols)
            #Tabular styling
            table_props=matplotlib_tab.properties()
            table_cells=table_props['children']
            #matplotlib_tab.auto_set_font_size(False)
            #matplotlib_tab.set_fontsize(4)
            for cell in table_cells:
                cell.set_height(0.024)
                cell.set_fontsize(5)
            # Header,Footer and Page Number
            fig.text(4.25/8.5, 10.5/11., table_info, ha='center', fontsize=12)
            fig.text(4.25/8.5, 0.5/11., 'P'+str(page_number), ha='right', fontsize=8)
            self.savefig()
            plt.close()
            #Update variables
            rows_printed += rows_per_page;
            total_rows -= rows_per_page;
            page_number+=1;
            self.current_page=page_number

def main():
    
    dp=DataPool()
    #loop thorugh sensors that sent signal the previous month:
    today=datetime.date.today()
    #To get a good estimate of this weeks psr, we include all data until the previous day until 23:59:59
    yesterday = today- datetime.timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")+ " 23:59:59"

    #we compare it to the PSR over the last 16 months
    last_4month = today - datetime.timedelta(weeks=16)
    last_4month = last_4month.strftime("%Y-%m-%d")
    
    #we make three different tables:
    skipped_sensors = pd.DataFrame(columns=['source_name'])
    suspicious_sensors = pd.DataFrame(columns=['source_name', 'last_recorded_PSR', 'mean of last 4 months', 'Last weeks PSR','z_score'])
    unsuspicious_sensors = pd.DataFrame(columns=['source_name', 'last_recorded_PSR', 'mean of last 4 months', 'Last weeks PSR','z_score'])
    counter=0
    for sensor in tqdm.tqdm(dp.source.all()['name'].unique()):
        counter=counter+1
        if counter<10:
            try:   
                sensor_to_check=PSR_Sensor(sensor, start_date=last_4month, end_date=yesterday)
                sensor_to_check.calculate_z_score_PSR()
                #make decision wheter or not current PSR is suspicious
                if sensor_to_check.data.iloc[-1]['z_score']<-2:
                    suspicious_sensor = pd.DataFrame(columns=['source_name', 'last_recorded_PSR', 'mean of last 4 months', 'Last weeks PSR', 'z_score'], data=[[sensor, str(sensor_to_check.data.iloc[-1]['timestamp'])[0:10], sensor_to_check.old_mean, sensor_to_check.data.iloc[-1]['normalized_count'],sensor_to_check.data.iloc[-1]['z_score']]])
                    suspicious_sensors = pd.concat([suspicious_sensors,suspicious_sensor])
                else:
                    unsuspicious_sensor = pd.DataFrame(columns=['source_name','last_recorded_PSR', 'mean of last 4 months', 'Last weeks PSR', 'z_score'], data=[[sensor, str(sensor_to_check.data.iloc[-1]['timestamp'])[0:10], sensor_to_check.old_mean, sensor_to_check.data.iloc[-1]['normalized_count'], sensor_to_check.data.iloc[-1]['z_score']]])
                    unsuspicious_sensors= pd.concat([unsuspicious_sensors,unsuspicious_sensor])
                    
            except Exception as error:
                print(error)
                skipped_sensor=pd.DataFrame(columns=['source_name'], data=[sensor])
                skipped_sensors=pd.concat([skipped_sensors,skipped_sensor])
        
            suspicious_sensors=suspicious_sensors.sort_values(['last_recorded_PSR', 'Last weeks PSR'], ascending=[False, False]).reset_index().drop(columns='index')
            suspicious_sensors=suspicious_sensors.round(3)
            unsuspicious_sensors=unsuspicious_sensors.sort_values(['last_recorded_PSR', 'Last weeks PSR'], ascending=[False, False]).reset_index().drop(columns='index')
            unsuspicious_sensors=unsuspicious_sensors.round(3)
        else:
            break
    #make the report
    pp = Sensor_Report(f'PSR_Report_{today}.pdf')
    pp.df_to_pdf(suspicious_sensors, 'Sensors which PSR are suspicious:\nPSR is more than 2 standard deviation lower\nthan mean over last 16 weeks')
    pp.df_to_pdf(unsuspicious_sensors, 'Sensors which PSR are not suspicious:\nPSR is less than 2 standard deviation lower\nthan mean over last 16 weeks')
    pp.df_to_pdf(skipped_sensors, 'Skipped Sensors:\nProbably because no measurements during\nlast 4 months')      
    pp.close()  

if __name__ == '__main__':
    main()