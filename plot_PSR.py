import sys
sys.path.append(r'C:\Users\steineph\DataAnalysis\UWO-Dataanalysis\libs')
from datapool_client import DataPool
from UWO_DataAnalysis import helper_functions
import pandas as pd
import argparse
import tqdm
import os
from matplotlib import pyplot as plt
import seaborn as sns
from pathlib import Path
import datetime



def make_PSR_Plot(how, frequency,save_directory, start_date='2016-01-01', end_date=''):
    '''
    Function to Plot the PSR over time in as a heatmap. 

    Parameters 
    ----------
    how : str 
        Defines wheter to plot each sensor separately or groupwise (eg all bf, bt, ... together)
        Options are <1_Sensor_x_Frequencies> and <x_Sensors_1_Frequency>.
            1_Sensor_x_Frequencies: each Plot shows the PSR for one single sensor, however we can now show multiple PSR Frequencies 
                    within the same plot
            x_Sensors_1_Frequency: Sensors of the same type are grouped, and there will be one PSR Plot per group. However we cannot plot
                    multiple PSR Frequencies within the same plot.
    frequency : list
        A List containing the PSR Frequencies that we want to plot. Go from slowest frequency to highest. Possible values are
        ['Y', 'Q', 'M', 'W', 'D']
    save_directory : str
        directory where the plots will be saved. make sure you pass the path like this : 'C:/... instead of 'C:\...
    '''
    dp=DataPool()
    freq_annot_dict = {'Y': True, 'Q':True, 'M':False, 'W':False, 'D':False}
    freq_ShortLong_dict = {'Y': 'Yearly', 'Q':'Quarterly', 'M':'Monthly', 'W':'Weekly', 'D':'Daily'}

    today=datetime.date.today()
    #To get a good estimate of this weeks psr, we include all data until the previous day until 23:59:59
    yesterday = today- datetime.timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")+ " 23:59:59"

    if end_date=='':
        end_date=yesterday 
    
    if how == '1_Sensor_x_Frequencies':

        for source in tqdm.tqdm(list(dp.source.all()['name'])):
            savepath=os.path.join(save_directory, '{}'.format(source.split('_')[0]))
            if not os.path.exists(savepath):
                os.makedirs(savepath)
            
            if source != 'bt_dl942_11_veloweg_sd':
                fig, ax = plt.subplots(len(frequency), sharey=True, figsize=(15, len(frequency)))
                fig.suptitle("{}".format(source), fontsize="x-large")
                cbar_ax = fig.add_axes([.91, .3, .03, .4])
                print(source, '\n')

                grouped_data = []
                grouped_data_long = []
            
                for freq in frequency:
                    grouped_data_x = helper_functions.calculate_PSR(source_name=source, start_date=start_date, resolution=freq, allow_higher_samplingrates=False, end_date=end_date)
                    grouped_data.append(grouped_data_x)

                    grouped_data_long_x=pd.pivot(grouped_data_x, index='sensor', columns='timestamp', values='normalized_count')
                    grouped_data_long.append(grouped_data_long_x)

                for i, freq in enumerate(frequency):
                    sns.heatmap(data=grouped_data_long[i], annot=freq_annot_dict[freq], cmap='RdYlGn', vmin=0, vmax=1, ax=ax[i], cbar_ax=cbar_ax)
                    if i < len(frequency)-1:
                        ax[i].get_xaxis().set_visible(False)
                    else:
                        ax[i].set_xticklabels([pd.to_datetime(t.get_text()).strftime('%D') for t in ax[i].get_xticklabels()])
                    
                    ax[i].set_yticklabels([]) 
                    ax[i].tick_params(left=False)
                    ax[i].set_ylabel(freq_ShortLong_dict[freq])
                
                fig.savefig(os.path.join(savepath, '{}.png'.format(source)), dpi=1000, bbox_inches='tight')
                plt.close('all')
            else:
                continue

    elif how == 'x_Sensors_1_Frequency': 
        df_grouped=pd.DataFrame()
        sensor_groups=helper_functions.find_all_sensor_groups()
        for group in ['bp']:
            savepath=os.path.join(save_directory, group)
            if not os.path.exists(savepath):
                os.makedirs(savepath)
            nb_sensors_within_group=helper_functions.find_nb_of_sensor_within_group(group) #needed to know how big our plot should be
            
            #define size of the plot
            fig, ax = plt.subplots(figsize=(25, nb_sensors_within_group*1.5)) 
            dp = DataPool()
            for source in tqdm.tqdm(list(dp.source.all()['name'])):
                try:
                    if source.split('_')[0] == group:
                        PSR_data = helper_functions.calculate_PSR(source_name=source, start_date=start_date, resolution=frequency[0], allow_higher_samplingrates=False, end_date=end_date)
                        PSR_data_long=pd.pivot(PSR_data, index='sensor', columns='timestamp', values='normalized_count')
                        df_grouped=df_grouped.append(PSR_data_long)
                except:
                    print (f'skipped this sensor: {source}')
                   
            heatmap_plot = sns.heatmap(data=df_grouped, cmap='RdYlGn', vmin=0, vmax=1, ax=ax, annot=freq_annot_dict[frequency[0]])
            
            #ticklabel of x axis
            ax.set_xticklabels([pd.to_datetime(t.get_text()).strftime('%D') for t in ax.get_xticklabels()])
            
            #title of the plot
            fig.suptitle("PSR of {} sensors: Resolution : {}".format(group, frequency[0]), fontsize="x-large")
            fig.savefig(os.path.join(savepath,  '{}.png'.format('grouped_plot')), dpi=1000, bbox_inches='tight')
            plt.close('all')

    else:
        print('invalid option for how...')   
        return 0
        
def main(how, frequency, save_directory, start_date='2016-01-01', end_date=''):
    if how in['x_Sensors_1_Frequency','1_Sensor_x_Frequencies']:
        make_PSR_Plot(how, frequency=frequency,save_directory=save_directory, start_date=start_date, end_date=end_date)
    else: print("invalid value for how was set!")   


def parse_arguments():
    parser = argparse.ArgumentParser(description='Plot PSR Plots')
    parser.add_argument("--how", type=str, default='x_Sensors_1_Frequency', choices=['x_Sensors_1_Frequency', '1_Sensor_x_Frequencies'], help='Chose wheter you want a PSR Plot that is calcualted for one frequency \n'
                            "but shows all sensors belonging to the same group \n" 
                            "or wheter you want to plot one Sensor but with multiple Frequencies")
    parser.add_argument("--frequencies", default='Q', nargs='+', help="Define which Frequencies should be used for the PSR Plot\n"
                            "If <how> is set to <1_Sensor_x_Frequencies> you can provide a list of frequencies")
    parser.add_argument("--save_directory", default = '', help= 'directory where curve parameters will be stored')
    parser.add_argument("--start_date", default = '2016-01-01', help= 'first date to consider')
    parser.add_argument("--end_date", default = '', help= 'last date to consider')
    args = parser.parse_args()
    
    return args

if __name__ == '__main__':
   
    arguments = parse_arguments()
    
    main(how=arguments.how, frequency=arguments.frequencies, save_directory=arguments.save_directory,
        start_date=arguments.start_date, end_date=arguments.end_date)
    
