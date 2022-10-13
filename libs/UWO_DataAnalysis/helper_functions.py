import seaborn as sns
from datapool_client import DataPool
import pandas as pd
import warnings
from tqdm.notebook import tqdm
import os
from matplotlib import pyplot as plt
import numpy as np
import plotly.express as px
import matplotlib
import ipywidgets as widgets
from ipywidgets import interact, interactive, fixed, Layout, interact_manual
import time

# connection parameters (example)

def normalize_sent_packages(row, most_common_timedelta):
    '''
    normalize the received packages (0 means 0 percent of the expected sent packages did arrive, 1 means 100% of the expected sent packages did arrive)
    The function will applied on a a dataframe. 
    The column must contain a column days(that ) 

    '''
    normalized_count=row['value']/(row['day_difference']*24*60/most_common_timedelta)
    return normalized_count


def calculate_total_mins(row, col_name):
    '''
    helper function to calculate total mins of a timedelta.
    '''
    timedelta_mins=row[col_name].total_seconds()/60
    try:
        return round(timedelta_mins)
    except:
        return timedelta_mins

def find_most_common_timedelta(timeseries):
    '''
    helper function to find the most common timedelta in a timeseries
    '''

    timeseries['timedelta']=timeseries['timestamp']-timeseries['timestamp'].shift(1)
    timeseries['timedelta_mins']=timeseries.apply(calculate_total_mins, col_name='timedelta', axis=1)
    most_common_timedelta=timeseries.groupby('timedelta_mins').count().sort_values('timestamp', ascending=False).iloc[0].name
    
    return most_common_timedelta

def extract_main_parameter(parameter_list):
    '''
    helper function to extract the main measurement parameter 
    '''
    dp=DataPool()
    all_parameters = dp.parameter.all()
    main_parameters = all_parameters[all_parameters['description'].str.startswith('Main measurement parameter')]
    main_parameters = list(main_parameters['name'])
    found_main_parameters=[]
    for parameter in parameter_list:
        if parameter in main_parameters:
            found_main_parameters.append(parameter)
    return found_main_parameters

def downsampling_faulty_sensors(dataframe, frequency):
    '''
    for some sensors, the sampling frequency is too high: This might happen, if the values are not only logged via lora but also manually. 
    For those sensors, we want to aggregate data, that belongs to the 'same timestamp'.
    For instance if the sensor only sends a signal every 5 minutes, but there are two values in a time period < 5min, one entry will be discarded)
    '''
    min=dataframe['timestamp'].min()
    max=dataframe['timestamp'].max()
    helper_df=pd.DataFrame()
    helper_df['artificial_timestamp']=pd.date_range(start=min, end=max, freq=frequency)
    df_merged=pd.merge_asof(left=helper_df, right=dataframe,left_on='artificial_timestamp', right_on='timestamp', direction='nearest')
    df_merged=df_merged.dropna()
    df_merged.drop(columns='artificial_timestamp')
    return df_merged

def downsampling_faulty_sensors2(dataframe, frequency):
    '''
    for some sensors, the sampling frequency is too high: This might happen, if the values are not only logged via lora but also manually. 
    For those sensors, we want to aggregate data, that belongs to the 'same timestamp'.
    For instance if the sensor only sends a signal every 5 minutes, but there are two values in a time period < 5min, one entry will be discarded)
    '''
    dataframe['timestamp']=dataframe['timestamp'].dt.round(frequency)
    dataframe=dataframe.drop_duplicates(subset='timestamp')
    return dataframe
        
def plot_packages_received_histogram(source_name, start_date, save_directory=''):
    dp=DataPool()
    #get all sensor data from the database
    all_sensor_data = dp.signal.get(source_name=source_name, start=start_date, minimal=False, show_query=False, to_dataframe=True)

    #we are only interested in the sensors actual value
    all_parameters=all_sensor_data['parameter'].unique()
    main_parameters=extract_main_parameter(all_parameters)
    if len(main_parameters)<1:
        warnings.warn('could not find a main measurement parameter for this sensor: {}'.format(source_name))
        f = open("log_histogram.txt", "a")
        f.write('could not find a main measurement parameter for this sensor: {}\n'.format(source_name))
        f.close()
        return None
    elif len(main_parameters)>1:
        warnings.warn('found multiple main measurement parameters ({}) for this sensor: {}. All but {} will be discarded!'.format(main_parameters, source_name, main_parameters[0]))
        f = open("log_histogram.txt", "a")
        f.write('found multiple main measurement parameters ({}) for this sensor: {}. All but {} will be discarded!\n'.format(main_parameters, source_name, main_parameters[0]))
        f.close()
        parameter_value=(main_parameters[0])
    else:
        parameter_value=main_parameters[0]
    parameter_data=all_sensor_data[all_sensor_data['parameter']==parameter_value]

    #convert to datetime
    parameter_data['timestamp']=pd.to_datetime(parameter_data['timestamp'])

    #find timedelta in minutes
    parameter_data['timedelta']=parameter_data['timestamp']-parameter_data['timestamp'].shift(1)
    parameter_data['timedelta_mins']=parameter_data.apply(calculate_total_mins, col_name='timedelta', axis=1)
    parameter_data=parameter_data.dropna()

    #plot a histogram
    
    plot_data=parameter_data['timedelta_mins'].astype('Int64')
    fig, ax = plt.subplots(nrows=3, ncols=1, figsize=(20, 30))
    
    plt.figure(dpi=1000)
    counts, bins, bars =ax[2].hist(plot_data,  bins=100, align='left', )
    ax[2].set_ylim(0, 200)
    
    plt.figure(dpi=1000)
    counts, bins, bars =ax[1].hist(plot_data,  bins=range(min(plot_data), 200, 1), align='left', )

    plt.figure(dpi=1000)
    
    counts, bins, bars =ax[0].hist(plot_data,  bins=range(min(plot_data), 24, 1), align='left', )

    ax[0].title.set_text('{}'.format(source_name))
    ax[0].set(xticks=np.arange(0,25,1))

    if save_directory!='':
        save_directory3=os.path.join(save_directory, 'timedelta_histograms_zoomed_extra', source_name.split('_')[0])
        
        if not os.path.exists(save_directory3):
            os.makedirs(save_directory3)
        
        fig.savefig(os.path.join(save_directory3, '{}.png'.format(source_name)))
        
    plt.cla()
    plt.close('all')

def plot_timeseries(source_name, start_date, save_directory=''):
    'plots_the_timeseries'
    dp=DataPool()
    #get all sensor data from the database
    all_sensor_data = dp.signal.get(source_name=source_name, start=start_date, minimal=False, show_query=False, to_dataframe=True)

    #we are only interested in the sensors actual value
    all_parameters=all_sensor_data['parameter'].unique()
    main_parameters=extract_main_parameter(all_parameters)
    if len(main_parameters)<1:
        warnings.warn('could not find a main measurement parameter for this sensor: {}'.format(source_name))
        return None
    elif len(main_parameters)>1:
        warnings.warn('found multiple main measurement parameters ({}) for this sensor: {}. All but {} will be discarded!'.format(main_parameters, source_name, main_parameters[0]))
        f = open("log_histogram.txt", "a")
        f.write('found multiple main measurement parameters ({}) for this sensor: {}. All but {} will be discarded!\n'.format(main_parameters, source_name, main_parameters[0]))
        f.close()
        parameter_value=(main_parameters[0])
    else:
        parameter_value=main_parameters[0]
    for parameter_value in main_parameters:    
        parameter_data=all_sensor_data[all_sensor_data['parameter']==parameter_value]

        parameter_data['timestamp']=pd.to_datetime(parameter_data['timestamp'])
        fig=px.scatter(parameter_data, x='timestamp', y='value', title="{}_{}".format(source_name, parameter_value))
        if not os.path.exists(os.path.join(save_directory, 'timeseries')):
            os.makedirs(os.path.join(save_directory, 'timeseries'))
        fig.write_html(os.path.join(save_directory, 'timeseries', '{}_{}.html'.format(source_name, parameter_value)))

def calculate_PSR(source_name, start_date,  resolution='M', allow_higher_samplingrates=True, thresh_drop_values=-100000, end_date='' ):
    '''
    function to find the PSR of a sensor. Here PSR is defined as the amount of received packages divided by the expected amount of sent packages if the 
    transmission was allways successfull.
    '''
    dp=DataPool()
    #get all sensor data from the database
    if end_date!='':
        all_sensor_data = dp.signal.get(source_name=source_name, start=start_date, end=end_date, minimal=False, show_query=False, to_dataframe=True)
    else:
        all_sensor_data = dp.signal.get(source_name=source_name, start=start_date, minimal=False, show_query=False, to_dataframe=True)
    #we are only interested in the sensors actual value
    all_parameters=all_sensor_data['parameter'].unique()
    main_parameters=extract_main_parameter(all_parameters)
    if len(main_parameters)<1:
        warnings.warn('could not find a main measurement parameter for this sensor: {}.'.format(source_name))
        return None
    elif len(main_parameters)>1:
        warnings.warn('found multiple main measurement parameters ({}) for this sensor: {}. All but {} will be discarded!'.format(main_parameters, source_name, main_parameters[0]))
        parameter_value=(main_parameters[0])
    else:
        parameter_value=main_parameters[0]
    parameter_data=all_sensor_data[all_sensor_data['parameter']==parameter_value]

    #convert to datetime
    parameter_data['timestamp']=pd.to_datetime(parameter_data['timestamp'])

    #find the most comon timedelta between consecutive packages
    most_common_timedelta=find_most_common_timedelta(parameter_data)

    if allow_higher_samplingrates==False:
        frequency=str(most_common_timedelta)+'min'
        parameter_data=downsampling_faulty_sensors2(parameter_data, frequency=frequency)

    parameter_data=parameter_data.drop(parameter_data[parameter_data.value<thresh_drop_values].index)
        
    #add a column with the difference in days of current and previous date:
    parameter_data['day_difference']=((parameter_data['timestamp'].dt.floor(freq = 'D'))-(parameter_data['timestamp'].shift(1).dt.floor(freq = 'D')))/np.timedelta64(1, 'D')

    #groupby and count
    grouped_data=parameter_data.groupby(pd.Grouper(key='timestamp', axis=0, 
                      freq=resolution)).agg({'day_difference':'sum', 'value':'count'}).reset_index()
    
    
    #normalize the count
    grouped_data['normalized_count']=grouped_data.apply(normalize_sent_packages, most_common_timedelta=most_common_timedelta, axis=1)

    #add the sensor name
    grouped_data['sensor']=source_name

    #we are only interested in one count and dont need the same information multiple times
    grouped_data_minimal=grouped_data[['timestamp', 'sensor', 'normalized_count']]

    return grouped_data_minimal

def filter_db_for_boxplot(sensor_group, start, parameter_unit, keyword):
    ''' 
    querry to filter the the database to plot the boxplots.
    For most types of the sensors, this query is "good enough", ie. filtering for a sensor type
    combined with the sensors parameter unit will filter the database for comparable sensors.
    However in some cases we need more filtering: Most bt sensors measure two temperatures. 
    We have to filter more in these cases - TODO! maybe give an option in the function? 
    '''
    dp=DataPool()
    data_dataframe = dp.query_df(
    f'''
    SELECT  t_signal.value, t_signal.timestamp, t_signal.parameter_id, t_signal.source_id, t_source.name as source_name, t_parameter.name AS parameter_name, t_parameter.unit
    FROM signal AS t_signal
    LEFT JOIN source AS t_source
        ON t_signal.source_id = t_source.source_id
    LEFT JOIN parameter AS t_parameter
        ON t_signal.parameter_id = t_parameter.parameter_id
    WHERE LEFT(t_source.name, 2 ) = '{sensor_group}' AND t_signal.timestamp > '{start}' AND t_parameter.unit='{parameter_unit}'
    '''
    )
    return data_dataframe

def filter_db_multiple_parameters(sensor_group, start, parameter_units):
    '''
    querry to filter the the database to plot the boxplots.
    For most types of the sensors, this query is "good enough", ie. filtering for a sensor type
    combined with the sensors parameter unit will filter the database for comparable sensors.
    However in some cases we need more filtering: Most bt sensors measure two temperatures. 
    We have to filter more in these cases - TODO! maybe give an option in the function? 
    '''
    dp=DataPool()
    data_dataframe = dp.query_df(
    f'''
    SELECT  t_signal.value, t_signal.timestamp, t_signal.parameter_id, t_signal.source_id, t_source.name as source_name, t_parameter.name AS parameter_name, t_parameter.unit
    FROM signal AS t_signal
    LEFT JOIN source AS t_source
        ON t_signal.source_id = t_source.source_id
    LEFT JOIN parameter AS t_parameter
        ON t_signal.parameter_id = t_parameter.parameter_id
    WHERE LEFT(t_source.name, 2 ) = '{sensor_group}' AND t_signal.timestamp > '{start}' AND t_parameter.unit in {parameter_units}
    '''
    )
    return data_dataframe

def filter_db_for_boxplot2(sensor_group, start, parameter_unit, keyword):
    ''' 
    querry to filter the the database to plot the boxplots.
    For most types of the sensors, this query is "good enough", ie. filtering for a sensor type
    combined with the sensors parameter unit will filter the database for comparable sensors.
    However in some cases we need more filtering: Most bt sensors measure two temperatures. 
    We have to filter more in these cases - TODO! maybe give an option in the function? 
    '''
    dp=DataPool()
    data_dataframe = dp.query_df(
    f'''
    SELECT  date_trunc('hour', t_signal.timestamp) AS timestamp, avg(t_signal.value) AS value, 
        t_source.name as source_name, 
        t_parameter.name AS parameter_name, t_parameter.unit
    FROM signal AS t_signal
    LEFT JOIN source AS t_source
        ON t_signal.source_id = t_source.source_id
    LEFT JOIN parameter AS t_parameter
        ON t_signal.parameter_id = t_parameter.parameter_id
    WHERE LEFT(t_source.name, 2 ) = '{sensor_group}' AND t_signal.timestamp > '{start}' AND t_parameter.unit='{parameter_unit}'
    GROUP BY(1, t_source.name, parameter_name, t_parameter.unit)
    '''
    )
    return data_dataframe


def plot_boxplot(data, coloring_attribute, grouping_attribute, title,   img_name, save_directory='', showOutliers=True, y_lims=None, ):
    '''
    function that plots the boxplot. Will probably leave the part out where db is filtered

    Parameters
    ----------
    data : pandas dataframe
        dataframe that contains all necessary data for the plot. Must contain following columns:
        'timestamp' -->  year, and month will be extracted from timestamp and used for grouping the x-axis
        'value'  --> will be used for y axis
        name of grouping attribute --> for x --> axis
        name of coloring attribute --> for subgrouping using colors
    coloring_attribute: str
        defines which colum is used for subgrouping the plot by colors.
    grouping_attribute: str
        defines which column is used for grouping the in the x axis.
    title: str
        which title will be used
    save_directory: str
        directory where plot will be saved. 
    img_name: str
        name of img if it is saved. Plot will not be saved if save_directory==''
    '''

    #Extract year and month from timestamp
    data['timestamp']=pd.to_datetime(data['timestamp'])
    data['year'] = data['timestamp'].dt.year.astype(str)
    data['month'] = data['timestamp'].dt.month.astype(str)
    data['month'] = data['month'].apply(lambda x: '0'+x if len(x) ==1 else x)
    data['year-month'] = data['year']+'-'+data['month']
    data=data.sort_values('year-month')
    plt.clf()
    sns.set(font_scale=2) #set fontsize
    sns.set_style("whitegrid") #set stlye
    n_sources=len(data['source_name'].unique()) #find number of sources in specific group
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(40, 20)) #setup the plot
    if y_lims!=None:
        try:
            ax.set_ylim(bottom=y_lims[0], top=y_lims[1])
        except:
            print('could not set ylimits')
    box_plot = sns.boxplot(data=data, y='value', x=grouping_attribute, hue=coloring_attribute,ax = ax, showfliers=showOutliers)
    
    #add the legend
    plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    #turn on the ygrid
    box_plot.grid(axis='y')

    #rotate x_axis tick labels
    plt.xticks(rotation=90, ha='center')

    #get the labels and the handles for labling each box
    handles, labels = box_plot.get_legend_handles_labels()
    
    #make a dict that connects the label with the specific color in the plot
    color_label_dict={}
    for i, label in enumerate(labels):
        color_label_dict.update({label:handles[i].get_facecolor()})

    #loop through the boxes
    for c in box_plot.get_children():
        if type(c) == matplotlib.patches.PathPatch:
            #get correct label of box by matching it's color color in the legend.
            label_of_box = list(color_label_dict.keys())[list(color_label_dict.values()).index(c.get_facecolor())]
            
            #add a text at the correct position
            box_plot.text((c.get_extents().transformed(ax.transData.inverted()).extents[0]+c.get_extents().transformed(ax.transData.inverted()).extents[2])/2,
                c.get_extents().transformed(ax.transData.inverted()).extents[3]+1,label_of_box,
                horizontalalignment='center',size=10/len(color_label_dict)*12 ,color=c.get_facecolor(),weight='bold', rotation=90)
    fig.suptitle(title)

    if img_name != '':
        if not os.path.exists(save_directory):
            os.makedirs(save_directory)
        fig.savefig(os.path.join(save_directory, '{}.png'.format(img_name)), dpi=100, bbox_inches='tight' )

def generate_SelectMultipleWidget_for_boxplot(data, column_name):
    select=widgets.SelectMultiple(
        description='Select the parameters that should be compared',
        options=data[column_name].unique(),
        rows=len(data[column_name].unique()),
        layout=Layout(width='1000px'),
        style= {'description_width': 'initial'}
    )
    return select

def generate_ImgNameWidget():
    img_name=widgets.Text(
        description='Enter name of image:',
        disabled=False,
        layout=Layout(width='1000px'),
        style= {'description_width': 'initial'}
    )
    return img_name

def generate_SavePathWidget():
    path_name=widgets.Text(
        description='Enter path were image will be saved',
        disabled=False,
        layout=Layout(width='1000px'),style= {'description_width': 'initial'}
    )
    return path_name

def generate_Slider_for_smoothing():
    slider=widgets.IntSlider(
        description='Set the width of the smoothign window (rolling mean).',
        layout=Layout(width='700px'),
        style= {'description_width': 'initial'},
        min=1,
        max=24*365,
        continuous_update=False
    )
    return slider

def generate_Slider_for_noise_filtering():
    slider=widgets.IntSlider(
        description='Set the width for noise filtering (rolling median)',
        layout=Layout(width='700px'),style= {'description_width': 'initial'},
        min=1,
        max=30,
        continuous_update=False
    )
    return slider

def generate_Options_for_normalizing():
    button=widgets.RadioButtons(
    options=['No', 'mean', 'median'],
    value='No',
    description='Do you want to normalize the curve?',
    layout=Layout(width='700px'),style= {'description_width': 'initial'},
    disabled=False
    )
    return button

def generate_ylim(data):
    ylim=widgets.widgets.FloatRangeSlider(
        value=[min(data)-0.01*min(data), max(data)+0.01*max(data)],
        min=min(data)-0.01*min(data),
        max=max(data)+0.01*max(data),
        step=1,
        description='set the range of the y axis:',
        disabled=False,
        continuous_update=False,
        orientation='horizontal',
        readout=True,
        readout_format='.1f',
    )
    return ylim

def generate_accepted_values(data):
    ylim=widgets.widgets.FloatRangeSlider(
        value=[min(data), max(data)],
        min=min(data),
        max=max(data),
        step=1,
        description='set the range of accepted values. Measurements are bounded by the set limits :',
        layout=Layout(width='700px'),style= {'description_width': 'initial'},
        disabled=False,
        continuous_update=False,
        orientation='horizontal',
        readout=True,
        readout_format='.1f',
    )
    return ylim


def downsample_data(dataframe, timestamp_col_name='timestamp', groupby_att='source_name', resampling_freq='1H'):
    '''
    downsamples data of a dataframe. Can be used, if downsampling has to occur per group
    instead of th whole dataframe alltogheter.
    '''

    data=dataframe.set_index(timestamp_col_name)
    data.index=pd.to_datetime(data.index)
    data=data.groupby(groupby_att).resample(resampling_freq).mean().reset_index()
    return data

def filter_data2(data, valid_parameters, valid_sources):

    plot_data=data[data['parameter_name'].isin(valid_parameters)]
    plot_data=plot_data[plot_data['source_name'].isin(valid_sources)]

    return plot_data

def wait_until(somepredicate, timeout, period=0.25, *args, **kwargs):
  mustend = time.time() + timeout
  while time.time() < mustend:
    if somepredicate(*args, **kwargs): return True
    time.sleep(period)
  return False

def plot_flattened_signal(data, group_att='source_name', value_column='value', 
                            valid_parameters='', valid_sources='',
                            timestamp_column='timestamp', median_win_size=3, mean_win_size=24, 
                            normalize=None, min_max=[0,100]):
    
    plotdata=filter_data2(data, valid_parameters, valid_sources)
    #plotdata=downsample_data(plotdata)

    min_allowed_value=min_max[0]
    max_allowed_value=min_max[1]
    fig, ax = plt.subplots(figsize=(25, 10))

    for source in list(plotdata[group_att].unique()):

        plotdata_filtered=plotdata[plotdata['source_name']==source].sort_values('timestamp')
        
        #filtering the signal:
        plotdata_filtered['value_after_min_max']=plotdata_filtered[value_column].apply(lambda x: min_allowed_value if x < min_allowed_value else (max_allowed_value if x > max_allowed_value else x))
        plotdata_filtered['value_after_median']=plotdata_filtered['value_after_min_max'].rolling(window=median_win_size, center=True).median()
        plotdata_filtered['trend']=plotdata_filtered['value_after_median'].rolling(window=mean_win_size, center=True).mean()

        if normalize == 'mean':
            plotdata_filtered['trend']=plotdata_filtered['trend']/plotdata_filtered['value'].mean()

        if normalize == 'median':
            plotdata_filtered['trend']=plotdata_filtered['trend']/plotdata_filtered['value'].median()

        
        lineplot=sns.lineplot(data=plotdata_filtered, x='timestamp', y='trend', ax=ax, label=source)
        #lineplot=sns.scatterplot(data=plotdata_filtered, x='timestamp', y='trend', ax=ax, label=source)

def plot_QH_relation(source):
    fig, ax = plt.subplots(figsize=(15, 15))
    relation_plot=sns.scatterplot(data=source, x='flowrate', y='m')
    

