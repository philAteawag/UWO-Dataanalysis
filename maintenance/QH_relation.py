import numpy as np

from matplotlib.widgets import LassoSelector
from matplotlib.path import Path
from datapool_client import DataPool
import sys
sys.path.append(r'C:\Users\steineph\DataAnalysis\UWO-Dataanalysis\libs')
from UWO_DataAnalysis import helper_functions
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from scipy import stats
import numpy as np
import argparse
import json
import os



class SelectFromCollection:
    """
    Select indices from a matplotlib collection using `LassoSelector`.

    Selected indices are saved in the `ind` attribute. This tool fades out the
    points that are not part of the selection (i.e., reduces their alpha
    values). If your collection has alpha < 1, this tool will permanently
    alter the alpha values.

    Note that this tool selects collection objects based on their *origins*
    (i.e., `offsets`).

    Parameters
    ----------
    ax : `~matplotlib.axes.Axes`
        Axes to interact with.
    collection : `matplotlib.collections.Collection` subclass
        Collection you want to select from.
    alpha_other : 0 <= float <= 1
        To highlight a selection, this tool sets all selected points to an
        alpha value of 1 and non-selected points to *alpha_other*.
    """

    def __init__(self, ax, collection, alpha_other=0.1):
        self.canvas = ax.figure.canvas
        self.collection = collection
        self.alpha_other = alpha_other

        self.xys = collection.get_offsets()
        self.Npts = len(self.xys)

        # Ensure that we have separate colors for each object
        self.fc = collection.get_facecolors()
        if len(self.fc) == 0:
            raise ValueError('Collection must have a facecolor')
        elif len(self.fc) == 1:
            self.fc = np.tile(self.fc, (self.Npts, 1))

        self.lasso = LassoSelector(ax, onselect=self.onselect)
        self.ind = []

    def onselect(self, verts):
        path = Path(verts)
        self.ind = np.nonzero(path.contains_points(self.xys))[0]
        self.fc[:, -1] = self.alpha_other
        self.fc[self.ind, -1] = 1
        self.collection.set_facecolors(self.fc)
        self.canvas.draw_idle()

    def disconnect(self):
        self.lasso.disconnect_events()
        self.fc[:, -1] = 1
        self.collection.set_facecolors(self.fc)
        self.canvas.draw_idle()

def main(source_name, start='2021-01-01', save_directory='' ):
    
    data_q=helper_functions.filter_db_for_boxplot(sensor_group='bf', start=start, parameter_unit='l/s', keyword='' ).set_index('timestamp')
    data_h=helper_functions.filter_db_for_boxplot(sensor_group='bf', start=start, parameter_unit=('mm'), keyword='' ).set_index('timestamp')
    data_h=data_h[data_h['parameter_name']=='water_level']
       
    joined_df=new_df = pd.merge(data_q, data_h,  how='inner', on=['timestamp','source_name'], suffixes=('_q', '_h'))
    joined_df.index=pd.to_datetime(joined_df.index)
    
    data_minimal=joined_df[joined_df['source_name']==source_name][['value_h', 'value_q']]

    if len(data_minimal.index)==0:
        print('provided source does not contain both mesaurements (l/s and mm). Please select another source')
        return 0
    data_minimal_filtered = data_minimal[(np.abs(stats.zscore(data_minimal)) < 3).all(axis=1)]
    repeat_function='r'
    while repeat_function=='r':

        subplot_kw = dict(xlim=(np.min(np.array(data_minimal)[:, 0])-10, np.max(np.array(data_minimal)[:, 0])+10), ylim=(np.min(np.array(data_minimal)[:, 1])-10, np.max(np.array(data_minimal)[:, 1])+10), autoscale_on=False)
        fig, ax = plt.subplots(subplot_kw=subplot_kw)

        pts = ax.scatter(np.array(data_minimal)[:, 0], np.array(data_minimal)[:, 1], s=10)
        selector = SelectFromCollection(ax, pts)

        def accept(event):
            '''
            Function to accept drawed lasso
            '''
            if event.key == "enter":
                trend = np.polyfit(np.ma.getdata(selector.xys[selector.ind])[:,0], np.ma.getdata(selector.xys[selector.ind])[:,1], 3)
                trendpoly = np.poly1d(trend) 
                xp = np.linspace(-2, 500, 100)
                plot_fit = sns.lineplot(x=xp,y=trendpoly(xp), label = 'fitted 3rd degree curve', ax=ax, color='orange')
                discarded_data_x = np.delete(np.ma.getdata(selector.xys)[:,0], selector.ind,)
                discarded_data_y = np.delete(np.ma.getdata(selector.xys)[:,1], selector.ind,)
                
                discarded_data_plot = sns.scatterplot(x=discarded_data_x, y= discarded_data_y, label = 'discarded data', ax=ax, color='red')
                used_data_plot = sns.scatterplot(x=np.ma.getdata(selector.xys[selector.ind])[:,0], y= np.ma.getdata(selector.xys[selector.ind])[:,1], label = 'used data', ax=ax, color='blue')
                selector.disconnect()
                ax.set_title("fitted curve. Close Plot, then: \n Click on cmd window\nPress s (and ENTER) to save the curve parameters\nPress r (and ENTER) to reselect the data\nPress ENTER to finish")
                fig.canvas.draw()
        
        def get_curve():
            trend = np.polyfit(np.ma.getdata(selector.xys[selector.ind])[:,0], np.ma.getdata(selector.xys[selector.ind])[:,1], 3)
             
            return trend

        fig.canvas.mpl_connect("key_press_event", accept)
        ax.set_title("Encircle data that should be used for polyfit. Press enter Once your satisifed")

        plt.show()
        trend = get_curve ()

        repeat_function = input("Press s (and ENTER) to save the curve parameters\nPress r (and ENTER) to reselect the data\nPress ENTER to finish\nChoice: ")  # or raw_input in python2
        if repeat_function == "s":
            if not os.path.exists(save_directory):
                os.makedirs(save_directory)
            print(f"Saved curve at {os.path.join(save_directory,'qh_curve_params.json')}")
            curve_dict = {'source_name':source_name, 'Curve parameter': list(trend), 'Q(height)': f'{trend[0]}h3+{trend[1]}x2+{trend[2]}x+{trend[0]}'}
            # Serializing json

            if os.path.exists(os.path.join(save_directory, 'qh_curve_params.json')):
                with open("qh_curve_params.json", "r") as infile:
                    json_object=json.load(infile)
                json_object[source_name]=curve_dict
                with open(os.path.join(save_directory, "qh_curve_params.json"), "w") as outfile:
                    json.dump(json_object, outfile)

            else:
                with open(os.path.join(save_directory, 'qh_curve_params.json'), "w") as outfile:
                    json_object={source_name: {'source_name':source_name, 'Curve parameter': list(trend), 'Q(height)': f'{trend[0]}h3+{trend[1]}x2+{trend[2]}x+{trend[0]}'}}
                    json.dump(json_object, outfile)
                    
            
        elif repeat_function=='r':
            print("Redo curve fitting")
        
        else: 
            print('did not save the curve')
        plt.close('all')
        

def parse_arguments():
    parser = argparse.ArgumentParser(description='Select the data to find QH relation')
    parser.add_argument("source", type=str, help='Select a source that has both: measurement of l/s (q) and mm (height)')
    parser.add_argument("--start_date", default='2021-01-01', help='start date of sesor data.')
    parser.add_argument("--save_directory", default = '', help= 'directory where curve parameters will be stored')
    args = parser.parse_args()
    
    return args

if __name__ == '__main__':
   
    arguments = parse_arguments()
    main(source_name=arguments.source, start=arguments.start_date, save_directory=arguments.save_directory)
    