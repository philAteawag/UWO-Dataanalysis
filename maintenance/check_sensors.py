# -*- coding: utf-8 -*-


import datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd
import tqdm
from datapool_client import DataPool
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from scipy import stats
from scipy.stats import ks_2samp

from . import helper_functions


class Check_Signal_Sensor:
    # '''
    # Class to check the current Signal for validity. follwoing steps are conducted:
    #     1. a config file has the information which sensors measure similiar values. The config file does not exist yet...! See here
    #         how it should be constructed: C:\Users\steineph\DataAnalysis\UWO-Dataanalysis\config_concept.json
    #     2. Using the config file, we take the measurements of two similiar sensors, and calculate the relative difference of the measurements.
    #         2.1 both timeseries are resampled to a frequency of 1h
    #         2.2 we apply a moving average filter of 24h (window size of 24) to smoothen the timeseries.
    #         2.3 we standardize both timeseries
    #         2.4 we join the two timeseries on the same timestamp
    #         2.5 we calcualte the absolute and the realitve difference at every timestamp.
    #     3. We can than compare the difference of the two signals over the last week to the difference of the two signals over the last 16weeks.
    #         To do so we can compare wheter those two distributions are "similar" by calculating the two sided Kolmogorov-Smirnov (K-S) statistic
    #         of the two distributions. If the statistic gives a high P value, it means that the two samples
    #         (samples of last week and samples of last 16 weeks) arrose from a different underlying distribution
    #         --> This could then mean that one sensor is now making faulty measurements.

    # Attributes
    # ----------
    # config_data : json
    #     data that contains the information, which other sensor produces very similar values and which parameter is looked at
    # sourec_to_check : str
    #     the name of the source that is checked
    # source_to_check_against : str
    #     the source that has similiar measurements
    # parameter_name : str
    #     the parameter that is checked
    # signal_to_check_current : pandas dataframe
    #     signal of the last week
    # signal_to_check_historic pandas DataFrame
    #     singal of the last 4 month (the newest week is excluded)
    # signal_to_check_against_current : pandas DataFrame
    #     signal of the sensor that shows similiar measurements of the most recent week
    # signal_to_check_against_historic : pandas DataFrame
    #     signal of the sensor that shows similiar measurements of the last 4 month (most rececent week is excluded)
    # diff_historic_rel : pandas Series
    #     relativ difference of the sensor data compared to measurement values of a similar sensor
    #     (data collected over the last 16 weeks (most current one excluded))
    # diff_historic_abs : pandas Series
    #     absolut difference of the sensor data compared to measurement values of a similar sensor
    #     (data collected over the last 16 weeks (most current one excluded))
    # diff_current_rel : pandas Series
    #     relativ difference of the sensor data compared to measurement values of a similar sensor
    #     (data collected over most current week)
    # diff_current_abs : pandas Series
    #     absolut difference of the sensor data compared to measurement values of a similar sensor
    #     (data collected over most current week)
    # ks_test : KS statistic and P value
    #     The KS statistic is used to compare the distribution of the difference of the tow signals of the last week, and the of the last
    #     16 weeks.
    #         The KS statistic gives an estimate how well the distributions fit each other. If the value is 1, there is a perfect fit.
    #         If it is a 0 it is a terible fit.
    #         The P-value gives an estimate how likely it is that we observe such a fit.
    #         In short: if the KS statistic is low and the P value is low, we can assume, that the two distributions are not the same
    #         and therefore, that one sensor has faulty measurements.

    # Methods
    # -------
    # get_dif_between2signals
    #     takes two signals, joins them on the same timestamp, standardizes them and takes the difference.
    # '''
    def __init__(self, source_to_check, config_file):
        # '''
        # Parameters
        # ----------
        # source_to_check : str
        #     source_name which signal should be checked
        # init_file : json
        #     json file containing the information which main parameter is used for the checking and also which sensor shows similiar sensor data
        # '''
        self.config_data = self.load_config_json(config_file)
        self.source_to_check = source_to_check
        self.source_to_check_against = self.config_data[self.source_to_check][
            "similar_to"
        ]
        print(self.source_to_check_against)
        self.parameter_name = self_data[self.source_to_check]["main_parameter"]
        (
            self._current_date,
            self._date1weekago,
            self._date4monthago,
        ) = self.get_current_and_date4monthago()
        self.signal_to_check_current = self.load_signal(
            self.source_to_check,
            start=self._date1weekago,
            end=self._current_date,
            parameter_name=self.parameter_name,
        )
        self.signal_to_check_historic = self.load_signal(
            self.source_to_check,
            start=self._date4monthago,
            end=self._date1weekago,
            parameter_name=self.parameter_name,
        )
        self.signal_to_check_against_current = self.load_signal(
            self.source_to_check_against,
            start=self._date1weekago,
            end=self._current_date,
            parameter_name=self.parameter_name,
        )
        self.signal_to_check_against_historic = self.load_signal(
            self.source_to_check_against,
            start=self._date4monthago,
            end=self._date1weekago,
            parameter_name=self.parameter_name,
        )
        self.diff_historic_rel, self.diff_historic_abs = self.get_dif_between2signals(
            df1=self.signal_to_check_historic, df2=self.signal_to_check_against_historic
        )
        self.diff_current_rel, self.diff_current_abs = self.get_dif_between2signals(
            df1=self.signal_to_check_current, df2=self.signal_to_check_against_current
        )
        self.ks_test = ks_2samp(self.diff_current_rel, self.diff_historic_rel)

    today = datetime.date.today()
    # To get a good estimate of this weeks psr, we include all data until the previous day until 23:59:59
    yesterday = today - datetime.timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d") + " 23:59:59"

    def load_signal(self, source, parameter_name, start="2022-01-01", end=""):
        dp = DataPool()
        if end != "":
            data_loaded = dp.signal.get(
                source_name=source, start=start, end=end, parameter_name=parameter_name
            )
        else:
            data_loaded = dp.signal.get(
                source_name=source, start=start, end=end, parameter_name=parameter_name
            )
        data_loaded = helper_functions.downsample_data(
            data_loaded, groupby_att="source"
        )

        return data_loaded

    def load_config_json(self, path_to_config):
        path = Path(path_to_config)
        file = open(path)
        data = json.load(file)
        return data

    def get_current_and_date4monthago(self):
        today = datetime.date.today()

        last_week = today - datetime.timedelta(weeks=1)
        last_week = last_week.strftime("%Y-%m-%d")

        last_4month = today - datetime.timedelta(weeks=16)
        last_4month = last_4month.strftime("%Y-%m-%d")

        today = today.strftime("%Y-%m-%d")

        return (today, last_week, last_4month)

    def get_dif_between2signals(
        self,
        df1,
        df2,
        timestamp_col1="timestamp",
        timestamp_col2="timestamp",
        value_col1="value",
        value_col2="value",
        standardize=True,
    ):
        # '''
        # takes two signals, joins them on the same timestamp, standardizes them and takes the difference.

        # Parameters
        # ----------
        # df1 : pandas DataFrame
        #     first dataframe
        # df2 : pandas DataFrame
        #     second dataframe
        # timestamp_col1 : str
        #     name of column in df1 containing timestamp info
        # timestamp_col2 : str
        #     name of column in df2 containin timestamp info
        # value_col1 : str
        #     name of column in df1 containing value info
        # value_col2 : str
        #     name of column in df2 contatining value info
        # standardize : bool
        #     wheter or not data is standardized (x-mean(x))/std(x))
        # '''
        data1 = df1[[timestamp_col1, value_col1]].set_index(timestamp_col1)
        data2 = df2[[timestamp_col2, value_col2]].set_index(timestamp_col2)

        dataToCompare = pd.merge(
            data1,
            data2,
            left_index=True,
            right_index=True,
            how="inner",
            suffixes=("_x", "_y"),
        )
        if standardize == True:
            dataToCompare = (dataToCompare - dataToCompare.mean()) / dataToCompare.std()
        dataToCompare = dataToCompare.rolling(window=24).mean()
        dataToCompare["abs_difference"] = (
            dataToCompare["value_x"] - dataToCompare["value_y"]
        )
        dataToCompare["rel_difference"] = (
            dataToCompare["abs_difference"] / dataToCompare["value_y"]
        )
        return (dataToCompare["rel_difference"], dataToCompare["abs_difference"])


class PSR_Sensor:
    def __init__(self, source_name, start_date, end_date):
        self.old_mean = 0
        self.source_name = source_name
        self.loaded_data_since = start_date
        self.loaded_data_upto = end_date
        self.previous_PSR = self.calculate_previous_weekly_PSR(
            source_name=source_name, start_date=start_date, end_date=end_date
        )
        self.data = self.previous_PSR

    def calculate_previous_weekly_PSR(self, source_name, start_date, end_date):
        previous_PSR = helper_functions.calculate_PSR(
            source_name=source_name,
            start_date=start_date,
            end_date=end_date,
            resolution="W",
            allow_higher_samplingrates=False,
        )
        return previous_PSR

    def calculate_z_score_PSR(self):
        psr_df = self.data

        # replace inf values with nan
        psr_df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # drop nan values
        psr_df = psr_df.dropna()

        # make two tables. First stores all psr of previous weeks, second one stores psr of current (last recorded ) week
        psr_previous = psr_df.iloc[0:-1]
        psr_current = psr_df.iloc[-1]

        # we drop the psr values of previous weeks if they were strong outliers
        psr_previous["z_score"] = stats.zscore(psr_previous["normalized_count"])
        psr_df_filtered = psr_previous[psr_previous["z_score"] > -3]

        # calculate the mean value of PSR over the last couple of weeks (disregarding the current week) and update variable
        self.old_mean = np.mean(psr_df_filtered["normalized_count"])

        # now add the current week to the table again
        # psr_df_filtered=pd.concat([psr_df_filtered,psr_current])
        psr_df_filtered = psr_df_filtered.append(psr_current)

        # calculate the z score
        psr_df_filtered["z_score"] = stats.zscore(psr_df_filtered["normalized_count"])
        self.data = psr_df_filtered
        return psr_df_filtered


class Sensor_Report(PdfPages):
    def __init__(self, filename):
        self.current_page = 0
        super().__init__(filename=filename)

    def df_to_pdf(self, df, table_info):
        # '''
        # converts df into matplotlib table which can than be converted into a pdf

        # Parameters
        # ----------
        # df : pandas DataFrame
        #     the dataframe that should be converted into a pdf
        # table_info : str
        #     Table info that will be plotted on top of the table
        # numb_pages : int
        #     Number at which the report starts.
        #     Need this as we want to repeatedly add pages and tables to the report
        # '''

        total_rows, total_cols = df.shape

        rows_per_page = 30  # Number of rows per page
        rows_printed = 0
        page_number = self.current_page
        while total_rows > 0:
            fig = plt.figure(figsize=(8.5, 11))
            plt.gca().axis("off")
            matplotlib_tab = pd.plotting.table(
                plt.gca(),
                df.iloc[rows_printed : rows_printed + rows_per_page],
                loc="upper center",
                colWidths=[0.15] * total_cols,
            )
            # Tabular styling
            table_props = matplotlib_tab.properties()
            table_cells = table_props["children"]
            # matplotlib_tab.auto_set_font_size(False)
            # matplotlib_tab.set_fontsize(4)
            for cell in table_cells:
                cell.set_height(0.024)
                cell.set_fontsize(5)
            # Header,Footer and Page Number
            fig.text(4.25 / 8.5, 10.5 / 11.0, table_info, ha="center", fontsize=12)
            fig.text(
                4.25 / 8.5, 0.5 / 11.0, "P" + str(page_number), ha="right", fontsize=8
            )
            self.savefig()
            plt.close()
            # Update variables
            rows_printed += rows_per_page
            total_rows -= rows_per_page
            page_number += 1
            self.current_page = page_number


def main():
    dp = DataPool()
    # loop thorugh sensors that sent signal the previous month:
    today = datetime.date.today()
    # To get a good estimate of this weeks psr, we include all data until the previous day until 23:59:59
    yesterday = today - datetime.timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d") + " 23:59:59"

    # we compare it to the PSR over the last 16 months
    last_4month = today - datetime.timedelta(weeks=16)
    last_4month = last_4month.strftime("%Y-%m-%d")

    # we make three different tables:
    skipped_sensors = pd.DataFrame(columns=["source_name"])
    suspicious_sensors = pd.DataFrame(
        columns=[
            "source_name",
            "last_recorded_PSR",
            "mean of last 4 months",
            "Last weeks PSR",
            "z_score",
        ]
    )
    unsuspicious_sensors = pd.DataFrame(
        columns=[
            "source_name",
            "last_recorded_PSR",
            "mean of last 4 months",
            "Last weeks PSR",
            "z_score",
        ]
    )
    counter = 0
    for sensor in tqdm.tqdm(dp.source.all()["name"].unique()):
        counter = counter + 1
        if counter < 10:
            try:
                sensor_to_check = PSR_Sensor(
                    sensor, start_date=last_4month, end_date=yesterday
                )
                sensor_to_check.calculate_z_score_PSR()
                # make decision wheter or not current PSR is suspicious
                if sensor_to_check.data.iloc[-1]["z_score"] < -2:
                    suspicious_sensor = pd.DataFrame(
                        columns=[
                            "source_name",
                            "last_recorded_PSR",
                            "mean of last 4 months",
                            "Last weeks PSR",
                            "z_score",
                        ],
                        data=[
                            [
                                sensor,
                                str(sensor_to_check.data.iloc[-1]["timestamp"])[0:10],
                                sensor_to_check.old_mean,
                                sensor_to_check.data.iloc[-1]["normalized_count"],
                                sensor_to_check.data.iloc[-1]["z_score"],
                            ]
                        ],
                    )
                    suspicious_sensors = pd.concat(
                        [suspicious_sensors, suspicious_sensor]
                    )
                else:
                    unsuspicious_sensor = pd.DataFrame(
                        columns=[
                            "source_name",
                            "last_recorded_PSR",
                            "mean of last 4 months",
                            "Last weeks PSR",
                            "z_score",
                        ],
                        data=[
                            [
                                sensor,
                                str(sensor_to_check.data.iloc[-1]["timestamp"])[0:10],
                                sensor_to_check.old_mean,
                                sensor_to_check.data.iloc[-1]["normalized_count"],
                                sensor_to_check.data.iloc[-1]["z_score"],
                            ]
                        ],
                    )
                    unsuspicious_sensors = pd.concat(
                        [unsuspicious_sensors, unsuspicious_sensor]
                    )

            except Exception as error:
                print(error)
                skipped_sensor = pd.DataFrame(columns=["source_name"], data=[sensor])
                skipped_sensors = pd.concat([skipped_sensors, skipped_sensor])

            suspicious_sensors = (
                suspicious_sensors.sort_values(
                    ["last_recorded_PSR", "Last weeks PSR"], ascending=[False, False]
                )
                .reset_index()
                .drop(columns="index")
            )
            suspicious_sensors = suspicious_sensors.round(3)
            unsuspicious_sensors = (
                unsuspicious_sensors.sort_values(
                    ["last_recorded_PSR", "Last weeks PSR"], ascending=[False, False]
                )
                .reset_index()
                .drop(columns="index")
            )
            unsuspicious_sensors = unsuspicious_sensors.round(3)
        else:
            break
    # make the report
    pp = Sensor_Report(f"PSR_Report_{today}.pdf")
    pp.df_to_pdf(
        suspicious_sensors,
        "Sensors which PSR are suspicious:\nPSR is more than 2 standard deviation lower\nthan mean over last 16 weeks",
    )
    pp.df_to_pdf(
        unsuspicious_sensors,
        "Sensors which PSR are not suspicious:\nPSR is less than 2 standard deviation lower\nthan mean over last 16 weeks",
    )
    pp.df_to_pdf(
        skipped_sensors,
        "Skipped Sensors:\nProbably because no measurements during\nlast 4 months",
    )
    pp.close()

    # '''
    # Next i wanted to do a check of the acutal signal. The Idea is as following:
    #     1. Create a config_file.json file that stores information which sensors show similiar measurements. The file here shows the concept:
    #         C:\Users\steineph\DataAnalysis\UWO-Dataanalysis\config_concept.json
    #         The file with all the measurement comparisons has yet to be made - I didnt manage. The file I created only shows the "correct" structure
    #     2. Loop through all sensors and do following (the sample code should work... however since my config file does only really hold the necessary
    #         information for this specific sensor, it is the only one that currently "works".):

    #             #do this for all important sensors! (To check whats happenign, go to the Check_Signal_Sensor class)
    #             signal_to_check = Check_Signal_Sensor(source_to_check='bt_dl917_162_luppmenweg')
    #             if (signal_to_check.ks_test[0] < 0.2) and (signal_to_check.ks_test[1] )<0.05:  #The two thresholds have to be defined!
    #                 print(f'{signal_to_check.source_to_check} is suspicious! Its difference to {signal_to_check_against} is significantly\n different to what it used to be (over the last 4 month))
    #             else:
    #                 print(f'{signal_to_check.source_to_check} is not suspicious!)
    #     3. Instead of printing these statements, put the calcuated statistics in a report similar to the PSR test!
    # '''


if __name__ == "__main__":
    main()
