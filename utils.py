import pandas as pd
import matplotlib.pyplot as plt
import os
from os import path
from python_ml_common.config import RedshiftConfig, load_envvars
from python_ml_common.loader.redshift import RedshiftLoader
from pathlib import Path


def loader():
    # Load env vars of TURO_REDSHIFT_USER and TURO_REDSHIFT_PASSWORD
    load_envvars()
    db_config = RedshiftConfig()
    db_config.username = os.getenv("TURO_REDSHIFT_USER")
    db_config.password = os.getenv("TURO_REDSHIFT_PASSWORD")

    # Initialize RedshiftLoader
    rs = RedshiftLoader(db_config)
    
    return rs

def load_data(sql_path, loader):
    # Load data into pd.DataFrame from sql_path
    with open(sql_path, 'r') as f:
        sql = f.read()
        df = loader.load(sql)
    
    return df

def update_paid_days(signup_month_start, channel, signup_value):
    increments_start = 1

    # I'd consider a groupby apply here (I can see the need to iterate progressively through increments
    #   , but could be more efficient to groupby apply on channels)
    while signup_month_start <= pd.to_datetime('2024-08-01'):
        channel_month_ind = (signup_value.channels == channel) & (signup_value.signup_month == signup_month_start)
        # Take the first unpopulated increment, this MIGHT run into issues for thin channels where we project 0 paid days, but probably fine
        increments_start = signup_value.loc[
            channel_month_ind & (signup_value.projected_paid_days == 0), 
            'increments_from_signup'].min()

        while increments_start <= 12:
            ## For projection periods <= 18, update projected_paid_days (t) with projected_paid_days (t-1) * ds_curve (t)
            # Young man this line is far too long! Break it up! Also please add more comments (I know we're working fast, but it'll save you time later)

            # Simpler to get the filtered ind first, then get locs
            
            prev_paid_days = signup_value.loc[
                channel_month_ind & (signup_value.increments_from_signup == increments_start-1),
                'projected_paid_days'].values[0]
            ds_curve = signup_value.loc[
                channel_month_ind & (signup_value.increments_from_signup == increments_start), 
                'ds_curve'].values[0]
            signup_value.loc[
                channel_month_ind & (signup_value.increments_from_signup == increments_start)
                , 'projected_paid_days'] = prev_paid_days*ds_curve

            increments_start += 1
        
        signup_month_start = signup_month_start + pd.DateOffset(months=1)

def cpd_accuracy_channel_plot(df, size=(13, 80)):
    channels = df.channels.unique() # payback channels included in df
    num_plots = len(df.channels.unique())

    # list of metrics, colors, labels to utilize in each channel plot
    metrics = ['cpd_forecast_v2', 'cpd_forecast_v1', 'w_cpd_actual']
    colors = ['green', 'lightgreen', 'blue']
    names = ['New CPD Forecast', 'Old CPD Forecast', 'Actual CPD']

    _, axes = plt.subplots(num_plots, 1, figsize = size)
    plt.subplots_adjust(hspace=0.5)
    plt.suptitle('Overall CPD Accuracy by Channel', fontsize=16, fontweight='bold')

    for ax, channel in zip(axes, channels):
        for metric, color, name in zip(metrics, colors, names):
            ax.plot('forecast_month', metric, '--' if metric!='w_cpd_actual' else '-', label=name, color=color, marker='.', data=df.loc[df.channels==channel])
        
        ax1 = ax.twinx()
        ax1.bar(x = 'forecast_month', height = 'data_volume_y', label = 'Num Trips (Actual)', 
                width=20, color='blue', alpha=0.2,
                data = df.loc[df.channels==channel])
        ax.set_title(channel, fontweight='bold')
        ax.set_ylabel('CPD ($)')
        ax.set_xlabel('signup month')
        ax1.set_ylabel('# trips observed')
        
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax1.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, bbox_to_anchor=(1.1, 1), loc='upper left', borderaxespad=0.)

    plt.show()

def nrpd_accuracy_channel_plot(df):
    channels = df.channels.unique() # payback channels included in df
    num_plots = len(df.channels.unique())

    # list of metrics, colors, labels to utilize in each channel plot
    metrics = ['w_nrpd_forecast_v2', 'w_nrpd_forecast_v1', 'w_nrpd_actual']
    colors = ['green', 'lightgreen', 'blue']
    names = ['New NRPD Forecast', 'Old NRPD Forecast', 'Actual NRPD']

    _, axes = plt.subplots(num_plots, 1, figsize = (13,80))
    plt.subplots_adjust(hspace=0.5)

    for ax, channel in zip(axes, channels):
        for metric, color, name in zip(metrics, colors, names):
            ax.plot('forecast_month', metric, '--' if metric!='w_nrpd_actual' else '-', label=name, color=color, marker='.', data=df.loc[df.channels==channel])
        
        ax1 = ax.twinx()
        ax1.bar(x = 'forecast_month', height = 'data_volume_y', label = 'Num Trips (Actual)', 
                width=20, color='blue', alpha=0.2,
                data = df.loc[df.channels==channel])
        ax.set_title(channel, fontweight='bold')
        ax.set_ylabel('NRPD ($)')
        ax.set_xlabel('signup month')
        ax1.set_ylabel('# trips observed')
        
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax1.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, bbox_to_anchor=(1.1, 1), loc='upper left', borderaxespad=0.)

    plt.show()

def plot_monaco_forecasts(monaco_df, monaco_segment, size=(13, 80)):
    channels = monaco_df.channels.unique()  # payback channels included in df
    num_plots = len(channels)

    # list of metrics, colors, labels to utilize in each channel plot
    metrics = ['monaco_forecast_v2', 'monaco_forecast_v1', 'monaco_actual']
    colors = ['green', 'lightgreen', 'blue']
    names = ['Forecast V2', 'Forecast V1', 'Actual']

    _, axes = plt.subplots(num_plots, 1, figsize=size)
    plt.subplots_adjust(hspace=0.5)  # Increase the vertical space between plots
    plt.suptitle('Monaco Share Accuracy by Channel {monaco}'.format(monaco = monaco_segment), fontsize=16, fontweight='bold')

    for ax, channel in zip(axes, channels):
        for metric, color, name in zip(metrics, colors, names):
            ax.plot('forecast_month', metric, '--' if metric != 'monaco_actual' else '-', label=name, color=color, marker='.', data=monaco_df.loc[(monaco_df.channels == channel) & (monaco_df.monaco_bin == monaco_segment)])
        
        ax.set_title(f'Channel: {channel}', fontweight='bold')
        ax.set_ylabel('Values')
        ax.set_xlabel('Forecast Month')
        ax.legend(bbox_to_anchor=(1.1, 1), loc='upper left', borderaxespad=0.)

    plt.show()

def cpd_monaco_accuracy_channel_plot(df, monaco_segment, size=(13, 80)):
    channels = df.channels.unique() # payback channels included in df
    num_plots = len(df.channels.unique())

    # list of metrics, colors, labels to utilize in each channel plot
    metrics = ['cpd_raw_forecast', 'w_cpd_raw_actual']
    colors = ['green', 'blue']
    names = ['CPD per segment Forecast ', 'Actual CPD per segment']

    _, axes = plt.subplots(num_plots, 1, figsize = size)
    plt.subplots_adjust(hspace=0.5)
    plt.suptitle('Per Segment CPD Accuracy by Channel {monaco}'.format(monaco = monaco_segment), fontsize=16, fontweight='bold')

    for ax, channel in zip(axes, channels):
        for metric, color, name in zip(metrics, colors, names):
            ax.plot('forecast_month', metric, '--' if metric!='w_cpd_raw_actual' else '-', label=name, color=color, marker='.', data=df.loc[(df.channels==channel) & (df.monaco_bin == monaco_segment)])
        
        ax1 = ax.twinx()
        ax1.bar(x = 'forecast_month', height = 'data_volume_y', label = 'Num Trips (Actual)', 
                width=20, color='blue', alpha=0.2,
                data = df.loc[(df.channels==channel) & (df.monaco_bin == monaco_segment)])
        ax.set_title(channel, fontweight='bold')
        ax.set_ylabel('CPD per Monaco Segment ($)')
        ax.set_xlabel('signup month')
        ax1.set_ylabel('# trips observed')
        
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax1.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, bbox_to_anchor=(1.1, 1), loc='upper left', borderaxespad=0.)

    plt.show()