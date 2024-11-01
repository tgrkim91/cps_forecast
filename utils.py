import pandas as pd
import matplotlib.pyplot as plt
import os
from python_ml_common.config import RedshiftConfig, load_envvars
from python_ml_common.loader.redshift import RedshiftLoader


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
    with open(sql_path, "r") as f:
        sql = f.read()
        df = loader.load(sql)

    return df


def update_paid_days(signup_month_start, channel, signup_value):
    signup_value.sort_values(
        ["channels", "signup_month", "increments_from_signup"],
        inplace=True,
        ignore_index=True,
    )
    increments_start = 1

    # I'd consider a groupby apply here (I can see the need to iterate progressively through increments
    #   , but could be more efficient to groupby apply on channels)
    while signup_month_start <= pd.to_datetime("2024-11-01"):
        channel_month_ind = (signup_value.channels == channel) & (
            signup_value.signup_month == signup_month_start
        )
        # Take the first unpopulated increment, this MIGHT run into issues for thin channels where we project 0 paid days, but probably fine
        increments_start = signup_value.loc[
            channel_month_ind & (signup_value.projected_paid_days == 0),
            "increments_from_signup",
        ].min()

        while increments_start <= 12:
            ## For projection periods <= 12, update projected_paid_days (t) with projected_paid_days (t-1) * ds_curve (t)

            prev_paid_days = signup_value.loc[
                channel_month_ind
                & (signup_value.increments_from_signup == increments_start - 1),
                "projected_paid_days",
            ].values[0]
            ds_curve = signup_value.loc[
                channel_month_ind
                & (signup_value.increments_from_signup == increments_start),
                "ds_curve",
            ].values[0]
            signup_value.loc[
                channel_month_ind
                & (signup_value.increments_from_signup == increments_start),
                "projected_paid_days",
            ] = prev_paid_days * ds_curve

            increments_start += 1

        signup_month_start = signup_month_start + pd.DateOffset(months=1)


def update_paid_days_v2(signup_month_start, channel, signup_value):
    signup_value.sort_values(
        ["channels", "signup_month", "increments_from_signup"],
        inplace=True,
        ignore_index=True,
    )
    increments_start = 1

    while signup_month_start <= pd.to_datetime("2024-11-01"):
        channel_month_ind = (signup_value.channels == channel) & (
            signup_value.signup_month == signup_month_start
        )
        # Take the first unpopulated increment, this MIGHT run into issues for thin channels where we project 0 paid days, but probably fine
        increments_start = signup_value.loc[
            channel_month_ind & (signup_value.projected_paid_days == 0),
            "increments_from_signup",
        ].min()

        while increments_start <= 12:
            # Increment_start 3 - we multiply avg(projected_paid_days) from the first two increments
            # to ds_curve, which is the ratio of paid_days_per_signup from the third increment to the first two
            if increments_start != 3:
                prev_paid_days = signup_value.loc[
                    channel_month_ind
                    & (signup_value.increments_from_signup == increments_start - 1),
                    "projected_paid_days",
                ].values[0]

                ds_curve = signup_value.loc[
                    channel_month_ind
                    & (signup_value.increments_from_signup == increments_start),
                    "ds_curve",
                ].values[0]
            else:
                prev_paid_days = signup_value.loc[
                    channel_month_ind
                    & (
                        signup_value.increments_from_signup.isin(
                            [increments_start - 2, increments_start - 1]
                        )
                    ),
                    "projected_paid_days",
                ].mean()

                ds_curve = (
                    signup_value.loc[
                        channel_month_ind
                        & (signup_value.increments_from_signup == increments_start),
                        "paid_days_per_signup",
                    ].values[0]
                    / signup_value.loc[
                        channel_month_ind
                        & (
                            signup_value.increments_from_signup.isin(
                                [increments_start - 2, increments_start - 1]
                            )
                        ),
                        "paid_days_per_signup",
                    ].mean()
                )

            signup_value.loc[
                channel_month_ind
                & (signup_value.increments_from_signup == increments_start),
                "projected_paid_days",
            ] = prev_paid_days * ds_curve

            increments_start += 1

        signup_month_start = signup_month_start + pd.DateOffset(months=1)


def cpd_accuracy_channel_plot(df, size=(13, 80)):
    channels = df.channels.unique()  # payback channels included in df
    num_plots = len(df.channels.unique())

    # list of metrics, colors, labels to utilize in each channel plot
    metrics = ["cpd_forecast_v2", "cpd_forecast_v1", "w_cpd_actual"]
    colors = ["green", "lightgreen", "blue"]
    names = ["New CPD Forecast", "Old CPD Forecast", "Actual CPD"]

    _, axes = plt.subplots(num_plots, 1, figsize=size)
    plt.subplots_adjust(hspace=0.5)
    plt.suptitle("Overall CPD Accuracy by Channel", fontsize=16, fontweight="bold")

    for ax, channel in zip(axes, channels):
        for metric, color, name in zip(metrics, colors, names):
            ax.plot(
                "forecast_month",
                metric,
                "--" if metric != "w_cpd_actual" else "-",
                label=name,
                color=color,
                marker=".",
                data=df.loc[df.channels == channel],
            )

        ax1 = ax.twinx()
        ax1.bar(
            x="forecast_month",
            height="data_volume_y",
            label="Num Trips (Actual)",
            width=20,
            color="blue",
            alpha=0.2,
            data=df.loc[df.channels == channel],
        )
        ax.set_title(channel, fontweight="bold")
        ax.set_ylabel("CPD ($)")
        ax.set_xlabel("signup month")
        ax1.set_ylabel("# trips observed")

        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax1.get_legend_handles_labels()
        ax.legend(
            h1 + h2,
            l1 + l2,
            bbox_to_anchor=(1.1, 1),
            loc="upper left",
            borderaxespad=0.0,
        )

    plt.show()


def nrpd_accuracy_channel_plot(df):
    channels = df.channels.unique()  # payback channels included in df
    num_plots = len(df.channels.unique())

    # list of metrics, colors, labels to utilize in each channel plot
    metrics = ["w_nrpd_forecast_v2", "w_nrpd_forecast_v1", "w_nrpd_actual"]
    colors = ["green", "lightgreen", "blue"]
    names = ["New NRPD Forecast", "Old NRPD Forecast", "Actual NRPD"]

    _, axes = plt.subplots(num_plots, 1, figsize=(13, 80))
    plt.subplots_adjust(hspace=0.5)

    for ax, channel in zip(axes, channels):
        for metric, color, name in zip(metrics, colors, names):
            ax.plot(
                "forecast_month",
                metric,
                "--" if metric != "w_nrpd_actual" else "-",
                label=name,
                color=color,
                marker=".",
                data=df.loc[df.channels == channel],
            )

        ax1 = ax.twinx()
        ax1.bar(
            x="forecast_month",
            height="data_volume_y",
            label="Num Trips (Actual)",
            width=20,
            color="blue",
            alpha=0.2,
            data=df.loc[df.channels == channel],
        )
        ax.set_title(channel, fontweight="bold")
        ax.set_ylabel("NRPD ($)")
        ax.set_xlabel("signup month")
        ax1.set_ylabel("# trips observed")

        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax1.get_legend_handles_labels()
        ax.legend(
            h1 + h2,
            l1 + l2,
            bbox_to_anchor=(1.1, 1),
            loc="upper left",
            borderaxespad=0.0,
        )

    plt.show()


def plot_monaco_forecasts(monaco_df, monaco_segment, size=(13, 80)):
    channels = monaco_df.channels.unique()  # payback channels included in df
    num_plots = len(channels)

    # list of metrics, colors, labels to utilize in each channel plot
    metrics = ["monaco_forecast_v2", "monaco_forecast_v1", "monaco_actual"]
    colors = ["green", "lightgreen", "blue"]
    names = ["Forecast V2", "Forecast V1", "Actual"]

    _, axes = plt.subplots(num_plots, 1, figsize=size)
    plt.subplots_adjust(
        hspace=0.5, top=0.96
    )  # Increase the vertical space between plots and adjust top space
    plt.suptitle(
        "Monaco Share Accuracy by Channel {monaco}".format(monaco=monaco_segment),
        fontsize=16,
        fontweight="bold",
    )

    for ax, channel in zip(axes, channels):
        for metric, color, name in zip(metrics, colors, names):
            ax.plot(
                "forecast_month",
                metric,
                "--" if metric != "monaco_actual" else "-",
                label=name,
                color=color,
                marker=".",
                data=monaco_df.loc[
                    (monaco_df.channels == channel)
                    & (monaco_df.monaco_bin == monaco_segment)
                ],
            )

        ax.set_title(f"Channel: {channel}", fontweight="bold")
        ax.set_ylabel("Values")
        ax.set_xlabel("Forecast Month")
        ax.legend(bbox_to_anchor=(1.1, 1), loc="upper left", borderaxespad=0.0)

    plt.show()


def cpd_monaco_accuracy_channel_plot(df, monaco_segment, size=(13, 80)):
    channels = df.channels.unique()  # payback channels included in df
    num_plots = len(df.channels.unique())

    # list of metrics, colors, labels to utilize in each channel plot
    metrics = ["cpd_raw_forecast", "w_cpd_raw_actual"]
    colors = ["green", "blue"]
    names = ["CPD per segment Forecast ", "Actual CPD per segment"]

    _, axes = plt.subplots(num_plots, 1, figsize=size)
    plt.subplots_adjust(hspace=0.5)
    plt.suptitle(
        "Per Segment CPD Accuracy by Channel {monaco}".format(monaco=monaco_segment),
        fontsize=16,
        fontweight="bold",
    )

    for ax, channel in zip(axes, channels):
        for metric, color, name in zip(metrics, colors, names):
            ax.plot(
                "forecast_month",
                metric,
                "--" if metric != "w_cpd_raw_actual" else "-",
                label=name,
                color=color,
                marker=".",
                data=df.loc[
                    (df.channels == channel) & (df.monaco_bin == monaco_segment)
                ],
            )

        ax1 = ax.twinx()
        ax1.bar(
            x="forecast_month",
            height="data_volume_y",
            label="Num Trips (Actual)",
            width=20,
            color="blue",
            alpha=0.2,
            data=df.loc[(df.channels == channel) & (df.monaco_bin == monaco_segment)],
        )
        ax.set_title(channel, fontweight="bold")
        ax.set_ylabel("CPD per Monaco Segment ($)")
        ax.set_xlabel("signup month")
        ax1.set_ylabel("# trips observed")

        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax1.get_legend_handles_labels()
        ax.legend(
            h1 + h2,
            l1 + l2,
            bbox_to_anchor=(1.1, 1),
            loc="upper left",
            borderaxespad=0.0,
        )

    plt.show()


def calculate_accuracy_measures(top_n_channels, cpd_payback, monaco_payback):
    # Filter data for the specified period
    cpd_payback_2024 = cpd_payback.loc[
        cpd_payback.forecast_month >= "2024-01-01"
    ].reset_index(drop=True)

    # Get the top N channels based on data volume
    top_channels = (
        cpd_payback_2024.groupby(["channels"])["data_volume_y"]
        .mean()
        .sort_values(ascending=False)[:top_n_channels]
        .index.to_list()
    )

    # Filter monaco_payback data for the top N channels
    monaco_payback_2024_top_n = monaco_payback.loc[
        (monaco_payback.forecast_month >= "2024-01-01")
        & (monaco_payback.channels.isin(top_channels))
    ].reset_index(drop=True)

    # Calculate average MAPE for all segments
    avg_MAPE_v2_all = monaco_payback_2024_top_n["MAPE_v2"].mean()
    avg_MAPE_v1_all = monaco_payback_2024_top_n["MAPE_v1"].mean()

    # Calculate average MAPE for segment A
    avg_MAPE_v2_A = monaco_payback_2024_top_n.loc[
        monaco_payback_2024_top_n.monaco_bin.isin(["A1", "A2", "A3"])
    ]["MAPE_v2"].mean()
    avg_MAPE_v1_A = monaco_payback_2024_top_n.loc[
        monaco_payback_2024_top_n.monaco_bin.isin(["A1", "A2", "A3"])
    ]["MAPE_v1"].mean()

    # Calculate average MAPE for other segments
    avg_MAPE_REST = monaco_payback_2024_top_n.groupby("monaco_bin", as_index=False)[
        ["MAPE_v2", "MAPE_v1"]
    ].mean()
    avg_MAPE_REST.rename(
        {"monaco_bin": "Segments", "MAPE_v2": "avg_MAPE_v2", "MAPE_v1": "avg_MAPE_v1"},
        axis=1,
        inplace=True,
    )
    avg_MAPE_REST["channels"] = f"top_{top_n_channels}_channels"
    avg_MAPE_REST["Period"] = "2024-01 ~ 2024-09"

    # Create rows for all segments and append to accuracy_measure_A
    accuracy_measure_A = pd.DataFrame(
        {
            "channels": [f"top_{top_n_channels}_channels"] * 2,
            "Period": ["2024-01 ~ 2024-09"] * 2,
            "Segments": ["All", "A"],
            "avg_MAPE_v2": [avg_MAPE_v2_all, avg_MAPE_v2_A],
            "avg_MAPE_v1": [avg_MAPE_v1_all, avg_MAPE_v1_A],
        }
    )

    # Combine all accuracy measures
    accuracy_measure_top_n = pd.concat(
        [
            accuracy_measure_A,
            avg_MAPE_REST.loc[~avg_MAPE_REST.Segments.isin(["A1", "A2", "A3"])],
        ],
        ignore_index=True,
    )
    accuracy_measure_top_n["pcnt_MAPE_improvement"] = (
        (accuracy_measure_top_n["avg_MAPE_v1"] - accuracy_measure_top_n["avg_MAPE_v2"])
        * 100
        / accuracy_measure_top_n["avg_MAPE_v1"]
    )

    return accuracy_measure_top_n


def calculate_accuracy_measures_nrpd(top_n_channels, weighted_nrpd_payback):
    # Filter data for the specified period
    # cpd_payback_2024 = cpd_payback.loc[cpd_payback.forecast_month >= '2024-01-01'].reset_index(drop=True)

    # Get the top N channels based on data volume
    top_channels = (
        weighted_nrpd_payback.groupby(["channels"])["data_volume_y"]
        .mean()
        .sort_values(ascending=False)[:top_n_channels]
        .index.to_list()
    )

    # Filter monaco_payback data for the top N channels
    nrpd_payback_2024_top_n = weighted_nrpd_payback.loc[
        (weighted_nrpd_payback.channels.isin(top_channels))
    ].reset_index(drop=True)

    # Calculate average MAPE for all segments
    avg_MAPE_v2_all = nrpd_payback_2024_top_n["MAPE_v2"].mean()
    avg_MAPE_v1_all = nrpd_payback_2024_top_n["MAPE_v1"].mean()

    # Create rows for all segments and append to accuracy_measure_A
    accuracy_measure_A = pd.DataFrame(
        {
            "channels": [f"top_{top_n_channels}_channels"],
            "Period": ["2024-01 ~ 2024-09"],
            "avg_MAPE_v2": [avg_MAPE_v2_all],
            "avg_MAPE_v1": [avg_MAPE_v1_all],
        }
    )

    # Combine all accuracy measures
    accuracy_measure_A["pcnt_MAPE_improvement"] = (
        (accuracy_measure_A["avg_MAPE_v1"] - accuracy_measure_A["avg_MAPE_v2"])
        * 100
        / accuracy_measure_A["avg_MAPE_v1"]
    )

    return accuracy_measure_A


def calculate_accuracy_measures_pcp(top_n_channels, all_agg):
    # Filter data for the specified period
    # cpd_payback_2024 = cpd_payback.loc[cpd_payback.forecast_month >= '2024-01-01'].reset_index(drop=True)

    # Get the top N channels based on data volume
    top_channels = (
        all_agg.groupby(["channels"])["trips"]
        .mean()
        .sort_values(ascending=False)[:top_n_channels]
        .index.to_list()
    )

    # Filter monaco_payback data for the top N channels
    pcp_2024_top_n = all_agg.loc[(all_agg.channels.isin(top_channels))].reset_index(
        drop=True
    )

    # Calculate average MAPE for all segments
    avg_MAPE_v2_all = pcp_2024_top_n["MAPE_v2"].mean()
    avg_MAPE_v1_all = pcp_2024_top_n["MAPE_v1"].mean()

    # Create rows for all segments and append to accuracy_measure_A
    accuracy_measure_A = pd.DataFrame(
        {
            "channels": [f"top_{top_n_channels}_channels"],
            "Period": ["2024-01 ~ 2024-06"],
            "avg_MAPE_v2": [avg_MAPE_v2_all],
            "avg_MAPE_v1": [avg_MAPE_v1_all],
        }
    )

    # Combine all accuracy measures
    accuracy_measure_A["pcnt_MAPE_improvement"] = (
        (accuracy_measure_A["avg_MAPE_v1"] - accuracy_measure_A["avg_MAPE_v2"])
        * 100
        / accuracy_measure_A["avg_MAPE_v1"]
    )

    return accuracy_measure_A


def pdps_forecast_plot(df):
    channels = df["channels"].unique()

    fig, axes = plt.subplots(
        nrows=len(channels), ncols=1, figsize=(14, len(channels) * 4)
    )

    for ax, channel in zip(axes, channels):
        channel_data = df[df["channels"] == channel]

        # Plot projected_paid_days
        ax.plot(
            channel_data["signup_month"],
            channel_data["projected_paid_days"],
            marker="o",
            color="b",
            label="Projected Paid Days",
        )
        ax.set_title(f"Projected Paid Days and Signups for {channel}")
        ax.set_xlabel("Signup Month")
        ax.set_ylabel("Projected Paid Days", color="b")
        ax.tick_params(axis="y", labelcolor="b")

        # Create a twin Axes sharing the xaxis
        ax2 = ax.twinx()
        # Plot num_signups_actual_by_increment and num_signups_from_activation as bar plots with specified width
        ax2.bar(
            channel_data["signup_month"],
            channel_data["num_signups_actual_by_increment"],
            width=10,
            alpha=0.6,
            color="g",
            label="Actual Signups",
        )
        ax2.bar(
            channel_data["signup_month"],
            channel_data["num_signups_from_activation"],
            width=10,
            alpha=0.6,
            color="r",
            label="Signups from Activation",
            bottom=channel_data["num_signups_actual_by_increment"],
        )
        ax2.set_ylabel("Number of Signups", color="g")
        ax2.tick_params(axis="y", labelcolor="g")

        # Add legends
        ax.legend(loc="upper left")
        ax2.legend(loc="upper right")

    plt.tight_layout()
    plt.show()
