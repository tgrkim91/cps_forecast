import pandas as pd
import glob
import os
from os import path
import argparse

from datetime import datetime

from utils import loader, load_data, update_paid_days

timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

CURRENT_DIR = path.dirname(path.abspath(__file__))
SQL_PATH = path.join(CURRENT_DIR, "sql")
CSV_PATH = path.join(CURRENT_DIR, "nov_csv")
CPD_PATH = path.join(CURRENT_DIR, "cpd_csv")

payback_channels = [
    "Apple",
    "Apple_Brand",
    "Google_Desktop",
    "Google_Desktop_Brand",
    "Google_Mobile",
    "Google_Mobile_Brand",
    "Kayak_Desktop",
    "Kayak_Desktop_Core",
    "Kayak_Mobile_Core",
    "Mediaalpha",
    "Expedia",
    "Microsoft_Desktop",
    "Microsoft_Desktop_Brand",
    "Reddit",
    "Moloco",
    "Kayak_Desktop_Compare",
    "Google_Pmax",
    "Kayak_Desktop_Carousel",
    "Kayak_Mobile_Carousel",
    "Kayak_Afterclick",
    "Facebook/IG_App",
    "Facebook/IG_Web",
]


def main(forecast_month):
    ## Load data
    # NRPD
    nrpd_forecast_all = pd.read_csv(path.join(CSV_PATH, "nrpd_forecast_final.csv"))
    nrpd_forecast_v2 = nrpd_forecast_all.loc[
        nrpd_forecast_all.forecast_month == forecast_month
    ].reset_index(drop=True)
    nrpd_forecast_v2["forecast_month"] = pd.to_datetime(
        nrpd_forecast_v2["forecast_month"]
    )
    nrpd_forecast_v2["month"] = pd.to_datetime(nrpd_forecast_v2["month"])

    nrpd_forecast_v2.sort_values(
        ["forecast_month", "channels", "month"], inplace=True, ignore_index=True
    )
    nrpd_forecast_v2.rename(
        {"forecast_nrpd_by_incre_channel": "nrpd_forecast_v2"}, axis=1, inplace=True
    )

    nrpd_payback = nrpd_forecast_v2.loc[
        nrpd_forecast_v2.channels.isin(payback_channels)
    ].reset_index(drop=True)

    # CPD
    # v2_forecast_by_channel
    # Get a list of all CSV files starting with 'cpd_forecast_'
    csv_files = glob.glob(os.path.join(CPD_PATH, "cpd_forecast_*.csv"))

    # Read and append all CSV files into one dataframe
    cpd_forecast_v2 = pd.concat(
        [pd.read_csv(file) for file in csv_files], ignore_index=True
    )
    # cpd_nov = pd.read_csv(path.join(CSV_PATH, "cpd_forecast_0_12_v2_regrouped.csv"))

    cpd_forecast_v2["trip_end_month"] = pd.to_datetime(
        cpd_forecast_v2["trip_end_month"]
    )
    cpd_monthly = cpd_forecast_v2.loc[
        cpd_forecast_v2.trip_end_month == forecast_month
    ].reset_index(drop=True)

    # PDPS
    rs = loader()
    # pdps_forecast = load_data(
    # sql_path=path.join(SQL_PATH, "paid_days_forecast.sql"), loader=rs
    # )
    pdps_forecast = pd.read_csv(path.join(CSV_PATH, "pdps_forecast.csv"))
    pdps_forecast.signup_month = pd.to_datetime(pdps_forecast.signup_month)

    pdps_monthly = pdps_forecast.loc[
        pdps_forecast.signup_month == forecast_month
    ].reset_index(drop=True)

    # nov_pdps.loc[
    # (nov_pdps.projected_paid_days.isnull()),
    # "projected_paid_days",
    # ] = 0

    # for channel in nov_pdps.channels.unique():
    # signup_month_start = pd.to_datetime("2024-11-01")
    # update_paid_days_v2(signup_month_start, channel, nov_pdps)

    # Halo effect
    halo_effect = load_data(sql_path="./sql/halo_effect.sql", loader=rs)

    # Merge all data
    forecast_monthly = pd.merge(
        pdps_monthly,
        cpd_monthly,
        how="left",
        left_on=["channels", "signup_month"],
        right_on=["channels", "trip_end_month"],
    )
    forecast_monthly = pd.merge(
        forecast_monthly,
        nrpd_payback[
            ["channels", "forecast_month", "increments_from_signup", "nrpd_forecast_v2"]
        ],
        how="left",
        left_on=["channels", "signup_month", "increments_from_signup"],
        right_on=["channels", "forecast_month", "increments_from_signup"],
    )
    forecast_monthly = pd.merge(
        forecast_monthly, halo_effect, how="left", on=["channels"]
    )

    # nov pcp per signup
    forecast_monthly["pcp_per_signup"] = forecast_monthly["projected_paid_days"] * (
        forecast_monthly["nrpd_forecast_v2"] * 0.98 - forecast_monthly["cost_per_day"]
    )
    forecast_monthly["pcp_per_signup_with_halo"] = (
        forecast_monthly["projected_paid_days"]
        * (
            forecast_monthly["nrpd_forecast_v2"] * 0.98
            - forecast_monthly["cost_per_day"]
        )
        * forecast_monthly["paid_halo"]
    )

    forecast_monthly["projected_paid_days_ratio"] = forecast_monthly.groupby(
        "channels", as_index=False
    )["projected_paid_days"].transform(lambda x: x / x.sum())
    forecast_monthly["nrpd_forecast_v2_weighted"] = (
        forecast_monthly["nrpd_forecast_v2"]
        * forecast_monthly["projected_paid_days_ratio"]
    )

    forecast_monthly_cps_targets = forecast_monthly.groupby(
        "channels", as_index=False
    ).agg(
        {
            "projected_paid_days": "sum",
            "nrpd_forecast_v2_weighted": "sum",
            "cost_per_day": "max",
            "pcp_per_signup": "sum",
            "pcp_per_signup_with_halo": "sum",
        }
    )
    filename = f"cps_targets_{forecast_month}.csv"

    forecast_monthly_cps_targets.to_csv(filename, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run CPS Pipeline")
    parser.add_argument(
        "--forecast_month",
        type=str,
        default="2024-11-01",
        help="End month of the observation period",
    )
    args = parser.parse_args()

    main(args.forecast_month)
