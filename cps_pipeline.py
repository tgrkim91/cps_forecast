import pandas as pd
from os import path
from datetime import datetime

from utils import loader, load_data, update_paid_days

timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

CURRENT_DIR = path.dirname(path.abspath(__file__))
SQL_PATH = path.join(CURRENT_DIR, "sql")
CSV_PATH = path.join(CURRENT_DIR, "nov_csv")

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


def main():
    ## Load data
    # NRPD
    nrpd_forecast_all = pd.read_csv(
        path.join(CSV_PATH, "nrpd_forecast_regroup_final.csv")
    )
    nrpd_forecast_v2 = nrpd_forecast_all.loc[
        nrpd_forecast_all.forecast_month == "2024-11-01"
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

    nrpd_nov = nrpd_forecast_v2.loc[
        nrpd_forecast_v2.channels.isin(payback_channels)
    ].reset_index(drop=True)

    # CPD
    # v2_forecast_by_channel
    cpd_nov = pd.read_csv(path.join(CSV_PATH, "cpd_forecast_0_12_v2_regrouped.csv"))

    cpd_nov["trip_end_month"] = pd.to_datetime(cpd_nov["trip_end_month"])

    # PDPS
    rs = loader()
    # pdps_forecast = load_data(
    # sql_path=path.join(SQL_PATH, "paid_days_forecast.sql"), loader=rs
    # )
    pdps_forecast = pd.read_csv(path.join(CSV_PATH, "pdps_forecast.csv"))
    pdps_forecast.signup_month = pd.to_datetime(pdps_forecast.signup_month)

    nov_pdps = pdps_forecast.loc[
        pdps_forecast.signup_month == "2024-11-01"
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
    nov = pd.merge(
        nov_pdps,
        cpd_nov,
        how="left",
        left_on=["channels", "signup_month"],
        right_on=["channels", "trip_end_month"],
    )
    nov = pd.merge(
        nov,
        nrpd_nov[
            ["channels", "forecast_month", "increments_from_signup", "nrpd_forecast_v2"]
        ],
        how="left",
        left_on=["channels", "signup_month", "increments_from_signup"],
        right_on=["channels", "forecast_month", "increments_from_signup"],
    )
    nov = pd.merge(nov, halo_effect, how="left", on=["channels"])

    # nov pcp per signup
    nov["pcp_per_signup"] = nov["projected_paid_days"] * (
        nov["nrpd_forecast_v2"] * 0.98 - nov["cost_per_day"]
    )
    nov["pcp_per_signup_with_halo"] = (
        nov["projected_paid_days"]
        * (nov["nrpd_forecast_v2"] * 0.98 - nov["cost_per_day"])
        * nov["paid_halo"]
    )

    nov["projected_paid_days_ratio"] = nov.groupby("channels", as_index=False)[
        "projected_paid_days"
    ].transform(lambda x: x / x.sum())
    nov["nrpd_forecast_v2_weighted"] = (
        nov["nrpd_forecast_v2"] * nov["projected_paid_days_ratio"]
    )

    nov_cps_targets = nov.groupby("channels", as_index=False).agg(
        {
            "projected_paid_days": "sum",
            "nrpd_forecast_v2_weighted": "sum",
            "cost_per_day": "max",
            "pcp_per_signup": "sum",
            "pcp_per_signup_with_halo": "sum",
        }
    )
    nov_cps_targets.to_csv("nov_cps_targets.csv", index=False)


if __name__ == "__main__":
    main()
