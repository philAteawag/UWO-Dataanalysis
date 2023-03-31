# -*- coding: utf-8 -*-


import os
import json
import dotenv
import pathlib

import requests
import pandas as pd


dotenv.load_dotenv(dotenv_path=pathlib.Path(".env"))

DOMAIN = os.getenv("DECENTLAB_DOMAIN")
API_KEY = os.getenv("DECENTLAB_API_KEY")


def query(
    domain: str = DOMAIN,
    api_key: str = API_KEY,
    time_filter="",
    device="//",
    location="//",
    sensor="//",
    include_network_sensors=False,
    channel="//",
    agg_func=None,
    agg_interval=None,
    do_unstack=True,
    convert_timestamp=True,
    timezone="UTC",
):
    device = "/^" + device + "$/"  # limiting reg expressions
    select_var = "value"
    fill = ""
    interval = ""

    if agg_func is not None:
        select_var = agg_func + '("value") as value'
        fill = "fill(null)"

    if agg_interval is not None:
        interval = ", time(%s)" % agg_interval

    if time_filter != "":
        time_filter = " AND " + time_filter

    filter_ = (
        " location =~ %s"
        " AND node =~ %s"
        " AND sensor =~ %s"
        " AND ((channel =~ %s OR channel !~ /.+/)"
        " %s)"
    ) % (
        location,
        device,
        sensor,
        channel,
        ("" if include_network_sensors else "AND channel !~ /^link-/"),
    )

    q = ('SELECT %s FROM "measurements" ' " WHERE %s %s" ' GROUP BY "uqk" %s %s') % (
        select_var,
        filter_,
        time_filter,
        interval,
        fill,
    )

    r = requests.get(
        "https://%s/api/datasources/proxy/1/query" % domain,
        params={"db": "main", "epoch": "ms", "q": q},
        headers={"Authorization": "Bearer %s" % api_key},
    )

    data = json.loads(r.text)

    if "results" not in data or "series" not in data["results"][0]:
        return None
        # raise ValueError("No series returned: %s" % r.text)

    def _ix2df(series):
        df = pd.DataFrame(series["values"], columns=series["columns"])
        df["series"] = series["tags"]["uqk"]
        return df

    df = pd.concat(_ix2df(s) for s in data["results"][0]["series"])

    if convert_timestamp:
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        try:
            df["time"] = df["time"].dt.tz_localize("UTC")
        except TypeError:
            pass
        df["time"] = df["time"].dt.tz_convert(timezone)

    df = df.set_index(["time", "series"])
    df = df.sort_index()

    if do_unstack:
        df = df.pivot_table(columns="series", index="time", values="value")

    return df


if __name__ == "__main__":
    path = pathlib.WindowsPath(
        r"C:\Users\dischand\switchdrive\UWO\Arbeiten und Artikel\UWO_Data_paper\decentlab_data_correction"
    )

    devices = ["317", "319", "319", "330", "331", "907"]

    time_filter = (
        f"time >= '{'2019-06-09 00:00:00'}' AND time <= '{'2019-06-17 23:59:59'}'"
    )
    sensor = "/(distance)|(rssi)|(snr)|(counter)|(spreading)|(trials)|(precipitation)|(decagon)|(keller)|(pressure)|(battery)|(rain-gauge-interval)|(rain-gauge-precipitation)|(rain-gauge-precipitation-sum)|(temperature)|(id)/"

    for device in devices:
        df = query(
            time_filter=time_filter,
            device=device,
            sensor=sensor,
            include_network_sensors=True,
        )

        new_index = []
        tz_offset = []
        for date in df.index.to_list():
            new_index.append(date.strftime("%Y-%m-%d %H:%M:%S"))
            tz_offset.append("+" + "".join(str(date).split("+")[1].split(":")) + "")

        df.index = new_index
        df.index.name = "timestamp"
        df["tzOffset"] = tz_offset
        df["tzName"] = "UTC"

        df = df[~df.index.duplicated(keep="first")]
        df = df[~df.iloc[:, 3].isna()]

        for column in df.columns:
            if "bandwidth.link-lora" in column:
                del df[column]
                break

        if not device.startswith("bt_dl"):
            for column in df.columns:
                if "maxim-ds18b20-id" in column:
                    del df[column]

        for column in df.columns:
            if df[column].dtype == float:
                df[column] = pd.to_numeric(df[column], downcast="integer")

        df.to_csv(path / f"{device}.csv", sep=";", float_format="%g", decimal=",")
