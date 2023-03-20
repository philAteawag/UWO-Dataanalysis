
from pathlib import Path
import pendulum

from datapool_client import DataPool

target_directory = Path("C:/Users/dischand/switchdrive/UWO/Arbeiten und Artikel/UWO_Data_paper/_ERIC/UWO_datapaper_ERIC/_A_field_observations")

datapool_instance = DataPool(to_replace={"parameter": "variable"})

sources = ["bn_r03_rub_morg", "bf_plsZUL1100_inflow_ara"]

start = "2016-01-01 00:00:00"
end = pendulum.now("Europe/Zurich").to_datetime_string()

for s in sources:

    data = datapool_instance.signal.get(
        source_name=s, 
        start=start, 
        end=end
    )

    data = data[["timestamp", "value"]]

    data.to_csv(target_directory / f"{s}.csv", index=False)
