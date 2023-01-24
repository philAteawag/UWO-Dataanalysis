
from pathlib import Path
import pendulum

from datapool_client import DataPool, reshape

target_directory = Path("C:/Users/dischand/switchdrive/UWO/Arbeiten und Artikel/UWO_Data_paper/ara_inflow")

datapool_instance = DataPool(to_replace={"parameter": "variable"})

# sources = ["bf_plsZUL1102_inflow_ara", "bf_plsZUL1101_inflow_ara"]
sources = ["bf_plsABL1101_outflow_ara"]

start = "2016-04-11 08:54:00"
end = "2020-08-31 23:59:59"
# end = pendulum.now("Europe/Zurich").to_datetime_string()

for s in sources:

    data = datapool_instance.signal.get(
        source_name=s, 
        start=start, 
        end=end
    )

    data = data[["timestamp", "value"]]

    data.to_csv(target_directory / f"{s}.csv", index=False)
