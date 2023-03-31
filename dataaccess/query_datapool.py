from pathlib import Path
import pendulum

from datapool_client import DataPool

target_directory = Path("C:/Users/dischand/VisualStudioProjects/data/processed")

datapool_instance = DataPool(to_replace={"parameter": "variable"})

sources = ["bn_r02_school_chatzenrainstr"]

start = "2016-01-01 00:00:00"
end = pendulum.now("Europe/Zurich").to_datetime_string()

for s in sources:

    data = datapool_instance.signal.get(source_name=s, start=start, end=end)

    data = data[["timestamp", "value"]]

    data.to_csv(target_directory / f"{s}.csv", index=False)
