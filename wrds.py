
import qlib
import pandas as pd
from qlib.utils import init_instance_by_config



provider_uri="/storage/qlib/qlib_data/wrds/comp/d_global/g_funda"
qlib.init(provider_uri=provider_uri)


market="longest1k"
data_handler_config = {
    "start_time": "1987-06-30",
    "end_time": "2020-12-31",
    "fit_start_time": "2008-01-01",
    "fit_end_time": "2020-12-31",
    "instruments": market,
}


dataset_conifg= {
    "class": "DatasetH",
    "module_path": "qlib.data.dataset",
    "kwargs": {
        "handler": {
            "class": "FundA",
            "module_path": "data.wrds_handler",
            "kwargs": data_handler_config,
        },
        "segments": {
            "train": ("2008-01-01", "2014-12-31"),
            "valid": ("2015-01-01", "2016-12-31"),
            "test": ("2017-01-01", "2020-12-31"),
            "all": ("1987-06-30", "2020-12-31"),
        },
    },
}

dataset = init_instance_by_config(dataset_conifg)
df=dataset.prepare("all", col_set=["feature", "label"])
print(df.shape)
