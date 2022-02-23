# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import fire
from tqdm import tqdm
import os
import pandas as pd
from pathlib import Path
from loguru import logger
from qlib.utils import exists_qlib_data
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

class SplitInstruments:
    def __init__(
        self,
        csv_path: str,
        target_dir : str,
        symbol_field_name: str = "symbol",
        max_workers: int = 16,):
        """
        split one stock csv data into several csv datas
        each csv data is named using stock name

        Parameters
        ----------
        csv_path: str
            stock data path
        target_dir: str
            data save directory
        symbol_field_name: str, default "symbol"
            symbol field name
        max_workers: int, default None
            number of threads
        """
        
        csv_path=Path(csv_path).expanduser()
        self.symbol_field_name = symbol_field_name
        self.csv_file=self._read(csv_path)
        self.target_dir = Path(target_dir).expanduser()
        self.works = max_workers

    def _read(self,path):
        path=str(path)
        if path.endswith('csv'):
            return pd.read_csv(path)
        elif path.endswith('parquet'):
            return pd.read_parquet(path)

    def split(self):
        self.target_dir.mkdir(parents=True, exist_ok=True)
        grouped_csv_file=self.csv_file.groupby(by=self.symbol_field_name)

        # This mutiprocess method is very time-costing, don't know why
        #with tqdm(total=len(grouped_csv_file)) as p_bar:
        #    with ProcessPoolExecutor(max_workers=self.works) as executor:
        #        for _ in executor.map(_to_csv, grouped_csv_file):
        #            p_bar.update()

        #02:23 take for crsp/a_stock/msf
        with tqdm(iterable=grouped_csv_file, total=len(grouped_csv_file)) as p_bar:
            for group in p_bar:
                self._to_csv(group)

    def _to_csv(self, group):
        instrument_name, instrument_df = group
        if not isinstance(instrument_name, str):
            instrument_name = str(instrument_name)
        instrument_df.to_csv(os.path.join(self.target_dir,instrument_name+".csv"),index=False)
        

if __name__ == "__main__":
    fire.Fire(SplitInstruments)