# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import qlib
from qlib.data import D
import os
import fire
import datacompy
import pandas as pd
from tqdm import tqdm
from loguru import logger
import time
import numpy as np
class CheckBin:

    NOT_IN_FEATURES = "not in features"
    COMPARE_FALSE = "compare False"
    COMPARE_TRUE = "compare True"
    COMPARE_ERROR = "compare error"

    def __init__(
        self,
        qlib_dir: str,
        parquet_path: str,
        check_fields: str = None,
        freq: str = "day",
        symbol_field_name: str = "symbol",
        date_field_name: str = "date",
        max_workers: int = 16,
        check_symbol_num: int = 100,
        check_feature_num: int = 30,
    ):
        """

        Parameters
        ----------
        qlib_dir : str
            qlib dir
        parquet_path : str
            origin csv path
        check_fields : str, optional
            check fields, by default None, check qlib_dir/features/<first_dir>/*.<freq>.bin
        freq : str, optional
            freq, value from ["day", "1m"]
        symbol_field_name: str, optional
            symbol field name, by default "symbol"
        date_field_name: str, optional
            date field name, by default "date"
        max_workers: int, optional
            max workers, by default 16
        """
        self.qlib_dir = Path(qlib_dir).expanduser()
        self.check_symbol_num = check_symbol_num
        self.date_field_name = date_field_name
        self.max_workers = max_workers
        self.freq = freq
        
        bin_path_list = list(self.qlib_dir.joinpath("features").iterdir())
        
        if isinstance(symbol_field_name, str):
            symbol_field_name = symbol_field_name.split(",")
        self.symbol_field_tuple = tuple(filter(lambda x: len(x) > 0, map(str.strip, symbol_field_name)))
        
        try:
            self.catagory_mappers=self._get_mapper_dict(qlib_dir)
            self.catagory_fields=list(self.catagory_mappers.keys())
        except:
            self.catagory_mappers=None
            self.catagory_fields=None
        if check_fields is None:
            check_fields = list(map(lambda x: x.name.split(".")[0], bin_path_list[0].glob(f"*.bin")))
        else:
            check_fields = check_fields.split(",") if isinstance(check_fields, str) else check_fields
        self.check_fields = list(map(lambda x: x.strip(), check_fields))[:check_feature_num]
        self.check_fields = list(set(self.check_fields)|set(self.catagory_fields))
        self.qlib_fields = list(map(lambda x: f"${x}", self.check_fields))
        self.parquet_path = parquet_path
        self.origin_df = pd.read_parquet(parquet_path)
        self.symbol_field_name = self._get_symbol_field_name()
        
        self.origin_df[date_field_name] = pd.to_datetime(self.origin_df[date_field_name])
        self.origin_df[[self.symbol_field_name]] = self.origin_df[[self.symbol_field_name]].astype(str)
        self.check_symbols = self._get_check_symbols()
        
        qlib.init(
            provider_uri=str(self.qlib_dir.resolve()),
            mount_path=str(self.qlib_dir.resolve()),
            auto_mount=False,
            redis_port=-1,
        )
    
        self.qlib_df = D.features(self.check_symbols, self.qlib_fields, freq=self.freq)
        self.qlib_df.rename(columns={_c: _c.strip("$") for _c in self.qlib_df.columns}, inplace=True)
        self.qlib_df = self._map_catagory_fields(self.qlib_df)
        
        self.origin_df = self.origin_df[self.origin_df[self.symbol_field_name].isin(self.check_symbols)]
        self.origin_df.set_index([self.symbol_field_name, self.date_field_name], inplace=True)
        self.origin_df.index.names = self.qlib_df.index.names
        self.origin_df = self.origin_df[self.qlib_df.columns]
        
        self.qlib_df = self.qlib_df.reindex(self.origin_df.index)
        self.qlib_df = self.qlib_df.astype(self.origin_df.dtypes.to_dict())
    
    def _get_check_symbols(self):
        def takeSecond(elem):
            return elem[1]
        check_symbols = self.origin_df[self.symbol_field_name].value_counts().reset_index()
        check_symbols = check_symbols.values.tolist()
        check_symbols.sort(key=takeSecond)
        check_symbols = sorted(np.array(check_symbols)[-self.check_symbol_num:,0].tolist())
        return check_symbols
    def _get_mapper_dict(self, uri: str):

        catagory_dict={}
        dir=uri+'/catagories'
        for file in os.listdir(dir):
            field= file.split('.')[0]
            with open(dir+'/'+file, 'r') as f:
                content=f.read().splitlines()
            catagory_dict[field]=dict(zip(range(len(content)), content))
        return catagory_dict
    def _map_catagory_fields(self, df:pd.DataFrame):
        if self.catagory_mappers is None:
            return df
        for catafory_field_i in self.catagory_mappers: 
            if catafory_field_i in df.columns:
                df[catafory_field_i]=df[catafory_field_i].apply(lambda x:self.catagory_mappers[catafory_field_i].get(x))
        return df
    
    def _get_symbol_field_name(self):
        if len(self.symbol_field_tuple)==0:
            raise ValueError("symbol field name must be specified! ")
        elif len(self.symbol_field_tuple)==1:
            return self.symbol_field_tuple[0]
        else:
            #merge multi cols into one col
            symbol_field_name="_".join(self.symbol_field_tuple)
            self.origin_df[symbol_field_name]=""
            for field in self.symbol_field_tuple:
                self.origin_df[symbol_field_name]+= "_"+self.origin_df[field]
            self.origin_df[symbol_field_name]=self.origin_df[symbol_field_name].apply(lambda x:x[1:])
            return symbol_field_name
    
    def check_single(self):
        """Check whether the bin file after ``dump_bin.py`` is executed is consistent with the original parquet file data"""
        logger.info("start check......")
        
        try:
            compare = datacompy.Compare(
                self.origin_df,
                self.qlib_df,
                on_index=True,
                abs_tol=1e-08,  # Optional, defaults to 0
                rel_tol=1e-05,  # Optional, defaults to 0
                df1_name="Original",  # Optional, defaults to 'df1'
                df2_name="New",  # Optional, defaults to 'df2'
            )
            _r = compare.matches(ignore_extra_columns=True)
            if _r:
                logger.info(f"compare True!")
            else:
                logger.info("compare False!")
        except Exception as e:
            logger.warning(f"compare error: {e}")
        
if __name__ == "__main__":
    fire.Fire(CheckBin)