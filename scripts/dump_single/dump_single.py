# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import abc
import shutil
import traceback
from pathlib import Path
from typing import Iterable, List, Union
from functools import partial

import fire
import numpy as np
import pandas as pd
from tqdm import tqdm
from loguru import logger
from qlib.utils import fname_to_code, code_to_fname

numeric_types=['float64']
catagory_types=['object','datetime64[ns]']


class DumpDataBase:
    INSTRUMENTS_START_FIELD = "start_datetime"
    INSTRUMENTS_END_FIELD = "end_datetime"
    CALENDARS_DIR_NAME = "calendars"
    FEATURES_DIR_NAME = "features"
    INSTRUMENTS_DIR_NAME = "instruments"
    DUMP_FILE_SUFFIX = ".bin"
    DAILY_FORMAT = "%Y-%m-%d"
    HIGH_FREQ_FORMAT = "%Y-%m-%d %H:%M:%S"
    INSTRUMENTS_SEP = "\t"
    CATAGORIES_SEP="\t"
    INSTRUMENTS_FILE_NAME = "all.txt"

    UPDATE_MODE = "update"
    ALL_MODE = "all"

    def __init__(
        self,
        csv_path: str,
        qlib_dir: str,
        backup_dir: str = None,
        freq: str = "day",
        max_workers: int = 16,
        date_field_name: str = "date",
        file_suffix: str = ".csv",
        symbol_field_name: str = "symbol",
        exclude_fields: str = "",
        include_fields: str = "",
        limit_nums: int = None,
    ):
        """
        Parameters
        ----------
        csv_path: str
            stock data path or directory
        qlib_dir: str
            qlib(dump) data director
        backup_dir: str, default None
            if backup_dir is not None, backup qlib_dir to backup_dir
        freq: str, default "day"
            transaction frequency
        max_workers: int, default None
            number of threads
        date_field_name: str, default "date"
            the name of the date field in the csv
        file_suffix: str, default ".csv"
            file suffix
        symbol_field_name: str, default "symbol"
            symbol field name
        include_fields: tuple
            dump fields
        exclude_fields: tuple
            fields not dumped
        limit_nums: int
            Use when debugging, default None
        """
        csv_path = Path(csv_path).expanduser()
        if isinstance(exclude_fields, str):
            exclude_fields = exclude_fields.split(",")
        if isinstance(include_fields, str):
            include_fields = include_fields.split(",")
        self._exclude_fields = tuple(filter(lambda x: len(x) > 0, map(str.strip, exclude_fields)))
        self._include_fields = tuple(filter(lambda x: len(x) > 0, map(str.strip, include_fields)))
        self.file_suffix = file_suffix
        self.symbol_field_name = symbol_field_name
        self.csv_files = sorted(csv_path.glob(f"*{self.file_suffix}") if csv_path.is_dir() else [csv_path])
        if limit_nums is not None:
            self.csv_files = self.csv_files[: int(limit_nums)]
        self.qlib_dir = Path(qlib_dir).expanduser()
        self.backup_dir = backup_dir if backup_dir is None else Path(backup_dir).expanduser()
        if backup_dir is not None:
            self._backup_qlib_dir(Path(backup_dir).expanduser())

        self.freq = freq
        self.calendar_format = self.DAILY_FORMAT if self.freq == "day" else self.HIGH_FREQ_FORMAT

        self.works = max_workers
        self.date_field_name = date_field_name

        self._calendars_dir = self.qlib_dir.joinpath(self.CALENDARS_DIR_NAME)
        self._features_dir = self.qlib_dir.joinpath(self.FEATURES_DIR_NAME)
        self._instruments_dir = self.qlib_dir.joinpath(self.INSTRUMENTS_DIR_NAME)

        self._calendars_list = []

        self._mode = self.ALL_MODE
        self._kwargs = {}

    def _backup_qlib_dir(self, target_dir: Path):
        shutil.copytree(str(self.qlib_dir.resolve()), str(target_dir.resolve()))

    def _format_datetime(self, datetime_d: [str, pd.Timestamp]):
        datetime_d = pd.Timestamp(datetime_d)
        return datetime_d.strftime(self.calendar_format)

    def _get_date(
        self, file_or_df: [Path, pd.DataFrame], *, is_begin_end: bool = False, as_set: bool = False
    ) -> Iterable[pd.Timestamp]:
        if not isinstance(file_or_df, pd.DataFrame):
            df = self._get_source_data(file_or_df)
        else:
            df = file_or_df
        if df.empty or self.date_field_name not in df.columns.tolist():
            _calendars = pd.Series(dtype=np.float32)
        else:
            _calendars = df[self.date_field_name]

        if is_begin_end and as_set:
            return (_calendars.min(), _calendars.max()), set(_calendars)
        elif is_begin_end:
            return _calendars.min(), _calendars.max()
        elif as_set:
            return set(_calendars)
        else:
            return _calendars.tolist()

    def _get_source_data(self, file_path: Path) -> pd.DataFrame:
        df = pd.read_csv(str(file_path.resolve()), low_memory=False)
        df[self.date_field_name] = df[self.date_field_name].astype(str).astype(np.datetime64)
        # df.drop_duplicates([self.date_field_name], inplace=True)
        return df

    def get_symbol_from_file(self, file_path: Path) -> str:
        return fname_to_code(file_path.name[: -len(self.file_suffix)].strip().lower())

    def get_dump_fields(self, df_columns: Iterable[str]) -> Iterable[str]:
        return (
            self._include_fields
            if self._include_fields
            else set(df_columns) - set(self._exclude_fields)
            if self._exclude_fields
            else df_columns
        )

    @staticmethod
    def _read_calendars(calendar_path: Path) -> List[pd.Timestamp]:
        return sorted(
            map(
                pd.Timestamp,
                pd.read_csv(calendar_path, header=None).loc[:, 0].tolist(),
            )
        )

    def _read_instruments(self, instrument_path: Path) -> pd.DataFrame:
        df = pd.read_csv(
            instrument_path,
            sep=self.INSTRUMENTS_SEP,
            names=[
                self.symbol_field_name,
                self.INSTRUMENTS_START_FIELD,
                self.INSTRUMENTS_END_FIELD,
            ],
        )

        return df

    def save_calendars(self, calendars_data: list):
        self._calendars_dir.mkdir(parents=True, exist_ok=True)
        calendars_path = str(self._calendars_dir.joinpath(f"{self.freq}.txt").expanduser().resolve())
        result_calendars_list = list(map(lambda x: self._format_datetime(x), calendars_data))
        np.savetxt(calendars_path, result_calendars_list, fmt="%s", encoding="utf-8")

    def save_instruments(self, instruments_data: Union[list, pd.DataFrame]):
        self._instruments_dir.mkdir(parents=True, exist_ok=True)
        instruments_path = str(self._instruments_dir.joinpath(self.INSTRUMENTS_FILE_NAME).resolve())
        if isinstance(instruments_data, pd.DataFrame):
            _df_fields = [self.symbol_field_name, self.INSTRUMENTS_START_FIELD, self.INSTRUMENTS_END_FIELD]
            instruments_data = instruments_data.loc[:, _df_fields]
            instruments_data[self.symbol_field_name] = instruments_data[self.symbol_field_name].apply(
                lambda x: fname_to_code(x.lower()).upper()
            )
            instruments_data.to_csv(instruments_path, header=False, sep=self.INSTRUMENTS_SEP, index=False)
        else:
            np.savetxt(instruments_path, instruments_data, fmt="%s", encoding="utf-8")

    def data_merge_calendar(self, df: pd.DataFrame, calendars_list: List[pd.Timestamp]) -> pd.DataFrame:
        # calendars
        calendars_df = pd.DataFrame(data=calendars_list, columns=[self.date_field_name])
        calendars_df[self.date_field_name] = calendars_df[self.date_field_name].astype(np.datetime64)
        cal_df = calendars_df[
            (calendars_df[self.date_field_name] >= df[self.date_field_name].min())
            & (calendars_df[self.date_field_name] <= df[self.date_field_name].max())
        ]
        # align index
        cal_df.set_index(self.date_field_name, inplace=True)
        df.set_index(self.date_field_name, inplace=True)
        r_df = df.reindex(cal_df.index)
        return r_df

    @staticmethod
    def get_datetime_index(df: pd.DataFrame, calendar_list: List[pd.Timestamp]) -> int:
        return calendar_list.index(df.index.min())

    def _data_to_bin(self, df: pd.DataFrame, calendar_list: List[pd.Timestamp], features_dir: Path):
        if df.empty:
            logger.warning(f"{features_dir.name} data is None or empty")
            return
        # align index
        _df = self.data_merge_calendar(df, calendar_list)
        # used when creating a bin file
        date_index = self.get_datetime_index(_df, calendar_list)
        for field in self.get_dump_fields(_df.columns):
            bin_path = features_dir.joinpath(f"{field.lower()}.{self.freq}{self.DUMP_FILE_SUFFIX}")
            if field not in _df.columns:
                continue
            if bin_path.exists() and self._mode == self.UPDATE_MODE:
                # update
                with bin_path.open("ab") as fp:
                    np.array(_df[field]).astype("<f").tofile(fp)
            else:
                # append; self._mode == self.ALL_MODE or not bin_path.exists()
                np.hstack([date_index, _df[field]]).astype("<f").tofile(str(bin_path.resolve()))

    def _dump_bin(self, file_or_data: [Path, pd.DataFrame], calendar_list: List[pd.Timestamp]):
        if isinstance(file_or_data, pd.DataFrame):
            if file_or_data.empty:
                return
            code = fname_to_code(str(file_or_data.iloc[0][self.symbol_field_name]).lower())
            df = file_or_data
        elif isinstance(file_or_data, Path):
            code = self.get_symbol_from_file(file_or_data)
            df = self._get_source_data(file_or_data)
        else:
            raise NotImplementedError(f"not support {type(file_or_data)}")
        if df is None or df.empty:
            logger.warning(f"{code} data is None or empty")
            return

        # try to remove dup rows or it will cause exception when reindex.
        df = df.drop_duplicates(self.date_field_name)
        if self.symbol_field_name in df.columns:
            df=df.drop(self.symbol_field_name, axis=1)

        # features save dir
        features_dir = self._features_dir.joinpath(code_to_fname(code).lower())
        features_dir.mkdir(parents=True, exist_ok=True)
        self._data_to_bin(df, calendar_list, features_dir)

    @abc.abstractmethod
    def dump(self):
        raise NotImplementedError("dump not implemented!")

    def __call__(self, *args, **kwargs):
        self.dump()

class DumpNumeric(DumpDataBase):
    def __init__(
        self,
        csv_path: str,
        qlib_dir: str,
        backup_dir: str = None,
        freq: str = "day",
        max_workers: int = 16,
        date_field_name: str = "date",
        file_suffix: str = ".csv",
        symbol_field_name: str = "symbol",
        exclude_fields: str = "",
        include_fields: str = "",
        limit_nums: int = None,
    ):
        """
        Parameters
        ----------
        csv_path: str
            stock data path or directory
        qlib_dir: str
            qlib(dump) data director
        backup_dir: str, default None
            if backup_dir is not None, backup qlib_dir to backup_dir
        freq: str, default "day"
            transaction frequency
        max_workers: int, default None
            number of threads
        date_field_name: str, default "date"
            the name of the date field in the csv
        file_suffix: str, default ".csv"
            file suffix
        symbol_field_name: str, default "symbol"
            symbol field name
        include_fields: tuple
            dump fields
        exclude_fields: tuple
            fields not dumped
        limit_nums: int
            Use when debugging, default None
        """
        csv_path = Path(csv_path).expanduser()
        if isinstance(exclude_fields, str):
            exclude_fields = exclude_fields.split(",")
        if isinstance(include_fields, str):
            include_fields = include_fields.split(",")
        self._exclude_fields = tuple(filter(lambda x: len(x) > 0, map(str.strip, exclude_fields)))
        self._include_fields = tuple(filter(lambda x: len(x) > 0, map(str.strip, include_fields)))
        self.file_suffix = file_suffix
        self.symbol_field_name = symbol_field_name
        
        #read dataframe
        self.csv=self._read(csv_path)

        if limit_nums is not None:
            selected_symbols=self.csv[self.symbol_field_name].drop_duplicates()[:limit_nums]
            self.csv=self.csv[self.csv[symbol_field_name].isin(selected_symbols)]
        self._check()
        self._group_by_symbol=self.csv.groupby(self.symbol_field_name)
        
        self.qlib_dir = Path(qlib_dir).expanduser()
        self.backup_dir = backup_dir if backup_dir is None else Path(backup_dir).expanduser()
        if backup_dir is not None:
            self._backup_qlib_dir(Path(backup_dir).expanduser())

        self.freq = freq
        self.calendar_format = self.DAILY_FORMAT if self.freq == "day" else self.HIGH_FREQ_FORMAT

        self.works = max_workers
        self.date_field_name = date_field_name

        self._calendars_dir = self.qlib_dir.joinpath(self.CALENDARS_DIR_NAME)
        self._features_dir = self.qlib_dir.joinpath(self.FEATURES_DIR_NAME)
        self._instruments_dir = self.qlib_dir.joinpath(self.INSTRUMENTS_DIR_NAME)

        self._calendars_list = []

        self._mode = self.ALL_MODE
        self._kwargs = {}
    
    def _get_catagory_fields(self):
        dtypes=self.csv.dtypes.apply(str)
        catagory_fields=dtypes[~dtypes.isin(numeric_types)]
        catagory_fields=catagory_fields[~catagory_fields.index.isin([self.symbol_field_name, self.date_field_name])]
        return catagory_fields.index

    def _check(self):
        # cast each cols' dtype into that can be dump bin
        for col in self._get_catagory_fields():
            if col not in self._exclude_fields:
                raise NotImplementedError('column {} is not numeric! Please use the Class DumpNumericCatagory that support catagory columns'.format(col))
                
    def _get_date(self, group, as_set=True, is_begin_end=True):
        symbol, symbol_df=group
        return str(symbol).strip().lower(), super()._get_date(symbol_df, as_set=as_set,is_begin_end=is_begin_end)
    
    def _read(self, path):
        path=str(path)
        if path.endswith('csv'):
            return pd.read_csv(path)
        elif path.endswith('parquet'):
            return pd.read_parquet(path)
        else:
            raise NotImplementedError('not support for this file format!')
    
    def _get_all_date(self):
        logger.info("start get all date......")
        all_datetime = set()
        date_range_list = []
        _begin=self._group_by_symbol.apply(lambda x:x[self.date_field_name].min())
        _end=self._group_by_symbol.apply(lambda x:x[self.date_field_name].max())
        _begin_end=pd.DataFrame(dict(begin=_begin, end=_end)).reset_index()
        with tqdm(total=len(_begin_end)) as p_bar:
            for symbol, _begin_time, _end_time in zip(
                _begin_end[self.symbol_field_name],_begin_end['begin'],_begin_end['end']
                ):
                if isinstance(_begin_time, pd.Timestamp) and isinstance(_end_time, pd.Timestamp):
                    _begin_time = self._format_datetime(_begin_time)
                    _end_time = self._format_datetime(_end_time)
                    symbol=str(symbol).strip().lower()
                    _inst_fields = [symbol.upper(), _begin_time, _end_time]
                    date_range_list.append(f"{self.INSTRUMENTS_SEP.join(_inst_fields)}")
                p_bar.update()
        all_datetime = set(self.csv[self.date_field_name])
        self._kwargs["all_datetime_set"] = all_datetime
        self._kwargs["date_range_list"] = date_range_list
        logger.info("end of get all date.\n")

    def _dump_calendars(self):
        logger.info("start dump calendars......")
        self._calendars_list = sorted(map(pd.Timestamp, self._kwargs["all_datetime_set"]))
        self.save_calendars(self._calendars_list)
        logger.info("end of calendars dump.\n")

    def _dump_instruments(self):
        logger.info("start dump instruments......")
        self.save_instruments(self._kwargs["date_range_list"])
        logger.info("end of instruments dump.\n")
    
    def _dump_bin(self, group, calendar_list):
        _,df=group
        super()._dump_bin(df,calendar_list)

    def _dump_features(self):
        logger.info("start dump features......")

        with tqdm(total=len(self._group_by_symbol)) as p_bar:
            for group in self._group_by_symbol:
                self._dump_bin(group, self._calendars_list)
                p_bar.update()
        #_dump_func = partial(self._dump_bin, calendar_list=self._calendars_list)
        #with tqdm(total=len(self._group_by_symbol)) as p_bar:
        #    with ProcessPoolExecutor(max_workers=self.works) as executor:
        #        for _ in executor.map(_dump_func, self._group_by_symbol):
        #            p_bar.update()
        logger.info("end of features dump.\n")

    def dump(self):
        self._get_all_date()
        self._dump_calendars()
        self._dump_instruments()
        self._dump_features()
        
class DumpNumericCatagory(DumpNumeric):
    CATAGORY_DIR_NAME='catagories'
    SYMBOL_FILE='symbol_fileds.txt'
    DATE_FILE='date_field.txt'
    CATAGORY_DTYPE_FILE='catagory_dtypes.txt'
    
    def __init__(
        self,
        csv_path: str,
        qlib_dir: str,
        backup_dir: str = None,
        freq: str = "day",
        max_workers: int = 16,
        date_field_name: str = "date",
        file_suffix: str = ".csv",
        symbol_field_name: str = "symbol",
        exclude_fields: str = "",
        include_fields: str = "",
        limit_nums: int = None,
    ):
        """
        Parameters
        ----------
        csv_path: str
            stock data path or directory
        qlib_dir: str
            qlib(dump) data director
        backup_dir: str, default None
            if backup_dir is not None, backup qlib_dir to backup_dir
        freq: str, default "day"
            transaction frequency
        max_workers: int, default None
            number of threads
        date_field_name: str, default "date"
            the name of the date field in the csv
        file_suffix: str, default ".csv"
            file suffix
        symbol_field_name: str, default "symbol"
            symbol field name
        include_fields: tuple
            dump fields
        exclude_fields: tuple
            fields not dumped
        limit_nums: int
            Use when debugging, default None
        """
        csv_path = Path(csv_path).expanduser()
        if isinstance(exclude_fields, str):
            exclude_fields = exclude_fields.split(",")
        if isinstance(include_fields, str):
            include_fields = include_fields.split(",")
        self._exclude_fields = tuple(filter(lambda x: len(x) > 0, map(str.strip, exclude_fields)))
        self._include_fields = tuple(filter(lambda x: len(x) > 0, map(str.strip, include_fields)))
        self.file_suffix = file_suffix
        ## key fields
        if isinstance(symbol_field_name, str):
            symbol_field_name = symbol_field_name.split(",")
        self.symbol_field_tuple = tuple(filter(lambda x: len(x) > 0, map(str.strip, symbol_field_name)))
        self.date_field_name = date_field_name

        self.csv=self._read(csv_path)
        self.symbol_field_name=self._get_symbol_field_name()
        
        if limit_nums is not None:
            seleted_symbols=self.csv[self.symbol_field_name].drop_duplicates()[:limit_nums]
            self.csv=self.csv[self.csv[self.symbol_field_name].isin(seleted_symbols)]
        self._group_by_symbol=self.csv.groupby(self.symbol_field_name, as_index=True)

        self.qlib_dir = Path(qlib_dir).expanduser()
        self.qlib_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir = backup_dir if backup_dir is None else Path(backup_dir).expanduser()
        if backup_dir is not None:
            self._backup_qlib_dir(Path(backup_dir).expanduser())

        self.freq = freq
        self.calendar_format = self.DAILY_FORMAT if self.freq == "day" else self.HIGH_FREQ_FORMAT
        self.works = max_workers
        

        self._calendars_dir = self.qlib_dir.joinpath(self.CALENDARS_DIR_NAME)
        self._features_dir = self.qlib_dir.joinpath(self.FEATURES_DIR_NAME)
        self._instruments_dir = self.qlib_dir.joinpath(self.INSTRUMENTS_DIR_NAME)
        self._catagory_dir=self.qlib_dir.joinpath(self.CATAGORY_DIR_NAME)

        self._catagories_list=[]
        self._calendars_list = []

        self._mode = self.ALL_MODE
        self._kwargs = {}

        self.log_symbol_date_filed()
    
    def log_symbol_date_filed(self):
        key_list=','.join(self.symbol_field_tuple)
        logger.info(f"Using symbol field {key_list}.\n")
        np.savetxt(self.qlib_dir.joinpath(self.SYMBOL_FILE),[key_list], fmt="%s", encoding="utf-8")
        
        logger.info(f"Using date field {self.date_field_name}.\n")
        np.savetxt(self.qlib_dir.joinpath(self.DATE_FILE),[self.date_field_name], fmt="%s", encoding="utf-8")

    def _get_symbol_field_name(self):
        if len(self.symbol_field_tuple)==0:
            raise ValueError("symbol field name must be specified! ")
        elif len(self.symbol_field_tuple)==1:
            return self.symbol_field_tuple[0]
        else:
            #merge multi cols into one col
            symbol_field_name="_".join(self.symbol_field_tuple)
            self.csv[symbol_field_name]=""
            for field in self.symbol_field_tuple:
                self.csv[symbol_field_name]+= "_"+self.csv[field]
            self.csv[symbol_field_name]=self.csv[symbol_field_name].apply(lambda x:x[1:])
            return symbol_field_name

    def _get_all_catagory(self):
        logger.info("start get all catagory......")
        all_catagory={}
        all_catagory_types=[]
        for col in self._get_catagory_fields():
            series=self.csv[col]
            _type_fileds=[col,str(series.dtype)]
            all_catagory_types.append(f"{self.CATAGORIES_SEP.join(_type_fileds)}")
            if series.dtype == 'datetime64[ns]':
                all_catagory[col]=sorted(map(pd.Timestamp,self.csv[col].drop_duplicates()))
            elif series.dtype=='object':
                all_catagory[col]=sorted(map(str,self.csv[col].drop_duplicates()))
            else:
                raise NotImplementedError('Not support for this type')

        self._kwargs['all_catagory']=all_catagory
        self._kwargs['all_catagory_dtypes']=all_catagory_types
        logger.info("end of get all catagory.\n")
    
    def _dump_catagories(self):
        logger.info("start dump catagories......")
        self._catagory_dir.mkdir(parents=True, exist_ok=True)
        for cat, cat_list in self._kwargs['all_catagory'].items():
            cat_path = str(self._catagory_dir.joinpath(f"{cat}.{self.freq}.txt").expanduser().resolve())
            np.savetxt(cat_path, cat_list, fmt="%s", encoding="utf-8")
        cat_dtype_paths=str(self.qlib_dir.joinpath(self.CATAGORY_DTYPE_FILE).expanduser().resolve())
        np.savetxt(cat_dtype_paths, self._kwargs['all_catagory_dtypes'], fmt="%s", encoding="utf-8")
        logger.info("end of catagories dump.\n")
        
    def _convert_catagory_features(self):
        logger.info("start convert catagories to index......")
        for col, col_list in self._kwargs['all_catagory'].items():
            cat2index=dict(zip(col_list,range(len(col_list))))
            self.csv[col]=self.csv[col].map(cat2index)
        logger.info("end of conversion catagories to index.\n")

    def dump(self):
        self._get_all_date()
        self._get_all_catagory()
        self._convert_catagory_features()
        self._dump_calendars()
        self._dump_catagories()
        self._dump_instruments()
        self._dump_features()

if __name__ == "__main__":
    fire.Fire({
        "dump_numeric":DumpNumeric,
        "dump_all":DumpNumericCatagory,
        "dump_numeric_catagory":DumpNumericCatagory
    })