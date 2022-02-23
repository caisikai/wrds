
import os
import re
from typing import Tuple, Union, List
import pandas as pd

from qlib.config import C
from qlib.data.dataset.loader import QlibDataLoader

class WRDSDataLoader(QlibDataLoader):
    def __init__(
        self,
        config: Tuple[list, tuple, dict],
        filter_pipe: List = None,
        swap_level: bool = True,
        freq: Union[str, dict] = "day",
        inst_processor: dict = None,
    ):
        """
        Parameters
        ----------
        config : Tuple[list, tuple, dict]
            Please refer to the doc of DLWParser
        filter_pipe :
            Filter pipe for the instruments
        swap_level :
            Whether to swap level of MultiIndex
        freq:  dict or str
            If type(config) == dict and type(freq) == str, load config data using freq.
            If type(config) == dict and type(freq) == dict, load config[<group_name>] data using freq[<group_name>]
        inst_processor: dict
            If inst_processor is not None and type(config) == dict; load config[<group_name>] data using inst_processor[<group_name>]
        """
        super().__init__(config, filter_pipe, swap_level, freq,inst_processor)
        
        data_uri=[uri for uri in C.dpm.provider_uri.values()][0]
        try:
            self.catagory_mappers=self.get_mapper_dict(data_uri)
            self.catagory_fields=list(self.catagory_mappers.keys())
        except:
            self.catagory_mappers=None
            self.catagory_fields=None
    
    def get_mapper_dict(self, uri: str):

        catagory_dict={}
        dir=[uri for uri in C.dpm.provider_uri.values()][0]+'/catagories'
        for file in os.listdir(dir):
            field= file.split('.')[0]
            with open(dir+'/'+file, 'r') as f:
                content=f.read().splitlines()
            catagory_dict[field]=dict(zip(range(len(content)), content))
        return catagory_dict
    
    def load_group_df(
        self,
        instruments,
        exprs: list,
        names: list,
        start_time: Union[str, pd.Timestamp] = None,
        end_time: Union[str, pd.Timestamp] = None,
        gp_name: str = None,
    ) -> pd.DataFrame:
        self.check(exprs)
        df=super().load_group_df(instruments, exprs, names, start_time, end_time, gp_name)
        df=self.map(df)
        return  df
    
    
    def check(self, exprs: list,):
        """
        promise no operator applied on catagory fileds

        Args:
            exprs (list): _description_
        """
        if self.catagory_fields is None:
            return
        for catafory_field_i in self.catagory_fields:
            for expr in exprs:
                allfields=re.findall("\$[a-z]+", "$auth",flags=0)
                
                if "$"+catafory_field_i in allfields and expr!= f"${catafory_field_i}":
                    raise ValueError(f"Not Support Operator on Catagory_field {catafory_field_i} in expression {expr}!")
                
    def map(self, df: pd.DataFrame)-> pd.DataFrame:
        """
        apply self.catagory_mappers on df for each catagory field

        Args:
            df (pd.DataFrame): catagory filed is key

        Returns:
            pd.DataFrame: catagory filed is value
        """        
        
        if self.catagory_mappers is None:
            return df
        for catafory_field_i in self.catagory_fields: 
            if catafory_field_i in df.columns:
                df[catafory_field_i]=df[catafory_field_i].apply(lambda x:self.catagory_mappers[catafory_field_i].get(x))
        return df