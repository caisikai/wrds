<!-- vscode-markdown-toc -->
* 1. [Dump bin wrds dataset](#Dumpbinwrdsdataset)
	* 1.1. [Dump bin CRSP monthly stocks](#DumpbinCRSPmonthlystocks)
	* 1.2. [Compustat fundamentals](#Compustatfundamentals)
	* 1.3. [Compustat price data](#Compustatpricedata)
	* 1.4. [Compustat Global fundamentals](#CompustatGlobalfundamentals)
	* 1.5. [Compustat Global price data](#CompustatGlobalpricedata)
	* 1.6. [Compustat Global FX rates](#CompustatGlobalFXrates)
* 2. [Check dump](#Checkdump)
	* 2.1. [Dump bin CRSP monthly stocks](#DumpbinCRSPmonthlystocks-1)
	* 2.2. [Compustat fundamentals](#Compustatfundamentals-1)
	* 2.3. [Compustat price data](#Compustatpricedata-1)
	* 2.4. [Compustat Global fundamentals](#CompustatGlobalfundamentals-1)
	* 2.5. [Compustat Global price data](#CompustatGlobalpricedata-1)

<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc --><!-- vscode-markdown-toc -->

##  1. <a name='Dumpbinwrdsdataset'></a>Dump bin wrds dataset

###  1.1. <a name='DumpbinCRSPmonthlystocks'></a>Dump bin CRSP monthly stocks    

```bash
python dump_single.py  dump_all --csv_path  /storage/wrds/crsp/sasdata/a_stock/msf.parquet  --qlib_dir /storage/qlib/qlib_data/wrds/crsp/a_stock/msf --date_field_name date --symbol_field_name permno
```

###  1.2. <a name='Compustatfundamentals'></a>Compustat fundamentals 
```bash
python dump_single.py dump_all --csv_path /storage/wrds/comp/sasdata/naa/funda.parquet --qlib_dir /storage/qlib/qlib_data/wrds/comp/naa/funda --date_field_name datadate --symbol_field_name gvkey,indfmt,datafmt,consol,popsrc
```

###  1.3. <a name='Compustatpricedata'></a>Compustat price data
```bash
python dump_single.py dump_all --csv_path /storage/wrds/comp/sasdata/naa/secm.parquet --qlib_dir /storage/qlib/qlib_data/wrds/comp/naa/secm --date_field_name datadate --symbol_field_name gvkey,iid
```

###  1.4. <a name='CompustatGlobalfundamentals'></a>Compustat Global fundamentals
```bash
python dump_single.py dump_all --csv_path /storage/wrds/comp/sasdata/d_global/g_funda.parquet --qlib_dir /storage/qlib/qlib_data/wrds/comp/d_global/g_funda --date_field_name gvkey,indfmt,datafmt,consol,popsrc --symbol_field_name datadate
```

###  1.5. <a name='CompustatGlobalpricedata'></a>Compustat Global price data 
```bash
TODO
```
###  1.6. <a name='CompustatGlobalFXrates'></a>Compustat Global FX rates    
```bash
python dump_single.py dump_all --csv_path /storage/wrds/comp/sasdata/d_global/currency/g_exrt_mth.parquet --qlib_dir /storage/qlib/qlib_data/wrds/comp/d_global/currency/g_exrt_mth --date_field_name datadate --symbol_field_name tocurm
```


##  2. <a name='Checkdump'></a>Check dump

###  2.1. <a name='DumpbinCRSPmonthlystocks-1'></a>Dump bin CRSP monthly stocks    


###  2.2. <a name='Compustatfundamentals-1'></a>Compustat fundamentals 


###  2.3. <a name='Compustatpricedata-1'></a>Compustat price data


###  2.4. <a name='CompustatGlobalfundamentals-1'></a>Compustat Global fundamentals


###  2.5. <a name='CompustatGlobalpricedata-1'></a>Compustat Global price data 
