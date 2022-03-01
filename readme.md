wrds

1. TODO List
 - [x]  改好scripts/dump_single/check_dump_single.py
 - [x]  在data/wrds_handler.py写好handler类
 - [x]  dump文档
 - [ ]  check文档
 - [x]  服务器数据放在/storage/qlib/qlib_data/wrds/
 - [ ]  写好main.ipynb,展示每个数据集的dataset.prepare结果，数据太大情况下加载太久的话，可以选取观测period最长的100只instruments作为market（存一个test100.txt到每个数据集的instruments, test100.txt已经有了）
2. 数据集
   
| 数据集                        | dump | check | main.ipynb |
| ---------------------------- | ---- | ---- | ----|
| CRSP monthly stocks           | ✅    | ✅ |⬜️  |
| Compustat fundamentals        | ✅    | ⬜️ |⬜️  |
| Compustat price data          | ✅    | ⬜️ |⬜️  |
| Compustat Global fundamentals | ✅    | ✅ |⬜️  |
| Compustat Global price data   | ⬜️    | ⬜️ |⬜️  |
| Compustat Global FX rates     | ✅    | ⬜️ |⬜️  |