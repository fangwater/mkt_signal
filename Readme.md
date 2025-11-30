# 导出数据
bash scripts/export_all.sh 
默认导出到当前目录export_data文件夹

# 获取数据
python scripts/export_symbol_data.py  --symbol SAHARAUSDT --dir export_data --output saharasudt.parquet
