rm -f sell_info.csv
sqlite3 -header -csv lianjia-shanghai.db "select * from sellinfo;" > sell_info.csv
rm -f community_info.csv
sqlite3 -header -csv lianjia-shanghai.db "select * from community;" > community_info.csv
