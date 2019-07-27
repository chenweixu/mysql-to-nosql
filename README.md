# mysql-to-nosql

从mysql读取日志使用 pymysqlreplication

## 数据流
mysql --> kafka --> influxdb

## 目前存在问题
1. pymysqlreplication 消费mysql日志有些慢，数据量太大时跟不上入库进度；
    后续计划也拆分为IO进程和SQL进程；
