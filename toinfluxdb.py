#!/usr/bin/env python3

import sys
import json
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from influxdb import InfluxDBClient
from pykafka import KafkaClient
from lib.daemon import daemon
from lib.mylog import My_log


def user_login_data(values):
    """补充时间精度，减少 tags 的重复,日志时间+表的ID值(取模转换为毫秒)
    存在的限制：在相同时间戳时，如果元数据相同，数据会被覆盖；
    如果数据库同一秒内写入1001条数据，且第1条和1001条数据元数据一致(相同SPID和PROVINCE)则会发生"""

    old_time = datetime.strptime(values.get("LOGIN_TIME"), "%Y-%m-%d %H:%M:%S")

    add_time = int(values.get("ID")) % 1000
    new_time = old_time + timedelta(milliseconds=add_time)
    utc_data = datetime.utcfromtimestamp(new_time.timestamp())

    try:
        spid = int(values.get("SPID"))
    except (TypeError, ValueError):
        spid = 99999

    try:
        province = int(values.get("PROVINCE"))
    except (TypeError, ValueError):
        province = 99999

    user_type = int(values.get("LOGIN_USER_TYPE"))
    result = int(values.get("RESULT"))
    cost = int(values.get("COST"))

    ts_data = [
        {
            "measurement": "user_login",
            "time": utc_data,
            "tags": {"spid": spid, "province": province, "user_type": user_type},
            "fields": {"cost": cost, "result": result},
        }
    ]
    return ts_data


def task_work():
    tsdb_conf = conf_data("influxdb")
    host = tsdb_conf("host")
    port = tsdb_conf("port")
    user = tsdb_conf("user")
    passwd = tsdb_conf("passwd")
    db = tsdb_conf("db")

    tsdb = InfluxDBClient(host, port, user, passwd, db)

    kafka_host = conf_data("kafka", "host")
    topic_name = conf_data("kafka", "topic")
    group_id = conf_data("kafka", "consumer_group_id")

    kafka_client = KafkaClient(hosts=kafka_host)
    topic = kafka_client.topics[topic_name]

    # partitions = topic.partitions
    # last_offset = topic.latest_available_offsets()
    # print("最近可用offset {}".format(last_offset))  # 查看所有分区

    consumer = topic.get_balanced_consumer(group_id, managed=True)
    # managed=True 设置后，使用新式reblance分区方法，
    # 不需要使用zk，而False是通过zk来实现reblance的需要使用zk
    # 自动提交不重复消费

    last_offset = topic.latest_available_offsets()
    # print("最近可用offset {}".format(last_offset))  # 查看所有分区
    work_log.info("最近可用offset {}".format(last_offset))

    data_count = 0
    for message in consumer:
        if message is not None:
            data = json.loads(message.value.decode())
            work_log.debug(str(data))
            try:
                t_name = data.get("table")
                action = data.get("action")
                if (t_name.startswith("TL_USER_LOGIN_LOG_") and action == "insert"):
                    values = data.get("values")
                    try:
                        ts_data = user_login_data(values)
                    except Exception as e:
                        work_log.error("user_login_data --> error")
                        work_log.error(str(e))

                    work_log.debug(str(ts_data))
                    tsdb.write_points(ts_data)
                    data_count += 1
                    if data_count >= 3000:
                        work_log.info("insert tsdb +3000")
                        data_count = 0
                else:
                    work_log.error('t_name not match')
                    work_log.info(f"t_name: {t_name}, action: {action}")
            except Exception as e:
                work_log.error(str(e))
        else:
            work_log.error("message is None")


def work_start():
    try:
        task_work()
    except Exception as e:
        work_log.error("task_work error")
        work_log.error(str(e))


class work_daemon(daemon):
    """docstring for work_daemon"""

    def run(self):
        work_start()


def conf_data(style, age=None):
    conf_file = work_dir / "conf.yaml"
    data = yaml.load(conf_file.read_text(), Loader=yaml.FullLoader)
    if not age:
        return data.get(style)
    else:
        return data.get(style).get(age)


def main():
    if len(sys.argv) == 2:
        daemon = work_daemon(pidfile)
        if "start" == sys.argv[1]:
            work_log.info("------admin start daemon run ")
            daemon.start()
        elif "stop" == sys.argv[1]:
            work_log.info("------admin stop")
            daemon.stop()
        elif "restart" == sys.argv[1]:
            work_log.info("------admin restart")
            daemon.restart()
        else:
            print("unkonow command")
            sys.exit(2)
        sys.exit(0)
    elif len(sys.argv) == 1:
        work_start()


if __name__ == "__main__":
    work_dir = Path(__file__).resolve().parent
    pidfile = work_dir / "log/totsdb.pid"
    logfile = work_dir / "log/totsdb.log"
    log_level = conf_data("log_level")
    work_log = My_log(logfile, log_level).get_log()
    main()
