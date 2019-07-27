#!/usr/bin/env python3
import sys
import time
import os
import signal
import json
import yaml
from pathlib import Path
from datetime import datetime, date
from multiprocessing import Process, Queue
from lib.mylog import My_log
from pykafka import KafkaClient
from pymysqlreplication import BinLogStreamReader
from lib.daemon import daemon


class ComplexEncoder(json.JSONEncoder):
    """为了将mysql的时间类型字段序列化"""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


class ReadDBEvent(Process):
    """从数据库复制binlog"""

    def __init__(self, queue):
        super(ReadDBEvent, self).__init__()
        self.queue = queue
        self.read_insert_data_count = 0
        self.binlog_file = ""
        self.binlog_pos = 0
        self.tables = conf_data("mysql", "tables")
        self.table_head = conf_data("mysql", "table_head")

    def _update_registry(self, file=None, pos=None):
        """修改偏移量"""
        if not file:
            self.binlog_pos = pos
        else:
            self.binlog_file = file
            self.binlog_pos = pos
        work_log.debug(f"_update_registry: {self.binlog_file}:{self.binlog_pos}")

    def _save_registry_file(self):
        """保存偏移量到文件"""
        data = self.binlog_file + ":" + str(self.binlog_pos)
        with open(registry_file, "w", encoding="utf-8") as f:
            work_log.info(f"save registry data to file success: {data}")
            f.write(data)

    def save_ReadDBEvent_env(self, signum, frame):
        """退出程序时，保存环境"""
        work_log.info("-------exit Process save_ReadDBEvent_env----------")
        work_log.info(signum)
        work_log.info(frame)
        work_log.info("-------exit Process save_ReadDBEvent_env----------")
        self._save_registry_file()
        sys.exit()

    def get_run_log_pos(self):
        """启动程序时，获取初始化的偏移量"""
        if START_BINLOG_REGISTRY:
            self._update_registry(
                file=START_BINLOG_REGISTRY[0], pos=START_BINLOG_REGISTRY[1]
            )
            return START_BINLOG_REGISTRY
        else:
            try:
                with open(registry_file, "r", encoding="utf-8") as f:
                    data = f.read()
            except FileNotFoundError:
                work_log.error("mysql_registry file not found")
                sys.exit()
            except Exception as e:
                raise e
            if data:
                BinLogFile = data.split(":")[0]
                BinLogPos = int(data.split(":")[1])
                self._update_registry(file=BinLogFile, pos=BinLogPos)
            return [BinLogFile, BinLogPos]

    def RotateEvent(self, binlogevent):
        """RotateEvent 更换日志文件事件"""
        # binlogevent.dump()
        self._update_registry(binlogevent.next_binlog, binlogevent.position)
        self._save_registry_file()
        work_log.info(f"RotateEvent: {binlogevent.next_binlog}, {binlogevent.position}")

    def XidEvent(self, binlogevent):
        """XidEvent 事务结束后的类型，主要是为了记录偏移量"""
        # binlogevent.dump()
        binlog_pos = binlogevent.packet.log_pos
        work_log.debug(f"XidEvent: {self.binlog_file}, {self.binlog_pos}")
        self._update_registry(pos=binlog_pos)

    def user_login_table(self, binlogevent):
        for row in binlogevent.rows:
            event = {
                "db": binlogevent.schema,
                "table": binlogevent.table,
                "log_pos": binlogevent.packet.log_pos,
                "action": "insert",
                "values": row["values"],
            }

            data = json.dumps(event, cls=ComplexEncoder)
        return data

    def WriteRowsEvent(self, binlogevent):
        """WriteRowsEvent 插入事务"""
        # binlogevent.dump()

        if binlogevent.table not in self.tables and not list(
            filter(binlogevent.table.startswith, self.table_head)
        ):
            # 如果不匹配指定表名，或不以指定表名开头则忽略此事件
            return

        try:
            data = self.user_login_table(binlogevent)
            # work_log.debug('insert data')
            # work_log.debug(data)
        except Exception as e:
            work_log.error("row data to json error")
            work_log.error(str(e))
            return False
        try:
            self.queue.put(data, block=False)
            write_queue_status = True           # 写入queue成功
        except Exception as e:
            # 没有处理好写入失败的逻辑
            write_queue_status = False          # 写入queue失败
            work_log.error(str(e))
            if self.queue.qsize() >= 30000:
                # 如果queue中数据大于3万条，则等待5s再写数据
                work_log.error("queue full >= 30000, sleep 5s")
                time.sleep(5)
                self.queue.put(data, block=False)
                write_queue_status = True
            else:
                work_log.error("queue full not >= 30000")
                self.queue.put(data)
                write_queue_status = True

        if write_queue_status:
            self.read_insert_data_count += 1
            if self.read_insert_data_count >= 3000:
                # 每生产3000条数据记录一次日志
                work_log.info("insert event to queue +3000")
                self.read_insert_data_count = 0

    def run(self):
        signal.signal(signal.SIGTERM, self.save_ReadDBEvent_env)
        # 子进程只注册 kill 15 信号，以保存环境
        work_log.info("----------------ReadDBEvent run------------")
        binlog_file, binlog_pos = self.get_run_log_pos()
        work_log.info("ReadDBEvent pid: " + str(os.getpid()))
        work_log.info(f"start binlogfile: {binlog_file}, binlog_pos: {binlog_pos}")

        mysql_settings = conf_data("mysql", "settings")
        db_name = conf_data("mysql", "dbname")
        server_id = conf_data("mysql", "server_id")
        stream = BinLogStreamReader(
            connection_settings=mysql_settings,
            server_id=server_id,
            blocking=True,
            resume_stream=True,
            only_schemas=[db_name],
            log_file=binlog_file,
            log_pos=binlog_pos,
        )

        try:
            for binlogevent in stream:
                binlogevent.dump()
                if binlogevent.event_type == 4:
                    self.RotateEvent(binlogevent)
                elif binlogevent.event_type == 16:
                    self.XidEvent(binlogevent)
                elif binlogevent.event_type == 30:
                    try:
                        self.WriteRowsEvent(binlogevent)
                    except Exception as e:
                        work_log.error("WriteRowsEvent error")
                        work_log.error(str(e))
                else:
                    pass
        except Exception as e:
            work_log.error("exit error")
            work_log.error(str(e))
            self._save_registry_file()

        stream.close()
        sys.exit()


class WriteMq(Process):
    '''将数据从queue中读取然后写入MQ'''
    def __init__(self, queue):
        super(WriteMq, self).__init__()
        self.queue = queue

        self.kafka_host = conf_data("kafka", "host")
        self.topic_name = conf_data("kafka", "topic")

    def save_WriteMq_env(self, signum, frame):
        '''保存程序退出时的环境'''
        work_log.info("-------save_WriteMq_env.1------------")
        # sys.exit()
        work_log.info(signum)
        work_log.info(frame)
        if self.queue.empty():
            # 队列为空，已经全部数据都写入 MQ
            work_log.info("write mq read queue not data; exit")
            work_log.info("-------save_WriteMq_env.1------------")
            sys.exit()
        else:
            # 队列不为空，还有数据没有完成写入 MQ
            work_log.info("wait queue data >> mq")
            work_log.info("wait queue data = %s" % (self.queue.qsize(),))

            kafka_client = KafkaClient(hosts=self.kafka_host)
            work_log.info(kafka_client.topics)
            topic = kafka_client.topics[self.topic_name]

            # async_queue = topic.get_producer(sync=False)
            async_queue = topic.get_producer(sync=True)         # 以同步方式继续写入
            work_log.info("sync wirte log to mq")
            while 1:
                try:
                    data = self.queue.get()
                    async_queue.produce(data.encode())
                except Exception as e:
                    work_log.error(str(e))
            sys.exit()

    def run(self):
        try:
            signal.signal(signal.SIGTERM, self.save_WriteMq_env)
            # 子进程只注册 kill 15 信号，以保存环境
            work_log.info("----------------WriteMq run------------")
            work_log.info("WriteMq pid: " + str(os.getpid()))

            kafka_client = KafkaClient(hosts=self.kafka_host)
            work_log.info(kafka_client.topics)
            topic = kafka_client.topics[self.topic_name]

            async_queue = topic.get_producer(sync=False)
            # async_queue = topic.get_producer(sync=True)
        except Exception as e:
            work_log.error("wwrite mq env error")
            work_log.error(str(e))

        write_mq_count = 0
        try:
            work_log.info("read queue start")
            # partitions = topic.partitions
            # last_offset = topic.latest_available_offsets()
            # print("最近可用offset {}".format(last_offset))  # 查看所有分区

            while 1:
                try:
                    data = self.queue.get()
                    work_log.debug(data)
                    async_queue.produce(data.encode())      # 异步方式写入
                    write_mq_count += 1
                    if write_mq_count >= 3000:
                        write_mq_count = 0
                        work_log.info("write data to mq +3000")
                        work_log.info("current queue size = " + str(self.queue.qsize()))
                except Exception as e:
                    work_log.error(str(e))

        except Exception as e:
            work_log.info("exit WriteMq pid: " + str(os.getpid()))
            work_log.error(str(e))
            sys.exit()
        work_log.info("exit WriteMq pid: " + str(os.getpid()))
        sys.exit()


class work_daemon(daemon):
    """docstring for work_daemon"""

    def __init__(self, pidfile):
        daemon.__init__(self, pidfile)
        self.process_list = []

    def work_task_exit(self, signum, frame):
        work_log.info("-------work_task_exit----------")
        for i in self.process_list:
            work_log.info(i)
            i.terminate()
        sys.exit()

    def run(self):
        work_log.info("Work_task start")
        work_log.info("master pid: " + str(os.getpid()))
        queue = Queue(maxsize=40000)
        work_log.info("queue maxsize=40000")
        read_mysql = ReadDBEvent(queue)
        read_mysql.start()
        write_mq = WriteMq(queue)
        write_mq.start()

        self.process_list.append(read_mysql)
        self.process_list.append(write_mq)

        signal.signal(signal.SIGINT, self.work_task_exit)   # Ctrl-c
        signal.signal(signal.SIGTERM, self.work_task_exit)  # kill
        work_log.info("work_task_exit signal")

        read_mysql.join()
        write_mq.join()


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
        sys.exit()


if __name__ == "__main__":
    START_BINLOG_REGISTRY = []
    # START_BINLOG_REGISTRY = ["mysql-binlog.005370", 487381314]
    # START_BINLOG_REGISTRY = ["mysql-binlog.000001", 154]

    work_dir = Path(__file__).resolve().parent
    registry_file = work_dir / "mysql_registry"
    pidfile = work_dir / "log/tokafka.pid"
    logfile = work_dir / "log/tokafka.log"
    log_level = conf_data("log_level")
    work_log = My_log(logfile, log_level).get_log()
    main()

