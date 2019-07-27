import os
import sys
from atexit import register
from signal import SIGTERM
from time import sleep


class daemon(object):
    """用于生成一个后台进程的类
    """

    def __init__(
        self, pidfile,
        stdin="/dev/null",
        stdout="/dev/null",
        stderr="/dev/null"
    ):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        # 关闭守护进程的输出

    def daemonize(self):
        # 第一次fork，生成子进程，脱离父进程
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
                # exit first parent
        except OSError as e:
            sys.stderr.write("fork fist faild:%d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        # 修改工作目录
        os.setsid()
        # 设置新的会话连接
        os.umask(0)
        # 重新设置文件创建权限

        # 第二次fork，禁止进程打开终端
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            sys.stderr.write("fork second faild:%d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        sys.stdout.flush()
        sys.stderr.flush()

        # 重定向标准输入、输出、错误输出
        si = open(self.stdin, "r")
        so = open(self.stdout, "a+")
        se = open(self.stderr, "a+")
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # with open(self.stdin, 'r') as si:
        #     os.dup2(si.fileno(), sys.stdin.fileno())
        # with open(self.stdin, 'a+') as so:
        #     os.dup2(so.fileno(), sys.stdout.fileno())
        # with open(self.stdin, 'a+') as se:
        #     os.dup2(se.fileno(), sys.stderr.fileno())

        # 注册退出函数
        register(self.delpid)
        pid = str(os.getpid())
        with open(self.pidfile, "w+") as f:
            f.write("%s\n" % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        # Check for a pidfile to see if the daemon already runs
        try:
            with open(self.pidfile, "r") as f:
                pid = int(f.read().strip())
        except IOError:
            pid = None

        if pid:
            ms = "pidfile %s already exist,daemon already running\n"
            sys.stderr.write(ms % self.pidfile)
            sys.exit(1)

        # Start the daemon
        self.daemonize()
        self.run()

    def stop(self):
        # Get the pid from the pidfile
        try:
            with open(self.pidfile, "r") as f:
                pid = int(f.read().strip())
        except IOError:
            pid = None

        if not pid:
            ms = "pidfile %s does not exit,daemon not running\n"
            sys.stderr.write(ms % self.pidfile)
            return

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                sleep(0.1)
                os.remove(self.pidfile)
        except OSError as err:
            err = str(err)
            if err.find("No sush process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
                else:
                    print(str(err))
                    sys.exit(1)

    def restart(self):
        self.stop()
        self.start()

    def run(self):
        # 该方法用于在子类中重新定义，用来运行你的程序
        """
        You should override this method when you subclass Daemon.
        It will be called after the process has been daemonized by start() or restart().
        """
