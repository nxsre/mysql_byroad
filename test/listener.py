#!/usr/bin/env python
#coding:utf-8
 
import sys
import os
import subprocess
import time
import requests
from supervisor import childutils
from optparse import OptionParser

 
sms_url = "http://sms.int.jumei.com/index.php"
sms_user = "int_notice"
sms_password = "notice_rt902pnkl10udnq"
sysname = "旁路系统"
notice_event = ["PROCESS_STATE_EXITED", "PROCESS_STATE_FATAL"]
nums = ["15108204134"]



def write_stdout(s):
    sys.stdout.write(s)
    sys.stdout.flush()


def write_stderr(s):
    sys.stderr.write(s)
    sys.stderr.flush()


class CallError(Exception):
    def __init__(self,value):
        self.value = value
    def __str__(self):
        return repr(self.value)


# 定义处理event的类
class ProcessesMonitor():
    def __init__(self):
        self.stdin = sys.stdin
        self.stdout = sys.stdout
 
    def runforever(self):
        # 定义一个无限循环，可以循环处理event，当然也可以不用循环，把listener的autorestart#配置为true，处理完一次event就让该listener退出，然后supervisord重启该listener，这样listen#er就可以处理新的event了
        while 1:
            # 向stdout发送"READY"，然后就阻塞在这里，一直等到有event发过来
            headers, payload = childutils.listener.wait(self.stdin, self.stdout)
            # write_stderr("recv event: headers: %s, payload: %s\n" % (headers, payload))
            if headers['eventname'] not in  notice_event:
                childutils.listener.ok(self.stdout)
                continue
 
            pheaders,pdata = childutils.eventdata(payload+'\n')
            # 判读event是否是expected是否是expected的，expected的话为1，否则为0
            expected = pheaders.get("expected")
            if expected:
                childutils.listener.ok(self.stdout)
                continue
 
            msg = "[%s][%s][Process:%s][from %s to %s]" % (time.strftime("%Y-%m-%d %X"), sysname, pheaders['processname'], pheaders['from_state'], headers["eventname"])
            # 调用报警接口
            for num in nums:
                send_notice(num, msg)
            # stdout写入"RESULT\nOK"，并进入下一次循环
            childutils.listener.ok(self.stdout)


def send_notice(num, notice):
    write_stderr("send %s: %s\n" % (num, notice))    
    try:
        resp = requests.post(sms_url, timeout=10, data={"task": sms_user, "key": sms_password, "num": num, "content": notice})
    except requests.ConnectionError, e:
        write_stderr(str(e.message))
        return
    write_stderr(resp.content)


def main():
    # 下面这个，表示只有supervisord才能调用该listener，否则退出
    if not 'SUPERVISOR_SERVER_URL' in os.environ:
        try:
            raise CallError("%s must be run as a supervisor event" % sys.argv[0])
        except CallError as e:
            write_stderr("Error occurred,value: %s\n" % e.value)
 
        return
 
    prog = ProcessesMonitor()
    prog.runforever()

 
if __name__ == '__main__':
    main()

