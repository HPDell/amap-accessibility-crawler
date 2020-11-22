import urllib.request
import json
import datetime
import time
import os
import threading
import uuid
import queue
import transform
import sys
import getopt
from tqdm import tqdm


def log(message: str):
    """
    含时间的通知
    """
    with open("service.log", mode="a", encoding="UTF8") as log:
        print(f"[{time.strftime('%m-%d %H:%M:%S', time.localtime(time.time()))}] {message}", file=log)


class KeyProvider(threading.Thread):
    """
    Key分发器
    """
    def __init__(self, key_queue, key_lock):
        threading.Thread.__init__(self)
        self.key_queue = key_queue  # type: queue.Queue
        self.key_lock = key_lock  # type: threading.Condition
        self.__read_key__()
    
    def __read_key__(self):
        """
        生成key
        """
        with open("./key.txt") as key_file:
            count = 0
            for key in key_file:
                self.key_queue.put(key.strip())
                count = count + 1
            log(f"读取到 {count} 个 key")
    
    def run(self):
        """
        执行线程
        """
        while True:
            if self.key_queue.empty():
                now = datetime.datetime.now()
                tomorrow = datetime.datetime.today() + datetime.timedelta(days=1)
                resume = datetime.datetime.combine(tomorrow.date(), datetime.time(0, 0, 0, 0))
                log(f"生产者：所有key已被消耗，于 {resume.strftime('%m-%d %H:%M:%S')} 重新读取")
                # resume = now + datetime.timedelta(seconds=10)
                time_delta = resume - now
                time.sleep(time_delta.total_seconds())
                with self.key_lock:
                    log(u"生产者：正在读取 key")
                    self.__read_key__()
                    self.key_lock.notify_all()
                    log(u"生产者：唤醒所有线程")
            time.sleep(1)
                


D0 = 360.0 / transform.equator_leng
D = (D0*60*0.8)   #d

class GaodeDirectionWalking(threading.Thread):
    """
    爬虫线程
    """
    def __init__(self, index, center_queue, key_queue, range_scan, key_lock):
        """
        初始化线程
        """
        threading.Thread.__init__(self)
        self.api = "https://restapi.amap.com/v3/direction/walking?" 
        self.id = uuid.uuid1()
        self.index = index
        self.center_queue = center_queue  # type: queue.Queue
        self.range_scan = range_scan  # type: int
        self.key_queue = key_queue  # type: queue.Queue
        self.key_lock = key_lock  # type: threading.Condition
        self.key = ""
        self.pbar = tqdm(total=(range_scan + 1)**2, desc="", position=self.index)  # type: tqdm
    
    def __rent_key__(self):  # type: str
        """
        获取一个key
        """
        while True:
            if self.key_queue.empty():
                with self.key_lock:
                    self.pbar.set_postfix_str("waiting for key...")
                    self.key_lock.wait()
            key = self.key_queue.get()  # type: str
            self.pbar.set_postfix_str("using key: " + key[0:7])
            return key

    def run(self):
        """
        进行爬虫
        """
        self.key = self.__rent_key__()
        # key_used = 0
        while not self.center_queue.empty():
            (pid, point) = self.center_queue.get()
            self.pbar.set_description(desc="{:>4d}".format(int(pid)))
            param_src = "{0},{1}".format(point[0], point[1])
            x0 = (point[0] - D * self.range_scan / 2, point[1] - D * self.range_scan / 2)
            save_file_name = './result/' + 'points' + '_' + str(pid) + '_wh' + '.txt'
            with open(save_file_name, mode="w", newline="\n") as save_file:
                self.pbar.reset(total=(self.range_scan + 1)**2)
                for i in range(self.range_scan + 1):
                    for j in range(self.range_scan + 1):
                        dst = (x0[0] + i * D, x0[1] + j * D)
                        param_dst = "{0},{1}".format(dst[0], dst[1])
                        retry = 3
                        flag = False
                        while retry > 0:
                            try:
                                url = self.api + "key={0}&origin={1}&destination={2}&output=json".format(self.key, param_src, param_dst)
                                #### 模拟爬取
                                # key_used = key_used + 1
                                # if key_used > 300:
                                #     self.key = self.__rent_key__()
                                #     key_used = 0
                                # else:
                                #     flag = True
                                #     save_file.write(url + "\n")
                                #     retry = 0
                                #### END
                                response = json.load(urllib.request.urlopen(url))
                                if (str(response["infocode"]) == "10000"):
                                    flag = True
                                    path = response["route"]["paths"][0]
                                    content = ",".join([str(item) for item in [point[0], point[1], path["duration"], path['distance']]])
                                    print(content, file=save_file, end="\n")
                                    retry = 0
                                elif (str(response["infocode"]) == "10003"):  # 访问已超出日访问量
                                    self.key = self.__rent_key__()
                                elif (str(response["infocode"]) == "10004"):  # 单位时间内访问过于频繁
                                    time.sleep(120)
                                elif (str(response["infocode"]) == "10016"):  # 服务器负载过高
                                    time.sleep(600)
                                elif (str(response["infocode"]) == "10044"):  # 账号日调用量超限
                                    self.key = self.__rent_key__()
                                elif (str(response["infocode"]) == "10001"):  # key不正确或过期
                                    log(f"key {self.key} 错误 'key不正确或过期'")
                                    self.key = self.__rent_key__()
                                elif (str(response["infocode"]) == "10002"):  # 没有权限使用相应的服务或者请求接口的路径拼写错误
                                    log(f"key {self.key} 错误 '没有权限使用相应的服务或者请求接口的路径拼写错误'")
                                    self.key = self.__rent_key__()
                                elif (str(response["infocode"]) == "10009"):  # 请求key与绑定平台不符
                                    log(f"key {self.key} 错误 '请求key与绑定平台不符'")
                                    self.key = self.__rent_key__()
                                else:
                                    retry = retry - 1
                            except Exception:
                                retry = retry - 1
                            time.sleep(0.02)
                        if (flag is not True):
                            log(f"({pid}) 第 ({i},{j}) 点爬取出错")
                        self.pbar.update(1)
            log(pid + u"爬取完毕")


if __name__ == "__main__":
    argv = sys.argv[1:]
    range_size = 51
    thread_num = 1
    ''' 读取参数
    '''
    try:
        opts, args = getopt.getopt(argv, "ht:r:", ["threads=", "range="])
        for (opt, arg) in opts:
            if opt == "-h":
                print('test.py -c <threads num> -r <range size>')
                sys.exit(2)
            elif opt in ("-t", "--threads"):
                try:
                    t = int(arg)
                    if t > 0:
                        thread_num = t
                    else:
                        raise ValueError()
                except ValueError:
                    print("Please set correct thread num (int>0)")
                    sys.exit(1)
            elif opt in ("-r", "--range"):
                try:
                    r = int(arg)
                    if r > 0:
                        range_size = r
                    else:
                        raise ValueError()
                except ValueError:
                    print("Please set correct thread num (int>0)")
                    sys.exit(1)
    except getopt.GetoptError:
        print('test.py -t <threads num> -r <range size>')
        sys.exit(2)
    # print(f"thread_num={thread_num},range_size={range_size}")
    if thread_num <= 0 or range_size <= 0:
        print('Args are not correct')
        sys.exit(1)

    ''' 读取数据
    '''
    result_list = [f.split("_")[1] for f in os.listdir("./result")]
    xcq_queue = queue.Queue()
    with open("XCQ.txt") as xcq_file:
        for line_fp in xcq_file:
            pl = line_fp.strip('\n').split(',')
            index = pl[0]
            gcj02_pnt = transform.wgs84_to_gcj02(float(pl[1]), float(pl[2]))
            if (index not in result_list):
                xcq_queue.put((pl[0], (gcj02_pnt[0], gcj02_pnt[1])))
            else:
                os.fstat
                with open(f"./result/points_{index}_wh.txt") as exist_file:
                    lines = 0
                    for (index, line) in enumerate(exist_file):
                        lines = lines + 1
                    if (lines < ((range_size + 1) ** 2)):
                        xcq_queue.put((pl[0], (gcj02_pnt[0],gcj02_pnt[1])))
    ''' Key设置
    '''
    key_queue = queue.Queue()
    key_lock = threading.Condition()
    key_provider = KeyProvider(key_queue, key_lock)
    key_provider.start()
    ''' 开始爬虫
    '''
    log(u"爬取开始")
    threads = [GaodeDirectionWalking(i, xcq_queue, key_queue, range_size, key_lock) for i in range(thread_num)]
    for t in threads:
        t.setDaemon(True)
        t.start()
        time.sleep(0.1)
