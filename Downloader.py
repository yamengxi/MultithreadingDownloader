"""
多线程下载器
By HViktorTsoi

"""
import multiprocessing
import sys
import threading
import time
from contextlib import closing

import requests
import requests.adapters


class Downloader:
    """多线程下载器

    用Downloader(目标URL, 保存的本地文件名)的方式实例化

    用start()开始下载

    样例:

    downloader=Downloader(
        url="http://file.example.com/somedir/filename.ext",
        file="/path/to/file.ext"
    )

    downloader.start()
    """

    def __init__(self,
                 threads_num=20,
                 chunk_size=1024 * 128,
                 timeout=0,
                 enable_log=True
                 ):
        """初始化

            :param threads_num=20: 下载线程数

            :param chunk_size=1024*128: 下载线程以流方式请求文件时的chunk大小

            :param timeout=60: 下载线程最多等待的时间
        """
        self.threads_num = threads_num
        self.chunk_size = chunk_size
        self.timeout = timeout if timeout != 0 else threads_num  # 超时时间正比于线程数
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "authority": "internal-api-drive-stream.feishu.cn",
            "cookie": "passport_web_did=6947578838889005057; locale=zh-CN; trust_browser_id=e9a754a6-69d0-47c1-8482-e9165e95760b; __tea__ug__uid=6947578820757145102; fid=dda233a4-1c1a-4ded-a3ab-49c16742b422; is_anonymous_session=; _csrf_token=ba3cd1ccbdb856e05c83e0a3f1f332a675f97c2b-1617609276; lang=zh; slardar_delay_type=a; _uuid_hera_ab_path_1=6948395566250344452; _ga=GA1.2.763540225.1617799414; _gid=GA1.2.852114280.1617799414; help_center_session=be440aa9-9e82-4628-9127-0d09b979851f; session=XN0YXJ0-28d07e40-91e2-4353-ba18-79da3e6c8f8g-WVuZA; landing_url=https://activity.feishu.cn/activity/ug/signup/1041?tracking_code=7010o0000024cQ3AAI&utm_from=baidu_gjc_pc_qg_allppqt_all&source=baidu&device=PC&e_keywordid=208827806780&e_keywordid2=208827806780&ad_platform_id=baidusearch_lead&account_id=28166825&bd_vid=8538216140877543234; __tea_cookie_tokens_3414=%257B%2522web_id%2522%253A%25229007334307869518981%2522%252C%2522ssid%2522%253A%25227e0b4ff5-fc3d-4126-a39a-2d84d403e8bf%2522%252C%2522user_unique_id%2522%253A%25226947578820757145102%2522%252C%2522timestamp%2522%253A1617869516553%257D; swp_csrf_token=3e1b470e-0ce3-4c3a-833c-883b1fea4995; t_beda37=60e0e37e2c7616e6ef45252cb6e59d47acc3ef845edde153e25233eda5044687"
        }

        self.__content_size = 0
        self.__file_lock = threading.Lock()
        self.__threads_status = {}
        self.__crash_event = threading.Event()
        self.__msg_queue = multiprocessing.Queue()

        self.__enable_log = enable_log
        self.__logger = Logger(msgq=self.__msg_queue) if enable_log else None

        requests.adapters.DEFAULT_RETRIES = 2

    def __establish_connect(self, url):
        """建立连接
        """
        print("建立连接中......")
        hdr = requests.head(url=url,headers=self.headers,stream=True,timeout=1000).headers
        self.__content_size = int(hdr["Content-Length"])
        print("连接已经建立. 文件大小：{}B".format(self.__content_size))

    def __page_dispatcher(self):
        """分配每个线程下载的页面大小
        """
        # 基础分页大小
        basic_page_size = self.__content_size // self.threads_num
        start_pos = 0
        # 分配给各线程的页面大小
        while start_pos + basic_page_size < self.__content_size:
            yield {
                'start_pos': start_pos,
                'end_pos': start_pos + basic_page_size
            }
            start_pos += basic_page_size + 1
        # 最后不完整的一页补齐
        yield {
            'start_pos': start_pos,
            'end_pos': self.__content_size - 1
        }

    def __download(self, url, file, page):
        """下载

            :param url="": 下载的目标URL

            :param file: 保存在本地的目标文件

            :param page: 下载的分页信息 start_pos:开始字节 end_pos:结束字节
        """
        # 当前线程负责下载的字节范围
        headers = {
            "Range": "bytes={}-{}".format(page["start_pos"], page["end_pos"])
        }
        headers.update(self.headers)

        thread_name = threading.current_thread().name
        # 初始化当前进程信息列表
        self.__threads_status[thread_name] = {
            "page_size": page["end_pos"] - page["start_pos"],
            "page": page,
            "status": 0,
        }
        try:
            # 以流的方式进行get请求
            with closing(requests.get(
                    url=url,
                    headers=headers,
                    stream=True,
                    timeout=self.timeout
            )) as response:
                for data in response.iter_content(chunk_size=self.chunk_size):
                    # 向目标文件中写入数据块,此处需要同步锁
                    with self.__file_lock:
                        # 查找文件写入位置
                        file.seek(page["start_pos"])
                        # 写入文件
                        file.write(data)
                    # 数据流每向前流动一次,将指针位置同时前移
                    page["start_pos"] += len(data)
                    self.__threads_status[thread_name]["page"] = page
                    if self.__enable_log:
                        self.__msg_queue.put(self.__threads_status)

        except requests.RequestException as exception:
            print("XXX From {}: ".format(exception), file=sys.stderr)
            self.__threads_status[thread_name]["status"] = 1
            if self.__enable_log:
                self.__msg_queue.put(self.__threads_status)
            # 设置crash_event标记为1
            self.__crash_event.set()

    def __run(self, url, target_file, urlhandler):
        """执行下载线程

            :param url="": 下载的目标URL

            :param target_file="": 保存在本地的目标文件名（完整路径，包括文件扩展名）

            :param urlhandler: 目标URL的处理器，用来处理重定向或Content-Type不存在等问题
        """
        thread_list = []
        self.__establish_connect(url)
        self.__threads_status["url"] = url
        self.__threads_status["target_file"] = target_file
        self.__threads_status["content_size"] = self.__content_size
        self.__crash_event.clear()
        # 处理url
        url = urlhandler(url)
        with open(target_file, "wb+") as file:
            for page in self.__page_dispatcher():
                thd = threading.Thread(
                    target=self.__download, args=(url, file, page)
                )
                thd.start()
                thread_list.append(thd)
            for thd in thread_list:
                thd.join()
        self.__threads_status = {}
        # 判断是否有线程失败
        if self.__crash_event.is_set():
            raise Exception("下载未成功！！！")

    def start(self, url, target_file=None, urlhandler=lambda u: u):
        """开始下载

            :param url="": 下载的目标URL

            :param urlhandler=lambdau:u: 目标URL的处理器，用来处理重定向或Content-Type不存在等问题
        """
        if target_file is None:
            target_file = url.split('/')[-1]

        # 记录下载开始时间
        start_time = time.time()
        self.__logger.start_time = start_time
        self.__logger.last_time = start_time
        self.__logger.last_size = 0

        # 如果允许log
        if self.__enable_log:
            # logger进程
            self.__logger.start()
            # 开始下载
            self.__run(url, target_file, urlhandler)
            # 结束logger进程
            self.__logger.join(0.5)
        else:
            # 直接开始下载
            self.__run(url, target_file, urlhandler)

        # 记录下载总计用时
        span = time.time() - start_time
        print("下载完成. 总计用时:{}s".format(span - 0.5))


class Logger(multiprocessing.Process):
    """日志进程

    记录每个线程的下载状态以及文件下载状态
    """

    def __init__(self, msgq):
        """初始化日志记录器

            :param msgq: 下载进程与日志进程通信的队列
        """
        multiprocessing.Process.__init__(self, daemon=True)
        self.__threads_status = {}
        self.__msg_queue = msgq
        self.start_time = 0
        self.last_time = 0
        self.last_size = 0
        self.KBps = 0

    def __log_metainfo(self):
        """输出文件元信息
        """
        print("文件元信息:\nURL: {}\n文件名:{}\n文件大小:{}KB".format(
            self.__threads_status["url"],
            self.__threads_status["target_file"],
            self.__threads_status["content_size"] / 1024
        ))

    def __log_threadinfo(self):
        """输出各线程下载状态信息
        """
        downloaded_size = 0
        for thread_name, thread_status in self.__threads_status.items():
            if thread_name not in ("url", "target_file", "content_size"):
                page_size = thread_status["page_size"]
                page = thread_status["page"]
                thread_downloaded_size = page_size - \
                                         (page["end_pos"] - page["start_pos"])
                downloaded_size += thread_downloaded_size
                self.__print_thread_status(
                    thread_name, thread_status["status"], page_size,
                    page, thread_downloaded_size
                )
        self.__print_generalinfo(downloaded_size)

    def __print_thread_status(self, thread_name, status, page_size, page, thread_downloaded_size):
        """打印线程信息

            :param thread_name: 线程名

            :param status: 线程执行状态

            :param page_size: 分页大小

            :param page: 分页信息

            :param thread_downloaded_size: 线程已经下载的字节数
        """
        if status == 0:
            if page["start_pos"] < page["end_pos"]:
                print("|- {}  Downloaded: {}KB / Chunk: {}KB".format(
                    thread_name,
                    thread_downloaded_size / 1024,
                    page_size / 1024,
                ))
            else:
                print("|=> {} Finished.".format(thread_name))
        elif status == 1:
            print("|XXX {} Crushed.".format(
                thread_name
            ), file=sys.stderr)

    def __print_generalinfo(self, downloaded_size):
        """打印文件整体下载信息

            :param downloaded_size: 整个文件已经下载的字节数
        """
        if time.time() > self.last_time + 1:
            self.KBps = (downloaded_size - self.last_size) / 1024 / (time.time() - self.last_time)
            self.last_size = downloaded_size
            self.last_time = time.time()

        print("|__已下载: {}KB / Total: {}KB\n|__平均速度: {}KB/s\n|__最近一秒速度: {}KB/s".format(
            downloaded_size / 1024,
            self.__threads_status["content_size"] / 1024,
            downloaded_size / 1024 / (time.time() - self.start_time),
            self.KBps
        ))

    def run(self):
        while True:
            if self.__msg_queue.qsize() != 0:
                print("\033c")
                self.__threads_status = self.__msg_queue.get()
                self.__log_metainfo()
                self.__log_threadinfo()

if __name__ == '__main__':
    DOWNLOADER = Downloader(threads_num=int(sys.argv[1]))
    if len(sys.argv) == 3:
        DOWNLOADER.start(url=str(sys.argv[2]))
    elif len(sys.argv) == 4:
        DOWNLOADER.start(url=str(sys.argv[2]), target_file=str(sys.argv[3]))
    else:
        print('Argument num error!')
