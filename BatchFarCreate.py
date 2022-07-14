#!/miniconda3/envs/py39us/bin/python
# coding: utf-8
import argparse
import json
import logging
import os
import shlex
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from enum import Enum
from typing import List

import pandas as pd

from common import file_size_format
from common import getstatusoutput_s
from common import log_formatter_get
from common import real_time_get
from common import sh2bash
from common import str_md5_get
from common import time_now_get
from common import user_time_get
from common import video_duration_get

# =================

# 日志相关和报告相关

backup = os.path.join(os.getcwd(), "backup")
log_filename = "batch_far_create.log"
xlsx_export = "batch_far_create_report.xlsx"
path_report = "batch_far_create_path_report.txt"

# 设备相关配置
# compress_threshold = 1
compress_threshold = 1280 * 720

# ffmpeg_devices = [] 表示不进行解码
ffmpeg_devices = [
    # CPU设备ID 支持线程个数
    # -1 标识CPU
    # [-1, 2]
    # [0, 2],
    # [2, 7],
]

ffmpeg_shell_tpl = [
    "time ffmpeg -i {src} -s 400:244 {dst}",
    "time /root/ffmpeg.N-107154-gc11fb46731 -hwaccel_device {gpu_id} -hwaccel cuvid -c:v {codec}_cuvid -i {src} -c:v h264_nvenc -vf scale_npp=400:-2 -y {dst}"
]

ffmpeg_rebuild = True
vdnagen_rebuild = True


# =================


class Reporter:
    backup_dir = backup

    def __init__(self):
        os.makedirs(self.backup_dir, exist_ok=True)

        time_start = time.strftime("%Y%m%d%H%M%S", time.localtime())

        self.__log_path = os.path.join(os.getcwd(), log_filename)
        self.__log_path_bkp = os.path.join(self.backup_dir, f"{time_start}-{log_filename}")
        if os.path.isfile(self.__log_path):
            os.remove(self.__log_path)
        self.logger = self.logger_create()

        self.__path_report = os.path.join(os.getcwd(), path_report)
        self.__path_report_bkp = os.path.join(self.backup_dir, f"{time_start}-{path_report}")
        if os.path.isfile(self.__path_report):
            os.remove(self.__path_report)

        self.__xlsx_export = os.path.join(os.getcwd(), xlsx_export)
        self.__xlsx_export_bkp = os.path.join(self.backup_dir, f"{time_start}-{xlsx_export}")

    def logger_create(self, logger_name: str = "batch_far_create_logger"):
        logg = logging.getLogger(logger_name)
        # 定义一个模板
        FORMATTER = log_formatter_get()
        # 创建一个屏幕流
        p_stream = logging.StreamHandler()
        # 创建一个文件流
        f_stream = logging.FileHandler(self.__log_path, mode="a", encoding="utf-8")
        f_stream_bkp = logging.FileHandler(self.__log_path_bkp, mode="a", encoding="utf-8")
        p_stream.setFormatter(FORMATTER)
        f_stream.setFormatter(FORMATTER)
        f_stream_bkp.setFormatter(FORMATTER)
        logg.addHandler(p_stream)
        logg.addHandler(f_stream)
        logg.addHandler(f_stream_bkp)
        logg.setLevel(logging.DEBUG)
        return logg

    def log_write(self, msg: str, level=logging.DEBUG) -> None:
        """ 写入日志
        """
        self.logger.log(level, msg)

    def path_write(self, far_path: str) -> None:
        """ 记录生成的far文件路径
        """
        with open(self.__path_report, mode="a", encoding="utf-8") as f:
            f.write(far_path + "\n")
        with open(self.__path_report_bkp, mode="a", encoding="utf-8") as f:
            f.write(far_path + "\n")

    def xlsx_write(self, df: pd.DataFrame) -> None:
        """ far生成报告导出
        """
        df.to_excel(self.__xlsx_export)
        df.to_excel(self.__xlsx_export_bkp)


class MediaInfo:
    __cmd_tpl = "ffprobe {media_path} -show_streams -select_streams v -print_format json"

    def __init__(self, media_path: str):
        self.media_path = media_path
        self.__cmd = sh2bash(self.__cmd_tpl.format(media_path=shlex.quote(media_path)))

        self.__sts = -1
        self.__data = ""
        if os.path.isfile(media_path):
            self.__sts, self.__data = getstatusoutput_s(self.__cmd)

        data_ = []
        report = False
        for line in self.__data.split("\n"):
            if line == "{":
                report = True
            if report:
                data_.append(line)

            if line == "}":
                report = False
        # 规划院机器上有点特别， 它们命令中返回的json体内还有不是json的内容
        streams_line = 0
        for idx, line in enumerate(data_):
            if line.strip() == '"streams": [':
                streams_line = idx

        if 0 < streams_line < len(data_):
            for i in range(1, streams_line):
                del data_[i]

        data_ = "\n".join(data_)
        try:
            self.__data = json.loads(data_)["streams"][0]
        except Exception:
            self.__data = {}

    @property
    def status(self):
        return self.__sts

    @property
    def duration(self):
        duration = self.__meta_info_get("duration")
        if duration is not None:
            duration = int(float(duration))
        else:
            duration = video_duration_get(self.media_path)
        return duration

    @property
    def codec(self):
        return self.__meta_info_get("codec_name")

    @property
    def width(self):
        return self.__meta_info_get("width")

    @property
    def height(self):
        return self.__meta_info_get("height")

    def __meta_info_get(self, key: str):
        return self.__data.get(key, None)


class TaskStatus(Enum):
    null = -1
    parse_error = 0
    task_create = 1
    no_need_compress = 2
    need_compress = 3
    compress_running = 4
    compress_done = 5
    compress_error = 6
    no_need_dnagen = 7
    need_dnagen = 8
    dnagen_runing = 9
    dnagen_done = 10
    dnagen_error = 11


class Task:
    def __init__(self):
        self.status = TaskStatus.null
        self.media_path = ""
        self.media_size = 0
        self.media_width = -1
        self.media_height = -1
        self.media_codec = ""
        self.media_duration = -1

        # -1表示cpu 0 1 ... 表示gpu 其他无意义
        self.fpg_gpu_id = -2
        self.fpg_task_proc = None
        self.compress_path = ""
        self.compress_size = -1
        self.compress_cmd = ""
        self.compress_start_time = ""
        self.compress_end_time = ""
        self.compress_time_used = -1

        self.vdg_task_proc = None
        self.vdg_cmd = ""
        self.vdg_start_time = ""
        self.vdg_end_time = ""
        self.vdg_time_used = -1
        self.far_path = ""
        self.far_size = -1


class FarCreater:
    __reporter = Reporter()

    def __init__(self, num_workers: int = 40, fpg_cache: str = "/tmp/far_create/"):
        self.__fpg_cache = fpg_cache
        os.makedirs(self.__fpg_cache, exist_ok=True)
        self.__tasks: List[Task] = []
        self.__tasks_init_error: List[Task] = []

        self.__num_workers = 1
        if num_workers > 1:
            self.__num_workers = num_workers

        # 放置不同状态的任务索引
        self.__fpg_devices = ffmpeg_devices
        # 创建关于fpg的线程池
        self.__fpg_pools = []
        # 特殊情况，gpu_id = -1 cpu
        for gpu_id, gpu_thread in self.__fpg_devices:
            self.__fpg_pools.append(ThreadPoolExecutor(max_workers=gpu_thread))

        self.__fpg_tasks_wait: List[int] = []  # ffmpeg 还没开始运行的
        self.__fpg_tasks_running: List[int] = []  # ffmpeg 正在运行的
        self.__fpg_tasks_done: List[int] = []  # ffmpeg 已经运行结束的
        self.__fpg_tasks_error: List[int] = []  # ffmpeeg运行出错的任务

        # ffmpeg 结果查询的控制变量
        self.__fpg_tasks_done_tr: int = 0  # ffmpeg 已经运行结束的 上次遍历结束的位置
        self.__fpg_tasks_error_tr: int = 0  # ffmpeg 运行错误的 上次遍历结束的位置

        self.__vdg_pool = ThreadPoolExecutor(max_workers=self.__num_workers)
        self.__vdg_tasks_wait: List[int] = []  # VDNAGen 还没开始运行的
        self.__vdg_tasks_running: List[int] = []  # VDNAGen 正在运行的
        self.__vdg_tasks_done: List[int] = []  # VDNAGen 已经运行结束的
        self.__vdg_tasks_error: List[int] = []  # VDNAGen 运行出错的任务

        # VDNAGen 结果查询的控制变量
        self.__vdg_tasks_done_tr: int = 0  # VDNAGen 已经运行结束的 上次遍历结束的位置
        self.__vdg_tasks_error_tr: int = 0  # VDNAGen 运行错误的 上次遍历结束的位置

    def _is_need_compress(self, task: Task, th: int = compress_threshold) -> bool:
        """
        判断视频是否需要进行压缩，判断逻辑是视频的宽高
        :param task:
        :param th: media width x height < th
        :return:
        """
        if len(self.__fpg_pools) > 0:
            width = task.media_width
            height = task.media_height
            return width > 0 and height > 0 and (th / width) < height
        return False

    def task_add(self, media_path: str,
                 far_path: str) -> None:
        """
        添加任务信息
        :param media_path: 视频/far文件路径
        :param far_path: 生成的far文件路径
        :return:
        """
        media_path = os.path.abspath(media_path)
        far_path = os.path.abspath(far_path)
        if os.path.isfile(media_path):
            if not vdnagen_rebuild and os.path.isfile(far_path):
                self.__reporter.log_write(f"{media_path} {far_path} already exists")
                return
            # 通过文件后缀名过滤
            meta = MediaInfo(media_path)

            res = Task()
            res.media_path = media_path
            res.far_path = far_path
            res.media_size = os.path.getsize(media_path)
            if meta.status != 0:
                self.__reporter.log_write(f"{media_path} ffmpeg get meta info failed.")
                res.status = TaskStatus.parse_error
                self.__tasks_init_error.append(res)
            else:
                try:
                    res.media_duration = meta.duration
                    res.media_width = meta.width
                    res.media_height = meta.height
                    res.media_codec = meta.codec
                    res.status = TaskStatus
                    self.__reporter.log_write(f"{media_path} task add success.")
                    self.__tasks.append(res)
                except Exception:
                    self.__reporter.log_write(f"{media_path} ffmpeg get meta info failed.")
                    self.__tasks_init_error.append(res)
        else:
            self.__reporter.log_write(f"{media_path} not exists.")

    def tasks_add_from_dir(self, media_dir: str, far_dir: str) -> None:
        """
        遍历视频文件夹的视频，在far文件夹创建对应文夹保存far文件, 支持递归
        :param media_dir: 视频文件文件夹
        :param far_dir: 生成的far文件文件夹
        :return:
        """
        os.makedirs(far_dir, exist_ok=True)

        for sub in os.listdir(media_dir):
            sub_path = os.path.join(media_dir, sub)
            if os.path.isdir(sub_path):
                sub_media_dir = sub_path
                sub_far_dir = os.path.join(far_dir, sub)
                self.tasks_add_from_dir(sub_media_dir, sub_far_dir)
            elif os.path.isfile(sub_path) and not sub_path.endswith(".far"):
                media_path = sub_path
                media_name, _ = os.path.splitext(sub)
                far_path = os.path.join(far_dir, media_name + ".far")
                self.task_add(media_path, far_path)

    def tasks_add_from_file(self, file: str, far_dir: str):
        if not os.path.isfile(file):
            return
        os.makedirs(far_dir, exist_ok=True)
        with open(file, mode="r", encoding="utf-8") as f:
            for idx, media_path in enumerate(f.readlines()):
                media_path = media_path.strip()
                media_name = os.path.basename(media_path)
                far_name = f"{idx + 1}-{media_name}"
                far_path = os.path.join(far_dir, far_name)
                self.task_add(media_path, far_path)

    def __fpg_runner(self, task_id: int) -> None:
        if 0 <= task_id < len(self.__tasks):
            task: Task = self.__tasks[task_id]
        else:
            return
        # 检查是否需要开启压缩任务
        if task.status != TaskStatus.need_compress:
            task.status = TaskStatus.compress_error
            return
        task.status = TaskStatus.compress_running
        media_path = task.media_path
        media_name = os.path.basename(media_path)
        cache_dir = os.path.join(self.__fpg_cache, str_md5_get(media_path.encode("utf-8")))
        os.makedirs(cache_dir, exist_ok=True)
        # 压缩视频保存路径
        compress_path = os.path.join(cache_dir, media_name)
        task.compress_path = compress_path

        gpu_id = task.fpg_gpu_id
        codec = task.media_codec
        # 判断使用什么方式进行压缩 -1 CPU <=1 GPU
        if gpu_id < 0:
            tpl = ffmpeg_shell_tpl[0]
            cmd = sh2bash(tpl.format(src=shlex.quote(media_path), dst=shlex.quote(compress_path)))
        else:
            tpl = ffmpeg_shell_tpl[1]
            cmd = sh2bash(
                tpl.format(src=shlex.quote(media_path), dst=shlex.quote(compress_path), gpu_id=gpu_id, codec=codec))
        task.compress_cmd = cmd

        if ffmpeg_rebuild and os.path.isfile(compress_path):
            # 删除早期压缩的视频文件
            os.remove(compress_path)

        time_used = 0
        status = 0
        time_begin = time_now_get()
        if not os.path.isfile(compress_path):
            status, output = getstatusoutput_s(cmd)
            time_used = real_time_get(output)
        time_end = time_now_get()

        task.compress_start_time = time_begin
        task.compress_end_time = time_end
        task.compress_time_used = time_used
        if status == 0 and os.path.isfile(compress_path):
            task.compress_size = os.path.getsize(compress_path)
            task.status = TaskStatus.compress_done
        else:
            task.status = TaskStatus.compress_error

    def __fpg_tasks_queue_update(self) -> None:
        # 1 统计ffmpeg已经完成的任务
        tasks = []
        for task_id in self.__fpg_tasks_running:
            task: Task = self.__tasks[task_id]
            task_proc = task.fpg_task_proc
            if task_proc.done():
                # task_proc.result()
                tasks.append(task_id)

        # 2 删除ffmpeg已经完成的任务
        for task_id in tasks:
            self.__fpg_tasks_running.remove(task_id)
            task: Task = self.__tasks[task_id]
            # 注意在compress_error状态下的任务是不会被添加到vndgen执行队列的
            # 但是compress_done的任务可能在当前ffmpeg队列还没被更新的时候已经添加到了vdnagen队列
            if task.status == TaskStatus.compress_done:
                self.__fpg_tasks_done.append(task_id)
                # 把压缩视频已经生成，加入到VDNAGen执行队列中
                self.__vdg_tasks_wait.append(task_id)
            else:
                self.__fpg_tasks_error.append(task_id)

        # 3. 统计每个GPU可以继续装载的任务数量
        devices = deepcopy(self.__fpg_devices)
        for task_id in self.__fpg_tasks_running:
            task: Task = self.__tasks[task_id]
            gpu_id = task.fpg_gpu_id
            for device in devices:
                if device[0] == gpu_id and device[1] > 0:
                    device[1] -= 1
                    break
        # 统计总共需要任务数量
        new_tasks_cnt = 0
        for gpu_id, gpu_thread in devices:
            new_tasks_cnt += gpu_thread

        # 4 根据需要任务的数量获得任务
        tasks = []  # 新的需要运行的任务
        for task_id in self.__fpg_tasks_wait:
            if len(tasks) >= new_tasks_cnt:
                break
            tasks.append(task_id)

        # 5 启动新任务
        device_idx = 0
        for task_id in tasks:
            task: Task = self.__tasks[task_id]
            device = devices[0]
            if device[1] == 0:
                device_idx += 1
                device = devices[1]
                del devices[0]

            device[1] -= 1

            task.fpg_gpu_id = device[0]
            task.status = TaskStatus.need_compress
            pool = self.__fpg_pools[device_idx]
            task_proc = pool.submit(self.__fpg_runner, task_id)
            task.fpg_task_proc = task_proc

            self.__fpg_tasks_running.append(task_id)
            self.__fpg_tasks_wait.remove(task_id)

    def __vdg_runner(self, task_id: int):
        if 0 <= task_id < len(self.__tasks):
            task: Task = self.__tasks[task_id]
        else:
            return

        # 判断是否需要执行
        if task.status != TaskStatus.need_dnagen:
            task.status = TaskStatus.dnagen_error
            return
        if os.path.isfile(task.compress_path):
            media_path = task.compress_path
        else:
            media_path = task.media_path

        far_path = task.far_path
        task.status = TaskStatus.dnagen_runing
        cmd = sh2bash(f"time VDNAGen {shlex.quote(media_path)} -o {shlex.quote(far_path)}")
        task.vdg_cmd = cmd

        if vdnagen_rebuild and os.path.isfile(far_path):
            os.remove(far_path)

        time_used = 0
        status = 0
        time_begin = time_now_get()
        if not os.path.isfile(far_path):
            status, output = getstatusoutput_s(cmd)
            time_used = user_time_get(output)
        time_end = time_now_get()
        task.vdg_start_time = time_begin
        task.vdg_end_time = time_end
        task.vdg_time_used = time_used
        if status == 0 and os.path.isfile(far_path):
            task.far_size = os.path.getsize(far_path)
            task.status = TaskStatus.dnagen_done
        else:
            task.status = TaskStatus.dnagen_error

    def __vdg_tasks_queue_update(self):

        # 1 统计VDNAGen已经完成的任务
        tasks = []
        for task_id in self.__vdg_tasks_running:
            task: Task = self.__tasks[task_id]
            task_proc = task.vdg_task_proc
            if task_proc.done():
                # task_proc.result()
                tasks.append(task_id)

        # 2 删除VDNAGen已经完成的任务
        for task_id in tasks:
            self.__vdg_tasks_running.remove(task_id)
            task: Task = self.__tasks[task_id]
            if task.status == TaskStatus.dnagen_done:
                self.__vdg_tasks_done.append(task_id)
            else:
                self.__vdg_tasks_error.append(task_id)

        # 3 获得新任务
        tasks = []
        for task_id in self.__vdg_tasks_wait:
            if len(tasks) + len(self.__vdg_tasks_running) >= self.__num_workers:
                break
            tasks.append(task_id)

        # 4 启动新任务
        for task_id in tasks:
            task: Task = self.__tasks[task_id]
            task.status = TaskStatus.need_dnagen
            task_proc = self.__vdg_pool.submit(self.__vdg_runner, task_id)
            task.vdg_task_proc = task_proc
            self.__vdg_tasks_running.append(task_id)
            self.__vdg_tasks_wait.remove(task_id)

    def __tasks_init(self):
        """任务分拣
        确定那些需要采用那些任务需要进行视频压缩，那些任务不需要
        """

        self.__fpg_tasks_wait = []
        self.__fpg_tasks_running = []
        self.__fpg_tasks_done = []
        self.__fpg_tasks_error = []

        self.__fpg_tasks_done_tr = 0
        self.__fpg_tasks_error_tr = 0

        self.__vdg_tasks_wait = []
        self.__vdg_tasks_running = []
        self.__vdg_tasks_done = []
        self.__vdg_tasks_error = []

        self.__vdg_tasks_done_tr = 0
        self.__vdg_tasks_error_tr = 0

        tasks_a = []  # 需要视频压缩的任务
        tasks_b = []  # 不需要视频压缩的任务
        # 先把视频分为需要压缩和不需要压缩的
        for idx, task in enumerate(self.__tasks):
            if self._is_need_compress(task, compress_threshold):
                task.status = TaskStatus.need_compress
                tasks_a.append(idx)
            else:
                task.status = TaskStatus.no_need_compress
                tasks_b.append(idx)
        # 不管是否需要压缩，都按照从大到小排列
        sort_by_size = lambda task_id: self.__tasks[task_id].media_size
        tasks_a.sort(key=sort_by_size, reverse=True)
        tasks_b.sort(key=sort_by_size, reverse=True)

        # 记录需要进行视频压缩的
        self.__fpg_tasks_wait.extend(tasks_a)
        # VDNAGen先处理不需要视频压缩的，后处理需要视频压缩的
        self.__vdg_tasks_wait.extend(tasks_b)
        # 这里需要先使用ffmpeg进行压缩，然后使用VDNAGen生成基因的
        # 等后面ffmpeg执行完成后，由主线程后期加入
        # self.__vdg_tasks_wait.extend(tasks_a)

    def __far_path_log_update(self):
        for i in range(len(self.__vdg_tasks_done) - self.__vdg_tasks_done_tr):
            task_id = self.__vdg_tasks_done[self.__vdg_tasks_done_tr + i]
            task: Task = self.__tasks[task_id]  # 这就等于获得当面任务的执行结果
            self.__reporter.path_write(task.far_path)

    def __vdg_task_log_update_op(self, task_id: int):
        """ VDNAGen 日志打印内容
        :param task_id: 任务索引
        :return:
        """
        task = self.__tasks[task_id]  # 这就等于获得当面任务的执行结果
        self.__reporter.log_write(f"media path: {task.media_path}")
        self.__reporter.log_write(f"media size: {file_size_format(task.media_size)}")
        self.__reporter.log_write(f"media shape: {task.media_width}x{task.media_height}")
        self.__reporter.log_write(f"media codec: {task.media_codec}")
        self.__reporter.log_write(f"media duration : {task.media_duration} sec")
        if os.path.isfile(task.compress_path):
            self.__reporter.log_write(f"compress path: {task.compress_path}")
            self.__reporter.log_write(f"compress size: {file_size_format(task.compress_size)}")
        self.__reporter.log_write(f"far path: {task.far_path}")
        self.__reporter.log_write(f"far size: {file_size_format(task.far_size)}")
        self.__reporter.log_write(f"vdnagen command: {task.vdg_cmd}")
        self.__reporter.log_write(f"vdnagen start time: {task.vdg_start_time}")
        self.__reporter.log_write(f"vdnagen end time: {task.vdg_end_time}")
        self.__reporter.log_write(f"vdnagen time used {task.vdg_time_used} sec")
        self.__reporter.log_write("done.")

    def __vdg_tasks_log_update(self):
        """ VDNAGen 日志更新
        打印新完成的任务日志
        """
        for i in range(len(self.__vdg_tasks_done) - self.__vdg_tasks_done_tr):
            self.__reporter.log_write(
                f"=============== Success VDNAGen task {self.__vdg_tasks_done_tr + i + 1} ===============")
            task_id = self.__vdg_tasks_done[self.__vdg_tasks_done_tr + i]
            self.__vdg_task_log_update_op(task_id)
        self.__far_path_log_update()
        self.__vdg_tasks_done_tr = len(self.__vdg_tasks_done)

        for i in range(len(self.__vdg_tasks_error) - self.__vdg_tasks_error_tr):
            self.__reporter.log_write(
                f"=============== Error VDNAGen task {self.__vdg_tasks_error_tr + i + 1} ===============")
            task_id = self.__vdg_tasks_error[self.__vdg_tasks_error_tr + i]
            self.__vdg_task_log_update_op(task_id)
        self.__vdg_tasks_error_tr = len(self.__vdg_tasks_error)

    def __fpg_log_update_op(self, task_id: int):
        """ 打印日志日志的基本内容
        :param task_id: 任务索引
        :return:
        """
        task: Task = self.__tasks[task_id]  # 这就等于获得当面任务的执行结果
        self.__reporter.log_write(f"media path: {task.media_path}")
        self.__reporter.log_write(f"media size: {file_size_format(task.media_size)}")
        self.__reporter.log_write(f"media shape: {task.media_width}x{task.media_height}")
        self.__reporter.log_write(f"media codec: {task.media_codec}")
        self.__reporter.log_write(f"media duration : {task.media_duration} sec")
        if os.path.isfile(task.compress_path):
            self.__reporter.log_write(f"compress path: {task.compress_path}")
            self.__reporter.log_write(f"compress size: {file_size_format(task.compress_size)}")
        self.__reporter.log_write(f"compress command: {task.compress_cmd}")
        self.__reporter.log_write(f"compress start time: {task.compress_start_time}")
        self.__reporter.log_write(f"compress end time: {task.compress_end_time}")
        self.__reporter.log_write(f"compress time used {task.compress_time_used} sec")
        gpu_id = task.fpg_gpu_id
        device = "Error Device"
        if gpu_id == -1:
            device = "CPU"
        elif gpu_id >= 0:
            device = f"GPU:{gpu_id}"
        self.__reporter.log_write(f"ffmpeg use device: {device}")
        self.__reporter.log_write("done.")

    def __fpg_tasks_log_update(self):
        """ ffmpeg 视频压缩日志更新
        打印新完成的任务日志
        """
        for i in range(len(self.__fpg_tasks_done) - self.__fpg_tasks_done_tr):
            self.__reporter.log_write(
                f"=============== Success ffmpeg task {self.__fpg_tasks_done_tr + i + 1} ===============")
            task_id = self.__fpg_tasks_done[self.__fpg_tasks_done_tr + i]
            self.__fpg_log_update_op(task_id)
        self.__fpg_tasks_done_tr = len(self.__fpg_tasks_done)

        for i in range(len(self.__fpg_tasks_error) - self.__fpg_tasks_error_tr):
            task_id = self.__fpg_tasks_error[self.__fpg_tasks_error_tr + i]
            self.__reporter.log_write(
                f"=============== Error ffmpeg task {self.__fpg_tasks_error_tr + i + 1} ===============")
            self.__fpg_log_update_op(task_id)
        self.__fpg_tasks_error_tr = len(self.__fpg_tasks_error)

    def __tasks_report_export(self):
        """ 将任务运行结果导出为报告
        """
        tpl = {
            "media_path": "",
            "media_size": "",
            "media_codec": "",
            "media_shape": "",
            "media_duration(s)": "",
            "far_path": "",
            "far_size": "",
            "status": "",
            "gpu_device": "",
            "gpu_start_time": "",
            "gpu_end_time": "",
            "gpu_time_used(s)": "",
            "vdnagen_start_time": "",
            "vdnagen_end_time": "",
            "vdnagen_time_used(s)": ""
        }
        reports = []
        for task in self.__tasks:
            report = deepcopy(tpl)
            report["media_path"] = task.media_path
            report["media_size"] = file_size_format(task.media_size)
            report["far_path"] = task.far_path
            report["media_duration(s)"] = task.media_duration
            if task.status in [TaskStatus.null, TaskStatus.parse_error]:
                report["status"] = "视频解析错误"
                reports.append(report)
                continue

            report["media_codec"] = task.media_codec
            report["media_shape"] = f"{task.media_width}x{task.media_height}"

            gpu_id = task.fpg_gpu_id
            if gpu_id >= -1:
                # 有进行视频压缩，记录信息
                if gpu_id == -1:
                    report["gpu_device"] = "CPU"
                elif gpu_id >= 0:
                    report["gpu_device"] = f"GPU:{gpu_id}"
                report["gpu_start_time"] = task.compress_start_time
                report["gpu_end_time"] = task.compress_end_time
                report["gpu_time_used(s)"] = task.compress_time_used
                if task.status == TaskStatus.compress_error:
                    report["status"] = "视频压缩错误"
                    reports.append(report)
                    continue
            report["vdnagen_start_time"] = task.vdg_start_time
            report["vdnagen_end_time"] = task.vdg_end_time
            report["vdnagen_time_used(s)"] = task.vdg_time_used
            if task.status == TaskStatus.dnagen_error:
                report["status"] = "基因生成错误"
                reports.append(report)
                continue
            report["far_size"] = file_size_format(task.far_size)
            report["status"] = "执行成功"
            reports.append(report)
        task_report = pd.DataFrame(reports)
        self.__reporter.xlsx_write(task_report)

    def tasks_run(self):
        """ 采用多线程执行任务
        """

        # self.__tasks = self.__tasks[:3]
        self.__tasks_init()
        self.__reporter.log_write(f"start {self.__num_workers} thread to running {len(self.__tasks)} task...")
        self.__reporter.log_write(f"{len(self.__fpg_tasks_wait)} tasks need to compressed.")

        while len(self.__vdg_tasks_wait) + \
                len(self.__fpg_tasks_running) + \
                len(self.__vdg_tasks_running) + \
                len(self.__fpg_tasks_wait) > 0:
            self.__fpg_tasks_queue_update()
            self.__fpg_tasks_log_update()
            self.__vdg_tasks_queue_update()
            self.__vdg_tasks_log_update()
            time.sleep(1)
        self.__reporter.log_write(f"{self.__num_workers} thread to running {len(self.__tasks)} task done.")
        self.__tasks_report_export()


def batch_far_create(input: str, output: str, num_workers: int, cache: str = "/tmp/batch_far_create"):
    """
    批量far文件生成入口函数
    :param input: 视频文件所在路径或指明视频路径的文本文件
    :param output: far文件所在路径
    :param num_workers: 工作线程数
    :param cache: 中间结果缓存路径
    :return:
    """
    fc = FarCreater(num_workers, fpg_cache=os.path.join(cache, "ffmpeg_compress"))
    if os.path.isfile(input):
        fc.tasks_add_from_file(input, output)
    else:
        fc.tasks_add_from_dir(input, output)
    fc.tasks_run()


def parse_args():
    """
    定义脚本输入参数，并完成解析
    :return:
    """
    parser = argparse.ArgumentParser(prog="./BatchFarCreate.py", description="批量视频far文件生成")
    parser.add_argument("-i", "--input", type=str, required=True, help="源视频文件路径信息")
    parser.add_argument("-o", "--output_dir", type=str, required=True, help="far文件保存路径")
    parser.add_argument("--cache", type=str, default="/tmp/cache", required=False, help="中间缓存路径")
    parser.add_argument("--num_workers", default=int(os.cpu_count() / 1.5) + 1, type=int, required=False, help="工作线程数")
    return parser.parse_args()


def main():
    args = parse_args()

    # 进程重复启动检测
    import subprocess
    proc = subprocess.Popen(["pgrep", "-f", __file__], stdout=subprocess.PIPE)
    std = [p for p in proc.communicate() if p is not None]
    if len(std[0].decode().split()) > 1:
        exit('Already running')

    time_begin = time.time()
    batch_far_create(args.input, args.output_dir, args.num_workers, args.cache)
    time_end = time.time()
    print(f"总共用时: {time_end - time_begin:.3f}s")


if __name__ == '__main__':
    main()
