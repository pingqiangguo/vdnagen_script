#!/miniconda3/envs/py39us/bin/python
# coding: utf-8
import argparse
import json
import logging
import os
import shlex
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from enum import IntEnum
from typing import List

import pandas as pd

from common import far_is_video_far
from common import far_video_duration_get
from common import getstatusoutput_s
from common import log_formatter_get
from common import mediawise_stdout_get_json
from common import real_time_get
from common import sh2bash
from common import str_md5_get
from common import symlink_real_path
from common import time_now_get

backup = os.path.join(os.getcwd(), "backup")
log_filename = "batch_far_match.log"
far_path_report = "batch_far_match_far_path.txt"
xlsx_export = "batch_far_match_report.xlsx"

# 使用上次查询结果
vdnagen_rematch = False


# 重新查询
# vdnagen_rematch = True


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

        self.far_path_report = os.path.join(os.getcwd(), far_path_report)
        self.far_path_report_bkp = os.path.join(self.backup_dir, f"{time_start}-{far_path_report}")
        if os.path.isfile(self.far_path_report):
            os.remove(far_path_report)

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

    def far_path_write(self, far_path: str) -> None:
        with open(self.far_path_report, mode="a", encoding="utf-8") as f:
            f.write(far_path + "\n")
        with open(self.far_path_report_bkp, mode="a", encoding="utf-8") as f:
            f.write(far_path + "\n")

    def xlsx_write(self, df: pd.DataFrame) -> None:
        """ far生成报告导出
        """
        df.to_excel(self.__xlsx_export)
        df.to_excel(self.__xlsx_export_bkp)


class TaskStatus(IntEnum):
    null = -1
    parse_error = 0
    task_create = 1
    need_match = 2
    no_need_match = 3
    match_running = 4
    match_done = 5
    match_error = 6


class Task:
    def __init__(self):
        self.status = TaskStatus.null
        self.far_path = ""
        self.far_size = -1
        self.media_duration = -1
        self.match_cmd = ""
        self.match_task_proc = None
        self.task_id = ""
        self.match_start_time = ""
        self.match_end_time = ""
        self.match_time_used = -1
        self.match_count = -1
        self.title = []
        self.asset_id = []
        self.sample_off = []
        self.ref_off = []
        self.match_duration = []
        self.likelihood = []
        self.request = {}

    def dump(self, file: str):
        res = {
            "status": self.status,
            "far_path": self.far_path,
            "far_size": self.far_size,
            "media_duration": self.media_duration,
            "match_cmd": self.match_cmd,
            "task_id": self.task_id,
            "match_start_time": self.match_start_time,
            "match_end_time": self.match_end_time,
            "match_time_used": self.match_time_used,
            "match_count": self.match_count,
            "title": self.title,
            "asset_id": self.asset_id,
            "sample_off": self.sample_off,
            "ref_off": self.ref_off,
            "match_duration": self.match_duration,
            "likelihood": self.likelihood,
            "request": self.request
        }
        jstr = json.dumps(res, indent=2, ensure_ascii=False)
        with open(file, mode="w") as f:
            f.write(jstr)

    def load(self, file):
        if os.path.isfile(file):
            with open(file, mode="r", encoding="utf-8") as f:
                js = json.load(f)
            self.status = TaskStatus(js.get("status", int(TaskStatus.null)))
            self.far_path = js.get("far_path", "")
            self.far_size = js.get("far_size", -1)
            self.media_duration = js.get("media_duration", -1)
            self.match_cmd = js.get("match_cmd", "")
            self.task_id = js.get("task_id", "")
            self.match_start_time = js.get("match_start_time", "")
            self.match_end_time = js.get("match_end_time", "")
            self.match_time_used = js.get("match_time_used", -1)
            self.match_count = js.get("match_count", -1)
            self.title = js.get("title", [])
            self.asset_id = js.get("asset_id", [])
            self.sample_off = js.get("sample_off", [])
            self.ref_off = js.get("ref_off", [])
            self.match_duration = js.get("match_duration", [])
            self.likelihood = js.get("likelihood", [])
            self.request = js.get("request", "")


class FarMatcher:
    reporter = Reporter()

    def __init__(self, host: str, user: str, passwd: str, num_workers: int = 40, match_cache: str = "/tmp/far_match"):
        self.__host = host
        self.__user = user
        self.__passwd = passwd

        os.makedirs(match_cache, exist_ok=True)
        self.match_cache = match_cache
        self.__tasks: List[Task] = []
        self.__tasks_init_error: List[Task] = []

        self.__num_workers = 1
        if num_workers > 1:
            self.__num_workers = num_workers

        self.__match_pools = ThreadPoolExecutor(max_workers=self.__num_workers)
        self.__match_tasks_wait: List[int] = []  # match 还没开始运行的
        self.__match_tasks_running: List[int] = []  # match 正在运行的
        self.__match_tasks_done: List[int] = []  # match 已经运行结束的
        self.__match_tasks_error: List[int] = []  # match 运行出错的任务

        # match 结果查询的控制变量
        self.__match_tasks_done_tr: int = 0  # match 已经运行结束的 上次遍历结束的位置
        self.__match_tasks_error_tr: int = 0  # match 运行错误的 上次遍历结束的位置

    def task_add(self, far_path: str) -> None:
        far_path = os.path.abspath(far_path)
        if os.path.isfile(far_path) and far_path.endswith(".far"):
            task = Task()
            task.far_path = far_path
            task.far_size = os.path.getsize(far_path)
            try:
                cache_dir = os.path.join(self.match_cache, str_md5_get(far_path.encode("utf-8")))
                os.makedirs(cache_dir, exist_ok=True)
                if far_is_video_far(far_path, cache_dir):
                    self.reporter.logger.info(f"{far_path} task add success")
                    self.reporter.far_path_write(far_path)
                    task.media_duration = far_video_duration_get(far_path, cache_dir)
                    task.status = TaskStatus.task_create
                    self.__tasks.append(task)
                else:
                    self.reporter.logger.warning(f"{far_path} not support.")
                    task.status = TaskStatus.no_need_match
                    self.__tasks_init_error.append(task)
            except:
                self.reporter.logger.warning(f"{far_path} parse error.")
                task.status = TaskStatus.parse_error
                self.__tasks_init_error.append(task)
        else:
            self.reporter.logger.warning(f"{far_path} not found or suffix error, ignored.")

    def tasks_add_from_dir(self, far_dir: str) -> None:
        for sub in os.listdir(far_dir):
            sub_path = os.path.join(far_dir, sub)
            if os.path.isdir(sub_path):
                sub_far_dir = sub_path
                self.tasks_add_from_dir(sub_far_dir)
            elif os.path.isfile(sub_path):
                far_path = sub_path
                self.task_add(far_path)

    def tasks_add_from_file(self, file: str):
        if not os.path.isfile(file):
            self.reporter.logger.error(f"{file} not found.")
            sys.exit(-1)
        with open(file, mode="r", encoding="utf-8") as f:
            for far_path in f.readlines():
                far_path = far_path.strip()
                self.task_add(far_path)

    def __tasks_init(self):

        self.__match_tasks_wait = []
        self.__match_tasks_done = []
        self.__match_tasks_running = []
        self.__match_tasks_error = []

        self.__match_tasks_done_tr = 0
        self.__match_tasks_error_tr = 0

        self.__match_tasks_wait = [*range(len(self.__tasks))]

    def __request_parse(self, task_id: int):
        if 0 <= task_id < len(self.__tasks):
            task: Task = self.__tasks[task_id]
        else:
            return
        request = task.request
        head: dict = request.get("Head", {})
        error_code: int = int(head.get("ErrorCode", -1))
        if error_code != 0:
            return

        body: dict = request.get("Body", {})
        query: list = body.get("Query", [])
        if len(query) == 0:
            return

        for query_idx, query_item in enumerate(query):
            # 这里会有多个query, query因为跟后台服务器数量有关
            # 一台后端服务器返回的结果就是一个query
            task.task_id = query_item.get("QueryLog", {}).get("TaskID", "")
            match_list = query_item.get("Match", [])
            task.match_count = len(match_list)
            if len(match_list) == 0:
                return

            # match_item 服务器查询的一个母本匹配结果
            for match_item in match_list:
                # 记录匹配信息
                # 母本的唯一标识信息(meta_uid) 母本入库时候返回的唯一标识号
                task.asset_id.append(match_item.get("AssetID", ""))
                # 匹配类型 Video(视频匹配) Audio(音频匹配) AV(音视频匹配)型
                # tpl_match["match_type"] = match_item.get("Type", "")
                # 匹配的视频名称
                task.title.append(match_item.get("Asset", {}).get("Title", ""))

                # 从多段匹配记录中,提取最长的匹配段
                likelihood = ""
                # 样本与母本匹配的时间长度
                match_duration = ""
                # 测试样本的偏移时间
                sample_offset = ""
                # 母本的偏移时间
                reference_offset = ""
                match_duration_sec = 0

                track_list = match_item.get("MatchDetail", {}).get("Track", [])
                if len(track_list) != 0:
                    for track_item in track_list:
                        duration = track_item.get("MatchDuration", "")
                        if len(duration) == 0:
                            continue
                            # 时:分:秒
                        h, m, s = duration.split(":")
                        h = int(h)
                        m = int(m)
                        s = int(s)
                        # 获得分钟
                        duration = (h * 60 + m) * 60 + s
                        if match_duration_sec == -1:
                            match_duration_sec = duration
                            likelihood = track_item.get("Likelihood", "")
                            match_duration = track_item.get("MatchDuration", "")
                            sample_offset = track_item.get("SampleOffset", "")
                            reference_offset = track_item.get("RefOffset", "")
                        elif duration > match_duration_sec:
                            match_duration_sec = duration
                            likelihood = track_item.get("Likelihood", "")
                            match_duration = track_item.get("MatchDuration", "")
                            sample_offset = track_item.get("SampleOffset", "")
                            reference_offset = track_item.get("RefOffset", "")
                task.likelihood.append(likelihood)
                task.match_duration.append(match_duration)
                task.sample_off.append(sample_offset)
                task.ref_off.append(reference_offset)

    def __match_runner(self, task_id: int):
        if 0 <= task_id < len(self.__tasks):
            task: Task = self.__tasks[task_id]
        else:
            return

        if task.status != TaskStatus.need_match:
            task.status = TaskStatus.match_error
            return
        task.status = TaskStatus.match_running
        far_path = task.far_path
        far_name = os.path.basename(far_path)
        cache_dir = os.path.join(self.match_cache, str_md5_get(far_path.encode("utf-8")))
        os.makedirs(cache_dir, exist_ok=True)
        task_dump_path = os.path.join(cache_dir, far_name + ".match")
        if not vdnagen_rematch and os.path.isfile(task_dump_path):
            task.load(task_dump_path)
        if task.status != TaskStatus.match_done:
            match_cmd = f"time python2 {shlex.quote(os.path.join(os.path.dirname(symlink_real_path(__file__)), 'FarQuerySampleCode.py'))} " \
                        f"-s {self.__host} -u {self.__user} -p {self.__passwd}" \
                        f" -i {shlex.quote(far_path)}"
            match_cmd = sh2bash(match_cmd)
            task.match_cmd = match_cmd
            time_begin = time_now_get()
            status, output = getstatusoutput_s(match_cmd)
            time_end = time_now_get()
            task.match_start_time = time_begin
            task.match_end_time = time_end
            task.match_time_used = real_time_get(output)
            request = mediawise_stdout_get_json(output)
            if len(request) == 0:
                task.status = TaskStatus.match_error
                task.request = output
                return
            request = json.loads(request)
            task.request = request
            self.__request_parse(task_id)
            task.status = TaskStatus.match_done
            task.dump(task_dump_path)

    def __match_tasks_queue_update(self):
        # 统计已经完成的任务
        tasks = []
        for task_id in self.__match_tasks_running:
            task: Task = self.__tasks[task_id]
            task_proc = task.match_task_proc
            if task_proc.done():
                tasks.append(task_id)

        # 删除已经完成的任务
        for task_id in tasks:
            self.__match_tasks_running.remove(task_id)
            task: Task = self.__tasks[task_id]
            if task.status == TaskStatus.match_done:
                self.__match_tasks_done.append(task_id)
            else:
                self.__match_tasks_error.append(task_id)

        # 获得新任务
        tasks = []
        for task_id in self.__match_tasks_wait:
            if len(tasks) + len(self.__match_tasks_running) >= self.__num_workers:
                break
            tasks.append(task_id)

        # 启动新任务
        for task_id in tasks:
            task: Task = self.__tasks[task_id]
            task.status = TaskStatus.need_match
            task_proc = self.__match_pools.submit(self.__match_runner, task_id)
            task.match_task_proc = task_proc
            self.__match_tasks_running.append(task_id)
            self.__match_tasks_wait.remove(task_id)

    def __match_task_log_update_op(self, task_id: int):
        if 0 <= task_id <= len(self.__tasks):
            task: Task = self.__tasks[task_id]
        else:
            return
        self.reporter.logger.info(f"far path: {task.far_path}")
        self.reporter.logger.info(f"far size: {task.far_size}")
        self.reporter.logger.info(f"media duration: {task.media_duration}")
        self.reporter.logger.info(f"match command: {task.match_cmd}")
        self.reporter.logger.info(f"match status: {task.status}")
        self.reporter.logger.info(f"match start time: {task.match_start_time}")
        self.reporter.logger.info(f"match end time: {task.match_end_time}")
        self.reporter.logger.info(f"match time used: {task.match_time_used}")
        self.reporter.logger.info(f"match task id: {task.task_id}")
        self.reporter.logger.info(f"match result count: {task.match_count}")
        if task.match_count <= 0:
            return
        for i in range(task.match_count):
            self.reporter.logger.info(f"match result {i + 1}:")
            self.reporter.logger.info(f"\tmatch title: {task.title[i]}")
            self.reporter.logger.info(f"\tmatch asset_id: {task.asset_id[i]}")
            self.reporter.logger.info(f"\tmatch sample offset: {task.sample_off[i]}")
            self.reporter.logger.info(f"\tmatch reference offset: {task.ref_off[i]}")
            self.reporter.logger.info(f"\tmatch duration: {task.match_duration[i]}")
            self.reporter.logger.info(f"\tmatch likelihood: {task.likelihood[i]}")

    def __match_task_log_update(self):
        for i in range(len(self.__match_tasks_done) - self.__match_tasks_done_tr):
            self.reporter.logger.info(
                f"=============== Success Match task {self.__match_tasks_done_tr + i + 1} ===============")
            task_id = self.__match_tasks_done[self.__match_tasks_done_tr + i]
            self.__match_task_log_update_op(task_id)

        self.__match_tasks_done_tr = len(self.__match_tasks_done)
        for i in range(len(self.__match_tasks_error) - self.__match_tasks_error_tr):
            self.reporter.logger.warning(
                f"=============== Error Match task {self.__match_tasks_error_tr + i + 1} ===============")
            task_id = self.__match_tasks_error[self.__match_tasks_error_tr + i]
            self.__match_task_log_update_op(task_id)
        self.__match_tasks_error_tr = len(self.__match_tasks_error)

    def __tasks_report_export(self):
        tpl = {
            # 文件信息
            "far_path": "",  # far文件路径
            "media_duration(s)": "",
            # 查询时间与查询状态
            "start_time": "",
            "end_time": "",
            "error": "",  # 错误信息 格式 error_code(error_message)
            "TaskID": "",

            # 匹配信息
            "match_count": "",  # 匹配数
            "Title": "",  # 匹配到的视频名称
            "AssetID": "",  # 匹配母本的唯一标识号
            "SampleOffset": "",  # 样本的偏移时间
            "RefOffset": "",  # 母本的偏移时间
            "MatchDuration(s)": "",  # 母本匹配时间
            "Likelihood": ""  # 匹配的相似度
        }
        res = []
        for task in self.__tasks:
            report = deepcopy(tpl)
            report["far_path"] = task.far_path
            report["media_duration(s)"] = task.media_duration
            report["start_time"] = task.match_start_time
            report["end_time"] = task.match_end_time
            report["TaskID"] = task.task_id
            if task.status != TaskStatus.match_done or task.match_count < 0:
                report["error"] = "-1(Failed)"
                res.append(report)
                continue
            report["error"] = "0(Success)"
            report["match_count"] = task.match_count
            if task.match_count == 0:
                res.append(report)
                continue
            for match_idx in range(task.match_count):
                report_match_item = deepcopy(report)
                report_match_item["Title"] = task.title[match_idx]
                report_match_item["AssetID"] = task.asset_id[match_idx]
                report_match_item["SampleOffset"] = task.sample_off[match_idx]
                report_match_item["RefOffset"] = task.ref_off[match_idx]
                report_match_item["MatchDuration(s)"] = task.match_duration[match_idx]
                report_match_item["Likelihood"] = task.likelihood[match_idx]
                res.append(report_match_item)
        res = pd.DataFrame(res)
        self.reporter.xlsx_write(res)

    def tasks_run(self):
        self.__tasks_init()
        self.reporter.logger.info(f"start {self.__num_workers} thread to running {len(self.__tasks)} task...")
        while len(self.__match_tasks_wait) + len(self.__match_tasks_running) > 0:
            self.__match_tasks_queue_update()
            self.__match_task_log_update()
            time.sleep(1)
        self.__tasks_report_export()
        self.reporter.logger.info(f"{self.__num_workers} thread to running {len(self.__tasks)} task done.")


def batch_far_match(host: str, user: str, passwd: str, input: str, num_workers: int):
    fm = FarMatcher(host, user, passwd, num_workers)
    if os.path.isfile(input):
        fm.tasks_add_from_file(input)
    else:
        fm.tasks_add_from_dir(input)
    fm.tasks_run()


def parse_args():
    parser = argparse.ArgumentParser(prog="python3 BatchFarMatch.py", description="批量far文件vddb查询")
    parser.add_argument("-s", "--host", type=str, required=True, help="VDDB服务地址")
    parser.add_argument("-u", "--user", type=str, required=True, help="VDDB用户名称")
    parser.add_argument("-p", "--password", type=str, required=True, help="VDDB用户密码")
    parser.add_argument("-i", "--input", type=str, required=True, help="far文件路径信息")
    parser.add_argument("--num_workers", default=1, type=int, required=False, help="工作线程数")
    return parser.parse_args()


def main():
    args = parse_args()

    # 进程重复启动检测
    # import subprocess
    # proc = subprocess.Popen(["pgrep", "-f", __file__], stdout=subprocess.PIPE)
    # std = [p for p in proc.communicate() if p is not None]
    # if len(std[0].decode().split()) > 1:
    #     exit('Already running')

    time_begin = time.time()
    batch_far_match(args.host, args.user, args.password, args.input, args.num_workers)
    time_end = time.time()
    print(f"总共用时: {time_end - time_begin:.3f}s")


if __name__ == '__main__':
    main()
