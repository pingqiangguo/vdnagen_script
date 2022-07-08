# -*- coding: utf-8 -*-
import json
import os
import random
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from typing import Optional, List

import pandas as pd

from VDNAGen import VDNAGen
from common import file_size_format, str_md5_get, time_now_get


class VDDBMatcher:
    def __init__(self, host: str,
                 user: str,
                 passwd: str,
                 cache_dir: str = "/tmp/far_match_cache"):
        """
        :param host: 域名VDDB查询地址
        :param user: VDDB查询账号
        :param passwd: VDDB查询账号密码
        :param cache_dir: VDDB查询结果的缓存路径
        """
        self.__cache_dir = cache_dir
        os.makedirs(self.__cache_dir, exist_ok=True)

        self.__host = host
        self.__user = user
        self.__passwd = passwd
        self.__vdg = VDNAGen()
        self.__vdg.host_set(self.__host)
        self.__vdg.user_set(self.__user)
        self.__vdg.passwd_set(self.__passwd)

        # [[电影或原始far文件路径, 拷贝后的far文件路径],...]
        self.__paths = []

    def tasks_sort(self):
        """
        按照处理文件的大小对任务进行排序
        :return:
        """
        # 按照视频文件的大小进行排序, 优先查询小文件
        self.__paths.sort(key=lambda item: os.path.getsize(item[0]))

    def tasks_shuffle(self):
        """
        对所有任务进行乱序
        :return:
        """
        random.shuffle(self.__paths)

    def tasks_export(self, xlsx_export: str = "./vddb_match_tasks.xlsx"):
        """
        导出所有任务的详细信息
        :param xlsx_export:xlsx文件路径
        :return:
        """
        tasks = []
        for idx, (movie_path, far_path) in enumerate(self.__paths):
            movie_size = file_size_format(os.path.getsize(movie_path)) if os.path.isfile(movie_path) else "not found"
            far_size = file_size_format(os.path.getsize(far_path)) if os.path.isfile(far_path) else "not found"
            tasks.append([idx + 1, movie_path, movie_size, far_path, far_size])
        df = pd.DataFrame(tasks)
        df.columns = ["index", "movie_path", "movie_size", "far_path", "far_szie"]
        df.to_excel(xlsx_export, index=False)

    def tasks_add(self, movie_path: str,
                  far_path: str) -> None:
        """
        添加任务信息
        :param movie_path: 视频/far文件路径
        :param far_path: 生成的far文件路径
        :return:
        """
        if os.path.isfile(movie_path):
            self.__paths.append([movie_path, far_path])

    def tasks_add_from_dir(self, movie_dir: str,
                           far_dir: str) -> None:
        """
        分析文件夹中的文件, 添加任务信息
        :param movie_dir: 视频/far文件夹路径
        :param far_dir: 生成的far文件夹路径
        :return:
        """
        for movie_name in os.listdir(movie_dir):
            movie_path = os.path.join(movie_dir, movie_name)
            if not os.path.isfile(movie_path):
                continue
            far_name, _ = os.path.splitext(movie_name)
            far_name = "样本-%s.far" % far_name
            far_path = os.path.join(far_dir, far_name)
            self.tasks_add(movie_path, far_path)

    def match_config_update(self, host: str,
                            user: str,
                            passwd: str) -> None:
        """
        利用host username password更新VDNAGen查询配置
        :param host: 域名VDDB查询地址
        :param user: VDDB查询账号
        :param passwd: VDDB查询账号密码
        :return:
        """
        self.__host = host
        self.__user = user
        self.__passwd = passwd
        self.__vdg.host_set(self.__host)
        self.__vdg.user_set(self.__user)
        self.__vdg.passwd_set(self.__passwd)

    @staticmethod
    def __match_log_parse(json_str: str) -> List[dict]:
        """
        提取json匹配结果中的匹配信息, 并返回
        :param json_str:
        :return:
        """
        res = []
        match_log: dict = json.loads(json_str)
        if match_log["mode"] != "far_db_match" \
                or match_log["exit_code"] != 0 \
                or "stdout2json" not in match_log.keys():
            return res

        tpl = {
            # 文件信息
            "movie_path": "",  # 视频路径
            "movie_size": "",
            "far_path": "",  # far文件路径
            "far_size": "",

            # 查询时间与查询状态
            "start_time": "",
            "end_time": "",
            "error": "",  # 错误信息 格式 error_code(error_message)
            "TaskID": "",

            # 匹配信息
            "match_count": "",  # 匹配数
            # "match_type": "",  # 匹配类型 Video(视频匹配) Audio(音频匹配) AV(音视频匹配)
            "Title": "",  # 匹配到的视频名称
            "AssetID": "",  # 匹配母本的唯一标识号
            "SampleOffset": "",  # 样本的偏移时间
            "RefOffset": "",  # 母本的偏移时间
            "MatchDuration": "",  # 母本匹配时间
            # "Likelihood": ""  # 匹配的相似度
        }

        # 原始数据路径
        movie_path = match_log.get("movie_path", "")
        far_path = match_log.get("far_path", "")
        tpl["movie_path"] = movie_path
        tpl["movie_size"] = file_size_format(os.path.getsize(movie_path)) if os.path.isfile(movie_path) else ""
        tpl["far_path"] = far_path
        tpl["far_size"] = file_size_format(os.path.getsize(far_path)) if os.path.isfile(far_path) else ""
        tpl["start_time"] = match_log.get("time_start", "")
        tpl["end_time"] = match_log.get("time_done", "")

        # 如果stdout2json不存在, 表示脚本查询出错, 是脚本问题
        stdout2json = match_log.get("stdout2json", {})
        if len(stdout2json) == 0:
            res.append(tpl)
            return res
        # 执行到这里, 下面的信息就是服务器返回的json信息, 解析这些信息
        head = stdout2json.get("Head", {})
        error_code = int(head.get("ErrorCode", -2))
        error_message = head.get("ErrorMessage", "ScriptError")
        tpl["error"] = f"{error_code}({error_message})"

        body = stdout2json.get("Body", {})
        query = body.get("Query", [])
        if len(query) == 0:
            tpl["match_count"] = 0
            res.append(tpl)

        for query_idx, query_item in enumerate(query):
            # 这里会有多个query, query因为跟后台服务器数量有关
            # 一台后端服务器返回的结果就是一个query
            tpl_query = deepcopy(tpl)
            tpl_query["TaskID"] = query_item.get("QueryLog", {}).get("TaskID", "")

            match_list = query_item.get("Match", [])
            tpl_query["match_count"] = len(match_list)
            if len(match_list) == 0:
                res.append(tpl_query)
            # match_item 服务器查询的一个母本匹配结果
            for match_item in match_list:
                tpl_match = deepcopy(tpl_query)
                # 记录匹配信息
                # 母本的唯一标识信息(meta_uid) 母本入库时候返回的唯一标识号
                tpl_match["AssetID"] = match_item.get("AssetID", "")
                # 匹配类型 Video(视频匹配) Audio(音频匹配) AV(音视频匹配)型
                # tpl_match["match_type"] = match_item.get("Type", "")
                # 匹配的视频名称
                tpl_match["Title"] = match_item.get("Asset", {}).get("Title", "")

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
                    # tpl_match["Likelihood"] = likelihood
                    tpl_match["MatchDuration"] = match_duration
                    tpl_match["SampleOffset"] = sample_offset
                    tpl_match["RefOffset"] = reference_offset
                res.append(tpl_match)
        return res

    def __task_runner(self, movie_path: str,
                      far_path: str,
                      rematch: bool = False) -> str:
        """
        多线程进行VDDB查询的任务入口函数
        :param movie_path: 本地视频/far文件路径
        :param far_path: 生成far文件路径
        :param rematch: 不使用本地缓存重新进行查询
        :return:
        """
        if not os.path.isfile(far_path) and movie_path.endswith(".far"):
            # 如果movie_path是far文件, 且far_path文件不存在,则拷贝
            shutil.copy(movie_path, far_path)
        else:
            # 调用VDNAGen模块生成基因文件
            self.__vdg.far_create(movie_path, far_name=far_path)
        # 获得日志文件
        # 日志文件名可以使用md5(far文件) 或者 md5(far文件名)
        # 前者保证不重复对内容相同的far文件进行查询,后者保证不对相同路径的far文件进行查询
        log_path = os.path.join(self.__cache_dir, str_md5_get(f"task({movie_path}, {far_path})".encode("utf-8")))
        if not rematch and os.path.isfile(log_path):
            with open(log_path, mode="r", encoding="utf-8") as f:
                task_log = f.read()
                js = json.loads(task_log)
                if js["exit_code"] == 0 and "stdout" not in js.keys():
                    return task_log
        time_run_start = time.time()
        time_fmt_run_start = time_now_get()
        task_log = self.__vdg.far_db_match(far_path)
        time_run_stop = time.time()
        time_fmt_run_stop = time_now_get()
        task_log = json.loads(task_log)
        task_log["movie_path"] = movie_path
        task_log["far_path"] = far_path
        task_log["time_used"] = time_run_stop - time_run_start
        task_log["time_start"] = time_fmt_run_start
        task_log["time_done"] = time_fmt_run_stop
        task_log = json.dumps(task_log, indent=2, ensure_ascii=False)
        with open(log_path, mode="w") as f:
            f.write(task_log)
        return task_log

    def tasks_run(self, num_workers: int = 1,
                  rematch: bool = False,
                  log_path: Optional[str] = None,
                  xlsx_export="./vddb_match_report.xlsx") -> None:
        """
        运行VDDB查询任务
        :param num_workers: 工作的线程数
        :param rematch: 当本地有样本匹配结果缓存时, 时候使用缓存
        :param log_path: 运行日志文件路径
        :param xlsx_export: 导出excel报告文件路径
        :return:
        """
        if num_workers < 1:
            num_workers = 1
        tasks = []  # 任务队列
        pool = ThreadPoolExecutor(max_workers=num_workers)  # 线程池
        # 创建任务并添加任务到队列
        for movie_path, far_path in self.__paths:
            task = pool.submit(self.__task_runner, movie_path, far_path, rematch)
            tasks.append(task)

        task_cnt = len(tasks)
        log = f"[{time_now_get()}]start {num_workers} thread to running {task_cnt} task..."
        if num_workers == 1:
            print(log)
        if log_path is not None:
            with open(log_path, mode="w", encoding="utf-8") as f:
                f.write(log + "\n")
        # 开启任务轮询, 知道所有任务结束
        match_results = []
        for idx, task in enumerate(tasks):
            log = f"[{idx + 1}:{task_cnt}] [{time_now_get()}] running match [movie: {self.__paths[idx][0]}] [far: {self.__paths[idx][1]}]"
            if num_workers == 1:
                print(log)
            tasklog = task.result()
            match_results.extend(self.__match_log_parse(tasklog))
            if num_workers == 1:
                print(tasklog)
            if log_path is not None:
                with open(log_path, mode="a", encoding="utf-8") as f:
                    f.write(log + "\n")
                    f.write(tasklog + "\n")
        pool.shutdown()
        # 对匹配结果进行排序, 没有匹配结果的放到最前面, 音频匹配第二, 视频匹配第三, 音视频都匹配第四
        # match_results.sort(key=lambda dic: ["", "Audio", "Video", "AV"].index(dic["match_type"]))
        match_results = pd.DataFrame(match_results)
        if xlsx_export is not None:
            match_results.to_excel(xlsx_export)

        log = f"[{time_now_get()}] start {num_workers} thread to running {task_cnt} task done."
        if num_workers == 1:
            print(log)
        if log_path is not None:
            with open(log_path, mode="a", encoding="utf-8") as f:
                f.write(log + "\n")
