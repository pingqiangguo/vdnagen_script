#!/miniconda3/envs/py39us/bin/python
# coding: utf-8
import argparse
import json
import os
import shlex
from typing import List

from common import getstatusoutput_s
from common import sh2bash
from common import str_md5_get


def _stdout_get_xml(stdout: str, tag: str) -> str:
    """
    从stdout从提取xml信息
    :param stdout:
    :param tag:
    :return:
    """
    start_tag = f"<{tag}>"
    end_tag = f"</{tag}>"
    res = ""
    if start_tag in stdout and end_tag in stdout:
        res = stdout[stdout.find(start_tag): stdout.find(end_tag) + len(end_tag)]
    return res


class Task:
    def __init__(self):
        self.far_path = ""
        self.meta_uid = ""
        self.delete_cmd = ""


class FarDBDeleter:
    def __init__(self, host, user, passwd, dbrm_cache="/tmp/far_db_remove"):
        self.host = host
        self.user = user
        self.passwd = passwd

        self.dbrm_cache = dbrm_cache
        os.makedirs(dbrm_cache, exist_ok=True)
        self.tasks: List[Task] = []

    def task_add(self, far_path: str):
        if not os.path.isfile(far_path):
            print(f"{far_path} 文件不存在")
            return
        upload_log = far_path + ".result"
        if not os.path.isfile(upload_log):
            print(f"{far_path} 未找到入库数据")
            return
        try:
            with open(upload_log, mode="r", encoding="utf-8") as f:
                log_data = json.load(f)
        except:
            print(f"{far_path} 检索入库信息失败")
            return
        meta_uid = log_data.get("receipt", {}).get("VobileRefID", "")
        if len(meta_uid) == 0:
            print(f"{far_path} 检索入库信息失败")
            return
        task = Task()

        task.far_path = far_path
        task.meta_uid = meta_uid
        self.tasks.append(task)

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
            print(f"{file} 文件不存在")
            return
        with open(file, mode="r", encoding="utf-8") as f:
            for far_path in f.readlines():
                far_path = far_path.strip()
                self.task_add(far_path)

    def tasks_run(self):
        template_xml = """
<?xml version="1.0" encoding="UTF-8"?>
<Media_Meta>
    <Actions>
        <Action>Delete VDNA</Action>
    </Actions>
    <VobileRefID>%s</VobileRefID>
</Media_Meta>
                """.strip()

        for task in self.tasks:
            print(f"正在删除入库记录: {task.far_path}")
            far_path = task.far_path
            sub_dir = str_md5_get(far_path.encode("utf-8"))
            sub_cache = os.path.join(self.dbrm_cache, sub_dir)
            os.makedirs(sub_cache, exist_ok=True)
            far_name = os.path.basename(far_path)
            delete_xml_path = os.path.join(sub_cache, far_name + ".delete-dna.xml")
            if not os.path.isfile(delete_xml_path):
                delete_xml = template_xml % task.meta_uid
                with open(delete_xml_path, mode="w", encoding="utf-8") as f:
                    f.write(delete_xml)
            delete_cmd = f"VDNAGen -s {shlex.quote(self.host)} -u {shlex.quote(self.user)} " \
                         f"-p {shlex.quote(self.passwd)} -m {shlex.quote(delete_xml_path)}"
            delete_cmd = sh2bash(delete_cmd)
            print(f"执行命令: {delete_cmd}")
            task.delete_cmd = delete_cmd
            sts, stdout = getstatusoutput_s(delete_cmd)
            print(stdout)
            print("done.")


def batch_far_remove(host, user, passwd, input):
    fr = FarDBDeleter(host, user, passwd)
    if os.path.isfile(input):
        fr.tasks_add_from_file(input)
    else:
        fr.tasks_add_from_dir(input)
    fr.tasks_run()


def parse_args():
    """
    定义脚本输入参数，并完成解析
    :return:
    """
    parser = argparse.ArgumentParser(prog="./BatchFarDBDelete.py", description="批量视频far文件取消入库")
    parser.add_argument("-s", "--host", type=str, required=True, help="VDDB服务地址")
    parser.add_argument("-u", "--user", type=str, required=True, help="VDDB用户名称")
    parser.add_argument("-p", "--password", type=str, required=True, help="VDDB用户密码")
    parser.add_argument("-i", "--input", type=str, required=True, help="far路径信息")
    return parser.parse_args()


def main():
    args = parse_args()

    # 进程重复启动检测
    # proc = subprocess.Popen(["pgrep", "-f", __file__], stdout=subprocess.PIPE)
    # std = [p for p in proc.communicate() if p is not None]
    # if len(std[0].decode().split()) > 1:
    #     exit('Already running')

    batch_far_remove(args.host, args.user, args.password, args.input)


if __name__ == '__main__':
    main()
