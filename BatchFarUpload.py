#!/miniconda3/envs/py39us/bin/python
# coding: utf-8

"""
3. 母本批量入库脚本
输入：母本基因文件列表。列表已文本文件的形式提供，文本文件中的每一行对应一个母本基因路径。
功能描述：对母本基因文件列表中的每一个母本基因，进行入库操作。
输出：入库完成后，将入库操作返回的信息保存到结果文件，结果文件命名规则为”基因文件名.far.result”。
"""

import argparse
import json
import os

from VDNAGen import VDNAGen
from common import far_is_video_far, xml_str_escape

host = ""
user = ""
passwd = ""


def batch_far_upload(file: str):
    global host, user, passwd
    vdg = VDNAGen()

    vdg.host_set(host)
    vdg.user_set(user)
    vdg.passwd_set(passwd)
    if not os.path.isfile(file):
        return
    with open(file, mode="r", encoding="utf-8") as f:
        far_paths = f.readlines()
    for far_path in far_paths:
        far_path = far_path.strip()
        if not os.path.isfile(far_path):
            continue
        if not far_is_video_far(far_path):
            print(f"{far_path} not support")
            continue
        log_path = far_path + ".result"
        if os.path.isfile(log_path):
            print(f"{far_path} already exists")
            with open(log_path, mode="r", encoding="utf-8") as f:
                log_dic = json.load(f)
            meta_uid: str = log_dic['receipt']['VobileRefID']
            far_path: str = log_dic['receipt']['FilePath']
            if not os.path.isabs(far_path):
                far_path = os.path.abspath(far_path)
            far_path = xml_str_escape(far_path)
            vdg.far_db_rename(meta_uid, far_path)
            continue

        log = vdg.far_db_insert(far_path)
        log_dic = json.loads(log)
        if "stdout2json" in log_dic.keys():
            log_dic = log_dic['stdout2json']
            error_msg = log_dic['receipt']['ErrorMsg']
            print(f"{far_path} {error_msg}")
            if error_msg in ["Success", "Duplicate instance"]:
                log_data = json.dumps(log_dic, indent=2, ensure_ascii=False)
                with open(log_path, mode="w") as f:
                    f.write(log_data)
                meta_uid: str = log_dic['receipt']['VobileRefID']
                far_path: str = log_dic['receipt']['FilePath']
                if not os.path.isabs(far_path):
                    far_path = os.path.abspath(far_path)
                vdg.far_db_rename(meta_uid, far_path)

        else:
            print(f"{far_path} 基因入库异常:")
            print(log_dic["stdout"])


def parse_args():
    """
    定义脚本执行参数并进行解析
    :return:
    """
    parser = argparse.ArgumentParser(prog="./BatchVDDBUpload.py", description="批量far文件vddb入库")
    parser.add_argument("-s", "--host", type=str, required=True, help="VDDB服务地址")
    parser.add_argument("-u", "--user", type=str, required=True, help="VDDB用户名称")
    parser.add_argument("-p", "--password", type=str, required=True, help="VDDB用户密码")
    parser.add_argument("-f", "--file", type=str, required=True, help="文件本件 指明需要入库的far文件路径")
    return parser.parse_args()


def main():
    args = parse_args()
    global host, user, passwd
    host = args.host
    user = args.user
    passwd = args.password

    # 进程重复启动检测
    import subprocess
    proc = subprocess.Popen(["pgrep", "-f", __file__], stdout=subprocess.PIPE)
    std = [p for p in proc.communicate() if p is not None]
    if len(std[0].decode().split()) > 1:
        exit('Already running')

    batch_far_upload(args.file)


if __name__ == '__main__':
    main()
