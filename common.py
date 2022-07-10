# coding: utf-8
import hashlib
import os
import shlex
import shutil
import subprocess
import time
from typing import Tuple


def sh2bash(cmd: str) -> str:
    return "bash -c {sh}".format(sh=shlex.quote(cmd))


def getstatusoutput_s(cmd: str) -> Tuple[int, str]:
    try:
        return subprocess.getstatusoutput(cmd)
    except Exception:
        return -1, ""


def video_duration_get(path: str):
    """
    通过ffprobe获得视频时长
    :param path: 视频文件地址
    :return:
    """
    duration = -1
    if os.path.isfile(path):
        sts, output = getstatusoutput_s(
            f"ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 {shlex.quote(path)}")
        if sts == 0:
            try:
                # 这里的视频时长信息再最后一行，最前面的信息进行过滤
                *_, output = output.split("\n")
                duration = int(float(output))
            except Exception:
                pass
    return duration


def time_now_get() -> str:
    """
    获得当前时间
    :return:
    """
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def file_size_format(size: int) -> str:
    """
    将文件大小进行格式化
    :param size:
    :return:
    """
    if size < 1000:
        return '%i' % size + 'bytes'
    elif 1000 <= size < 1000000:
        return '%.1f' % float(size / 1000) + 'KB'
    elif 1000000 <= size < 1000000000:
        return '%.1f' % float(size / 1000000) + 'MB'
    elif 1000000000 <= size < 1000000000000:
        return '%.1f' % float(size / 1000000000) + 'GB'
    elif 1000000000000 <= size:
        return '%.1f' % float(size / 1000000000000) + 'TB'


def str_md5_get(content: bytes) -> str:
    """
    计算字符串md5
    :param content:
    :return:
    """
    m = hashlib.md5(content)  # 创建md5对象
    return m.hexdigest()


def file_md5_get(file_name):
    """
    计算文件的md5
    :param file_name:
    :return:
    """
    m = hashlib.md5()  # 创建md5对象
    with open(file_name, 'rb') as fobj:
        while True:
            data = fobj.read(4096)
            if not data:
                break
            m.update(data)  # 更新md5对象

    return m.hexdigest()  # 返回md5对象


def user_time_get(time_output: str) -> int:
    """ 获得time命令下程序执行所消耗的时间
    :param time_output:
    :return:
    """
    try:
        *_, real_time, user_time, sys_time = time_output.split("\n")
        m, s = user_time.replace("user\t", "").split("m")
        s = s.replace("s", "")
        return int(m) * 60 + int(float(s))
    except Exception as e:
        return -1


def real_time_get(time_output: str) -> int:
    """ 获得time命令下程序执行所消耗的时间
    :param time_output:
    :return:
    """
    try:
        *_, real_time, user_time, sys_time = time_output.split("\n")
        m, s = real_time.replace("real\t", "").split("m")
        s = s.replace("s", "")
        return int(m) * 60 + int(float(s))
    except Exception as e:
        return -1


def xml_str_escape(s):
    return s.replace('&', "&amp;") \
        .replace('"', "&quot;") \
        .replace('\'', "&#39;") \
        .replace('<', "&lt;") \
        .replace(">", "&gt;")


def far_is_video_far(far_path: str, cache: str = "./far_split.d"):
    """
    判断far文件是否为视频dna
    :param far_path:
    :param cache:
    :return:
    """
    far_path = os.path.abspath(far_path)
    if not os.path.isfile(far_path):
        return False
    cache = os.path.abspath(cache)
    sub_cache = os.path.join(cache, os.path.basename(far_path) + ".far_split")
    support_log = os.path.join(cache, os.path.basename(far_path) + ".sup")
    if os.path.isfile(support_log):
        with open(support_log, mode="r", encoding="utf-8") as f:
            line = f.readline()
            return line.strip() == "Support"
    os.makedirs(sub_cache, exist_ok=True)
    split_cmd = f"/usr/local/VDNAGen/far_split -i {shlex.quote(far_path)} -d {shlex.quote(sub_cache)}"
    split_cmd = sh2bash(split_cmd)
    stats_file = os.path.join(sub_cache, "stats")
    sts, output = getstatusoutput_s(split_cmd)
    support_codec = {"flv", "h264", "hevc", "mpeg1video", "mpeg2video", "mpeg4", "msmpeg4", "rv30", "rv40", "theora",
                     "vp6f", "vp9", "wmv3"}
    waning_codec = {"ansi", "mjpeg", "png", "qtrle", "svq1"}

    # 默认情况，判断不支持
    with open(support_log, mode="w", encoding="utf-8") as f:
        flag = "No Support"
        f.write("\n".join([flag, "", far_path]))

    if sts == 0 and os.path.isfile(stats_file):
        with open(stats_file, mode="r", encoding="utf-8") as f:
            stats_data = f.read()
        vc_tag_l = "<VideoCodec>"
        vc_tag_r = "</VideoCodec>"
        if vc_tag_l in stats_data and vc_tag_r in stats_data:
            content_left = stats_data.index(vc_tag_l) + len(vc_tag_l)
            content_right = stats_data.index(vc_tag_r)
            content = stats_data[content_left:content_right]
            content = content.strip()
            with open(support_log, mode="w", encoding="utf-8") as f:
                flag = "Support" if content in support_codec else "No Support"
                f.write("\n".join([flag, content, far_path]))

    shutil.rmtree(sub_cache)
    with open(support_log, mode="r", encoding="utf-8") as f:
        line = f.readline()
        return line.strip() == "Support"


def far_video_duration_get(far_path: str, cache: str = "./far_split.d"):
    far_path = os.path.abspath(far_path)
    if not os.path.isfile(far_path):
        return -1

    cache = os.path.abspath(cache)
    sub_cache = os.path.join(cache, os.path.basename(far_path) + ".far_split")
    duration_log = os.path.join(cache, os.path.basename(far_path) + ".dur")
    if os.path.isfile(duration_log):
        with open(duration_log, mode="r", encoding="utf-8") as f:
            data = f.read().strip()
        try:
            return int(data)
        except:
            return -1

    if os.path.exists(sub_cache):
        shutil.rmtree(sub_cache)
    os.makedirs(sub_cache, exist_ok=True)

    split_cmd = f"/usr/local/VDNAGen/far_split -i {shlex.quote(far_path)} -d {shlex.quote(sub_cache)}"
    split_cmd = sh2bash(split_cmd)
    merge_dna = os.path.join(sub_cache, "merged.dna")
    status_cmd = f"/usr/local/VDNAGen/dna_status -i {shlex.quote(merge_dna)}"
    status_cmd = sh2bash(status_cmd)
    duration = -1
    sts, output = subprocess.getstatusoutput(split_cmd)
    if sts == 0 and os.path.isfile(merge_dna):
        sts, output = subprocess.getstatusoutput(status_cmd)
        output: list = [line for line in output.split("\n") if line.startswith("LENGTH=")]
        if len(output) > 0:
            output = output[0]
            output: str = output.replace("LENGTH=", "")
            try:
                duration = int(output)
            except Exception:
                duration = -1
    shutil.rmtree(sub_cache)
    with open(duration_log, mode="w", encoding="utf-8") as f:
        f.write(f"{duration}")
    return duration


def mediawise_stdout_get_json(stdout: str) -> str:
    """
    从stdout从提取json信息
    :param stdout:
    :return:
    """
    res = ""
    if "{" in stdout and "}" in stdout:
        start_idx = 0
        for i in range(len(stdout)):
            if stdout[i] == "{":
                start_idx = i
                break
        end_idx = len(stdout)
        for i in range(len(stdout), 0, -1):
            if stdout[i - 1] == "}":
                end_idx = i
                break
        res = stdout[start_idx:end_idx]
    return res


def vdnagen_stdout_get_xml(stdout: str, tag: str) -> str:
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
