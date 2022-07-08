# -*- coding: utf-8 -*-
# pip install xmltodict
import json
import os
import subprocess
from typing import Optional, Tuple

import xmltodict


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


def _stdout_get_json(stdout: str) -> str:
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


def _shell_run(shell_cmd: str) -> Tuple[int, str]:
    """
    运行shell命令,并获得命令的退出状态和打印信息
    """
    # print(f"running: {shell_cmd}")
    status, stdout = subprocess.getstatusoutput(shell_cmd)
    # print(f"exit code: {status}")
    # print(f"stdout: \n {stdout}")
    return status, stdout


class VDNAGen:

    def __init__(self):
        self.__host = None
        self.__user = None
        self.__passwd = None

    def host_set(self, host: str) -> None:
        self.__host = host

    def user_set(self, user: str) -> None:
        self.__user = user

    def passwd_set(self, passwd: str) -> None:
        self.__passwd = passwd

    def __config_check(self) -> bool:
        if None in [self.__host, self.__user, self.__passwd]:
            print("VDNAGen host user passwd 参数未设置")
            return False
        return True

    @staticmethod
    def far_create(movie_path: str,
                   far_dir: Optional[str] = None,
                   far_name: Optional[str] = None,
                   rebuild: bool = False) -> str:
        """
        调用VDNAGen生成far文件
        :param movie_path: 视频路径
        :param far_dir: 保存的far文件路径
        :param far_name: far文件名称, 当far_dir不存在时, far_name为绝对路径
        :param rebuild:
        :return:
        """
        movie_path = os.path.abspath(movie_path)
        # far_dir 为None far_name 为None 生成基因路径为 ${movie_path}.far
        # far_dir 为None far_name 不为None
        res = {"mode": "far_create"}
        movie_path_basename = os.path.basename(movie_path)
        movie_path_basename, _ = os.path.splitext(movie_path_basename)

        if far_name is None:
            far_name = movie_path_basename + ".far"

        if far_dir is None:
            if os.path.isabs(far_name):
                far_dir = os.path.dirname(far_name)
            else:
                far_dir = os.path.dirname(movie_path)

        far_dir = os.path.abspath(far_dir)
        os.makedirs(far_dir, exist_ok=True)
        far_path = os.path.join(far_dir, far_name)
        res["far_path"] = far_path
        if os.path.isfile(far_path) and not rebuild:
            res["rebuild"] = 0
        else:
            res["rebuild"] = 1
            shell_cmd = f"VDNAGen -o '{far_path}' '{movie_path}'"
            res["shell_cmd"] = shell_cmd
            status, stdout = _shell_run(shell_cmd)
            res["exit_code"] = status
            xml_str = _stdout_get_xml(stdout, "receipt")
            if len(xml_str) != 0:
                res["stdout2json"] = xmltodict.parse(xml_str)
            else:
                res["stdout"] = stdout
        return json.dumps(res, indent=2, ensure_ascii=False)

    def far_db_rename(self, meta_uid: str, dna_name: str) -> str:
        template_xml = """<?xml version="1.0" encoding="UTF-8"?>
<Media_Meta>
    <Actions>
        <Action>Update Metadata</Action>
    </Actions>
    <VobileRefID>%s</VobileRefID>
    <Title>%s</Title>
</Media_Meta>
        """
        res = {"mode": "far_db_rename"}
        if self.__config_check():
            rename_xml = template_xml % (meta_uid, dna_name)
            with open("rename_dna.xml", mode="w", encoding="utf-8") as f:
                f.write(rename_xml)
            shell_cmd = f"VDNAGen -s {self.__host} -u {self.__user} -p {self.__passwd} -m rename_dna.xml"
            res["shell_cmd"] = shell_cmd
            status, stdout = _shell_run(shell_cmd)
            res["exit_code"] = status
            xml_str = _stdout_get_xml(stdout, "receipt")
            if len(xml_str) != 0:
                res["stdout2json"] = xmltodict.parse(xml_str)
            else:
                res["stdout"] = stdout
            os.remove("rename_dna.xml")
        pass

    def far_db_remove(self, meta_uid: str) -> str:
        """
        删除VDDB数据库中meta_uid基因
        :param meta_uid:
        :return:
        """
        template_xml = """<?xml version="1.0" encoding="UTF-8"?>
<Media_Meta>
    <Actions>
        <Action>Delete VDNA</Action>
    </Actions>
    <VobileRefID>%s</VobileRefID>
</Media_Meta>
        """.strip()

        res = {"mode": "far_db_remove"}
        if self.__config_check():
            delete_xml = template_xml % meta_uid
            with open("delete_dna.xml", mode="w", encoding="utf-8") as f:
                f.write(delete_xml)
            shell_cmd = f"VDNAGen -s {self.__host} -u {self.__user} -p {self.__passwd} -m delete_dna.xml"
            res["shell_cmd"] = shell_cmd
            status, stdout = _shell_run(shell_cmd)
            res["exit_code"] = status
            xml_str = _stdout_get_xml(stdout, "receipt")
            if len(xml_str) != 0:
                res["stdout2json"] = xmltodict.parse(xml_str)
            else:
                res["stdout"] = stdout
            os.remove("delete_dna.xml")
        return json.dumps(res, indent=2, ensure_ascii=False)

    def far_db_insert(self, far_path: str) -> str:
        """
        在VDDB中插入基因
        :param far_path:
        :return:
        """
        res = {"mode": "far_db_insert"}
        if self.__config_check():
            shell_cmd = f"VDNAGen -s {self.__host} -u {self.__user} -p {self.__passwd} \"{far_path}\""
            res["shell_cmd"] = shell_cmd
            status, stdout = _shell_run(shell_cmd)
            res["exit_code"] = status
            xml_str = _stdout_get_xml(stdout, "receipt")
            if len(xml_str) != 0:
                res["stdout2json"] = xmltodict.parse(xml_str)
            else:
                res["stdout"] = stdout
        return json.dumps(res, indent=2, ensure_ascii=False)

    def far_db_match(self, far_path: str) -> str:
        """
        VDDB基因匹配
        :param far_path:
        :return:
        """
        res = {"mode": "far_db_match"}
        shell_cmd = f"python2 FarQuerySampleCode.py -s {self.__host} -u {self.__user} -p {self.__passwd} -i \"{far_path}\""
        res["shell_cmd"] = shell_cmd
        status, stdout = _shell_run(shell_cmd)
        res["exit_code"] = status
        json_str = _stdout_get_json(stdout)
        if len(json_str) != 0:
            res["stdout2json"] = json.loads(json_str)
        else:
            res["stdout"] = stdout
        return json.dumps(res, indent=2, ensure_ascii=False)
