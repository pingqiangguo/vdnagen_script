#!/usr/bin/env python
##########################################
#
# MediaWiseFarQuery.py
#
# Copyright (c) 2005, 2019 Vobile, Inc. All Rights Reserved.
#
##########################################
"""
this script demostrates how to query against MediaWise server
using generated FAR file, involving two steps: uploading to server,
fetching result from server
"""

# PQG: command format
# python FarQuerySampleCode.py   -s mediawise.vobile.net -u username -p password  -i file.far
import sys
import os
from xml.dom import minidom
from xml.dom.minidom import parseString
import time
import urllib
import urllib2
import mimetypes
from optparse import OptionParser
import json

reload(sys)
sys.setdefaultencoding('utf8')

SERVER_SUCCESS = "<ErrorCode>0</ErrorCode>"
TASK_ID_START = "<TaskID>"
TASK_ID_END = "</TaskID>"

VERSION = "2.6.3"

STATUS_VERBOSE = True
ERROR_SUCCESS = 0
ERROR_INVALID_PARAMENT = 1
ERROR_INTERNAL = 2


def print_pretty_xml(node, space_num):
    if not node:
        return
    if node.ELEMENT_NODE != node.nodeType and node.DOCUMENT_NODE != node.nodeType:
        return
    if node.childNodes.length == 0:
        print " " * space_num,
        print ("<%s></%s>" % (node.nodeName, node.nodeName)).encode("utf8")
    elif node.childNodes.length == 1 and node.childNodes[0].nodeType == node.TEXT_NODE:
        print " " * space_num,
        print ("<%s>%s</%s>" % (node.nodeName, node.firstChild.nodeValue.strip(" \t\r\n"), node.nodeName)).encode(
            "utf8")
    else:
        print " " * space_num,
        print ("<%s>" % node.nodeName).encode("utf8")
        for child_node in node.childNodes:
            print_pretty_xml(child_node, space_num + 4)
        print " " * space_num,
        print ("</%s>" % node.nodeName).encode("utf8")


def far_query_exit(error_code, error_message):
    print "<Result>"
    print "    <Head>"
    print "        <ErrorCode>%d</ErrorCode>" % error_code
    print "        <Message>%s</Message>" % error_message
    print "    </Head>"
    print "    <Body>"
    print "    </Body>"
    print "</Result>"
    sys.exit(error_code)


def prompt(str):
    global STATUS_VERBOSE
    if STATUS_VERBOSE:
        print str
        sys.stdout.flush()


def main():
    global STATUS_VERBOSE
    server, user, passwd, far, interval, retry, format, verbose = parse_options()
    STATUS_VERBOSE = verbose
    try:
        whole_flow(server, user, passwd, far, interval, retry, format)
    except Exception, e:
        sys.exit(e)


def parse_options():
    try:
        parser = OptionParser(version=VERSION)
        parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True,
                          help="don't output the status info to stdout",
                          metavar="QUIET")
        parser.add_option("-s", "--server", dest="server",
                          help="specify SERVER of MediaWise system",
                          metavar="SERVER")
        parser.add_option("-u", "--user", dest="user",
                          help="specify USER of MediaWise system",
                          metavar="USER")
        parser.add_option("-p", "--passwd", dest="passwd",
                          help="specify PASSWD of MediaWise system",
                          metavar="PASSWD")
        parser.add_option("-i", "--input", dest="far",
                          help="specify FAR file to query",
                          metavar="FAR")
        parser.add_option("-f", "--format", dest="format",
                          help='specify the output format of query result, only "vobile" or "crr"' \
                               'is available, default to "vobile"',
                          default="vobile",
                          metavar="FORMAT")
        (options, args) = parser.parse_args()
        options.retry = sys.maxint
        options.interval = 1
    except Exception, e:
        sys.exit(ERROR_INVALID_PARAMENT)

    if not options.server \
            or not options.user \
            or not options.passwd \
            or not options.far:
        far_query_exit(ERROR_INVALID_PARAMENT, "Invalid parameter:server, user, password, far_file");

    infar = options.far
    if not os.path.isfile(infar):
        far_query_exit(ERROR_INVALID_PARAMENT, "Specified FAR file does not exist");

    if options.format not in ("vobile", "crr"):
        far_query_exit(ERROR_INVALID_PARAMENT, 'Farmat option:only "vobile" and "crr" is valid format');

    return (options.server, options.user, \
            options.passwd, infar, \
            options.interval, \
            options.retry, options.format, options.verbose)


def whole_flow(server, user, passwd, far, interval, retry, format):
    prompt("    upload to server...")
    try:
        taskID = upload2server(server, user, passwd, far)
    except SystemExit, e:
        sys.exit(e)
    except Exception, e:
        far_query_exit(ERROR_INTERNAL, "Upload to server failed")

    prompt("    upload to server...                   done.")

    prompt("    fetch result...")
    try:
        fetch_result(server, user, passwd, taskID, interval, retry, format)
    except SystemExit, e:
        sys.exit(e)
    except Exception, e:
        far_query_exit(ERROR_INTERNAL, "Fetch result from server failed")
    prompt("    fetch result...                       done.")


def upload2server(host, user, password, farfile):
    """ upload VideoDNA file to the MediaWise Uploading Server
    options:
    host, user and password are quite clear.
    farfile: the path of previous generated temporary VideoDNA file
    """
    try:
        url = "http://%s/service/mediawise" % host
        post_param = [("action", "submit"),
                      ("username", user),
                      ("password", password)]
        post_file = [
            ("dna", farfile, file(farfile, 'rb').read()), ]  # file handle will be automatic close outside this function
        content_type, post_data = encode_multipart_formdata(post_param, post_file)
        header = {'Content-Type': content_type, 'Content-Length': str(len(post_data))}
        req = urllib2.Request(url, post_data, header)
        response = ""
        response = urllib2.urlopen(req).read()
        if response.find(SERVER_SUCCESS) == -1:
            raise Exception(response)
        else:
            tagStart = response.find(TASK_ID_START)
        if tagStart == -1:
            raise Exception(response)
        tagStart += len(TASK_ID_START)
        tagEnd = response.find(TASK_ID_END)
        if tagEnd == -1:
            raise Exception(response)
        return response[tagStart: tagEnd]
    except Exception, e:
        if response.find("<Result>") != -1:
            print e
            sys.exit(ERROR_INTERNAL)
        else:
            far_query_exit(ERROR_INTERNAL, "Upload to server error:%s" % e)


def encode_multipart_formdata(fields, files):
    BOUNDARY = '----------ThIs_Is_tHe_bouNdaRY_$'
    CRLF = '\r\n'
    L = []
    for (key, value) in fields:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"' % key)
        L.append('')
        L.append(value)
    for (key, filename, value) in files:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
        L.append('Content-Type: %s' % get_content_type(filename))
        L.append('')
        L.append(value)
        L.append('--' + BOUNDARY + '--')
        L.append('')
        body = CRLF.join(L)
        content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
    return content_type, body


def get_content_type(filename):
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'


def fetch_result(host, user, password, taskID, interval, retry, format):
    """ fetch query result from the MediaWise Query Web Service
    calculate the service URL.
    URL pattern:

    http://host/service/mediawise?
    action=check_status&
    username=USER&
    password=PASS&
    type=site_assetID&
    id=ASSET_ID
    format=FORMAT
    result sample:
    <Result>
    <Head>
    <ErrorCode>0</ErrorCode>
    <ErrorMessage>Succeeded</ErrorMessage>
    </Head>
    <Body>
    <ResultCount>1</ResultCount>
    <Query>
    <QueryLog>
    <TimeStamp>2009-09-05T07:15:41</TimeStamp>
    <File>f978af78-99eb-11de-8b8b-00e04c953ed6</File>
    <Status>2</Status>
    <Error></Error>
    </QueryLog>
    </Query>
    </Body>
    </Result>

    """

    url_param = urllib.urlencode({"action": "check_status",
                                  "username": user,
                                  "password": password,
                                  "type": "task_id",
                                  "format": format,
                                  "outputformat": "json",
                                  "id": taskID})
    dest_url = "http://%s/service/mediawise?%s" % (host, url_param)

    while (True):
        try:
            # read the entire message received from Web service
            result = urllib.urlopen(dest_url).read()
            # extract error code
            # tag look like: <ErrorCode>0</ErrorCode>
            result = json.loads(result)
            head = result[u"Head"]
            if head[u"ErrorCode"] == -1:
                raise Exception(result)

            # parse query status from tag <Status>
            # tag look like: <Status>2</Status>
            # status = 1: success and having match result
            # status = 2: processing
            # status = 0: success but no match result
            # status = -1: error
            body = result[u"Body"]
            first_query = body[u"Query"][0]
            status = first_query[u"QueryLog"][u"Status"]

            # if the query status is in progress, it means the VideoDNA
            # had been transferred to MediaWise Query Service, however it
            # still is being queried, so the identification result is not
            # available right now
            if status != 2:
                # print result
                result_str = json.dumps(result, indent=2, ensure_ascii=False)
                print result_str
                break
        except Exception, e:
            far_query_exit(ERROR_INTERNAL, "Failed to fetch result:%s" % e)

        if (retry < 1):  # if retry timeout
            far_query_exit(ERROR_INTERNAL, "Fetry timeout")
        time.sleep(interval)
        retry -= 1


def fetch_result_exit(error_code, error_message):
    head_info = ""
    if error_code != ERROR_SUCCESS:
        head_info = error_message,
    else:
        head_info = "success"

    print "<Result>"
    print "    <Head>"
    print "        <ErrorCode>%d</ErrorCode>" % error_code
    print "        <Message>%s</Message>" % head_info
    print "    </Head>"
    print "    <Body>"
    if error_code == ERROR_SUCCESS:
        for line in error_message.split("\n"):
            line = line.strip()
            if line == "":
                continue
            print "        <Item>%s</Item>" % line
    print "    </Body>"
    print "</Result>"
    sys.exit(error_code)


if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        pass
    except Exception, ex:
        fetch_result_exit(ERROR_INTERNAL, ex)
