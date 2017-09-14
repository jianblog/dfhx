# coding = utf-8
"""
Date: 2017-09-11
desc: 命令行执行，查询mysql、elk库检索指定账号在某时段内的访问记录
"""
import mysql.connector
from elasticsearch import Elasticsearch
import elasticsearch.helpers

import pandas as pd
import numpy as np
from collections import defaultdict
import datetime
import os, re, sys


def queryUser(client, index, columns, field, value, time_from, time_to):
    """
    方法：从UserTrack记录中，根据指定手机号查询指定时段的登录记录，获得相应session_id
    返回：登录时间、手机号、session_id的集合（可能1条或多条）
    """

    query_range = {
        "_source": columns,
        "query": {
            "bool": {
                "must": [
                    {"range": {
                        "localtime": {
                            "from": time_from,
                            "to": time_to,
                            "format": "yyyy-MM-dd:HH:mm:ss",
                            "time_zone": "+08:00"
                        }
                    }},
                    {"match": {
                        field: value
                    }}
                ]
            }
        }
    }

    ret = elasticsearch.helpers.scan(client, query_range, index=index, scroll='1m')
    ret_generator = (r['_source'] for r in ret)
    df = pd.DataFrame(ret_generator)
    return df


def inputTime():
    patt_time = re.compile("\d{4}-\d{2}-\d{2}:\d{2}:\d{2}")

    from_time, to_time = '', ''

    while True:
        from_time = input("请输入查询起始时间，格式yyyy-mm-dd:HH:MM   忽略请回车（默认查询最近三天）")
        if from_time:
            if re.match(patt_time, from_time):
                from_time = from_time + ":00"
                while True:
                    to_time = input("请输入查询终止时间，忽略请回车（默认当前时间):")
                    if to_time:
                        if re.match(patt_time, to_time):
                            to_time = to_time + ":59"
                            break
                        else:
                            print("时间格式错误，请重新输入")
                    else:
                        now = datetime.datetime.now()
                        to_time = now.strftime("%Y-%m-%d:%H:%M:59")
                        break
                if to_time:
                    break
            else:
                print("时间格式错误，请重新输入")

        else:
            # 默认检索当前时间之前三天内
            now = datetime.datetime.now()
            to_time = now.strftime("%Y-%m-%d:%H:%M:59")
            day_range = datetime.timedelta(days=3)
            from_time = (now - day_range).strftime("%Y-%m-%d:%H:%M:00")
            break
    return (from_time, to_time)


def inputPhone():
    patt_phone = re.compile("\d{11}")
    while True:
        phone = input("请输入注册手机号(直接回车退出):")
        if phone:
            if re.match(patt_phone, phone):
                break
            else:
                print("手机号码格式有误")
        else:
            sys.exit()
    return phone

if __name__ == '__main__':
    if len(sys.argv) == 5:    # 参数用于在django中直接调用脚本
        IS_detail, phone, from_time, to_time = sys.argv[ 1: ]    # IS_detail 为0时结果仅包含时间，url
    else:
        IS_detail = "simple"
        phone = inputPhone()
        from_time, to_time = inputTime()


    es = Elasticsearch(['ali.dev:9200'])
    columns_track = [ "agent",
                      "clientip",
                      "localtime",
                      "session_id",
                      "user_account",
                      "user_realname",
                      "invited_by_uid",
                      "apply_time" ]
    df_byaccount = queryUser(es, 'user_track_*', columns_track, 'user_account', phone, from_time, to_time)
    df_byaccount[ 'invited_by_uid' ] = df_byaccount[ 'invited_by_uid' ].astype('int')    # 类型转换
    df_byaccount.drop_duplicates(['session_id'])    # 删除用户有多个相同session_id重复记录    # 清理
    df_byaccount.sort_values('localtime', ascending=True, inplace=True)    # 排序
    df_byaccount.reset_index(drop=True, inplace=True)    # 索引重建

    sid_hash = {}
    for sid in range(len(df_byaccount)):
        session_id = df_byaccount.loc[sid, 'session_id']
        if session_id in sid_hash:
            continue
        else:
            sid_hash[session_id] = 1
        print("-----------------------------------------------------------------------------------------------------")
        print(">>>>>>","SESSION_ID:" + session_id,"IP:" + df_byaccount.loc[sid,'clientip'] )
        print(">>>>>>", "账号:" + str(phone),
                        "姓名:" + df_byaccount.loc[sid, 'user_realname'],
                        "invited_by_uid:" + str(df_byaccount.loc[sid, 'invited_by_uid']),
                        "登录时间:" + df_byaccount.loc[ sid, 'localtime' ])

        df_bysession = queryUser(es, 'user_track_*', columns_track, 'session_id', session_id, from_time, to_time)
        df_bysession['invited_by_uid'] = df_bysession['invited_by_uid'].astype('int')
        df_bysession.drop_duplicates(['user_account'])
        df_bysession.sort_values('localtime', inplace=True)
        df_bysession.reset_index(drop=True, inplace=True)
        other_track = []
        if len(df_bysession) > 1:
            # 多个用户相同session, 其他用户登录点信息
            for i in range(len(df_bysession)):
                if df_bysession.loc[i, 'user_account'] != phone:
                    rlogin = [
                               df_bysession.loc[i, 'localtime'],
                               df_bysession.loc[i, 'user_account'],
                               df_bysession.loc[i, 'user_realname'],
                               df_bysession.loc[i, 'invited_by_uid'] ]
                    other_track.append(rlogin)
        if other_track:
            print(">>>session发现其他用户登录记录：")
            for row in other_track:
                print(row)

        # 根据session_id 检索时间段内所有访问记录
        columns_ac = [ "localtime", "clientip", "url", "request", "request_body", "agent", "status" ]
        df_track = queryUser(es, 'nginx_jcjact_*', columns_ac, 'session_id', session_id, from_time, to_time)
        df_track.sort_values('localtime', ascending=True, inplace=True)
        df_track.reset_index(dro
        p=True, inplace=True)

        print(">>>>>>","session 起始时间:", df_track.head(n=1)[['localtime']].iloc[0,0],
                       "session 终止时间:",  df_track.tail(n=1)[['localtime']].iloc[0,0])
        print(">>>>>> 访问记录：")

        agent_identify = None    # 标识每次输出的agent是否与前次相同
        for i in range(len(df_track)):
            if df_track.loc[i, 'agent'] == agent_identify:
                agent_display = ''
            else:
                agent_display = df_track.loc[i, 'agent']
            if IS_detail != "simple":
                print(":>>", df_track.loc[i, 'localtime'], ",",
                  df_track.loc[i, 'url'], ",",
                  df_track.loc[i, 'request'], ",",
                  df_track.loc[ i, 'request_body' ], ",",
                  df_track.loc[ i, 'status' ], ",",
                  agent_display)
            else:
                print(":>>", df_track.loc[ i, 'localtime' ], ",",
                      df_track.loc[ i, 'url' ], ",",
                      df_track.loc[ i, 'status' ], ",",
                      agent_display)
        #x = input("回车继续......")