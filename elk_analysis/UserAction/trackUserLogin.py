import mysql.connector
from elasticsearch import Elasticsearch
import elasticsearch.helpers

import pandas as pd
import numpy as np
from collections import defaultdict
import datetime
import os, re


def getLastTime(client, index):
    """
    方法：查询指定索引记录的最新时间
    返回：timestamp
    """
    query_aggs = {
        "size": 0,
        "aggs": {
            "most_recent": {
                "max": {"field": "localtime", "format": "yyyy-MM-dd HH:mm:ss"}
            }
        }
    }
    try:
        ret = client.search(index=index, body=query_aggs)
        return int(ret['aggregations']['most_recent']['value']) / 1000
    except Exception as e:
        print("Query ELK failed for aggs max localtime")
        return None


def queryRecent(client, index, time_from, time_to):
    """
    方法：查询指定时间范围内的记录
    time_from, time_to: datetime.datetime类型，
    返回：DataFrame类型
    可快速按如下生成：
        cur_dt = datetime.datetime.now()   #当前时间
        cust_dt = datetime.datetime.strptime("2017-07-07 09:30:00", "%Y-%m-%d %H:%M:%S")   #自定义时间串

    tim_to = timePos.strftime("%Y-%m-%d %H:%M:%S")
    tim_from = (timePos - datetime.timedelta(seconds=diff)).strftime("%Y-%m-%d %H:%M:%S")
    """

    # 需要返回的字段
    fields = ["localtime", "clientip", "session_id", "request_body", "url", "agent"]

    query_range = {
        "_source": fields,
        "query": {
            "bool": {
                "must": [
                    {"range": {
                        "localtime": {
                            "from": time_from,
                            "to": time_to,
                            "format": "yyyy-MM-dd HH:mm:ss",
                            "time_zone": "+08:00"
                        }
                    }},
                    {"exists": {"field": "session_id"}}
                ]
            }
        }
    }

    ret = elasticsearch.helpers.scan(client, query_range, index=index, scroll='1m')
    ret_generator = (r['_source'] for r in ret)
    df = pd.DataFrame(ret_generator)
    return df


def insertUserTrack(client, elk_index, elk_type, df_usr):
    """
    方法：将用户行为记录更新到elk中

    """
    actions = []

    for idx in df_usr.index:
        dic = df_usr.loc[idx].to_dict()
        dic['_index'] = elk_index
        dic['_type'] = elk_type
        actions.append(dic)
    elasticsearch.helpers.bulk(client, actions)


def writeToFile(filename, df):
    """
    方法：将DataFrame记录以字典行写入文件
    """
    with open(filename, 'a') as f:
        for idx in df.index:
            f.writelines(str(df.loc[idx].to_dict()).replace("'", '"') + "\n")


def filter_field(df, field_name, term_list):
    """
    方法：对字段进行完整匹配过滤
    参数：field_name: 字段名称
         term_str:明确的字符串
    返回：DataFrame类型
    """
    df_term = pd.DataFrame()
    for tm in term_list:
        df_term = df_term.append(df[df[field_name] == tm])
    return df_term


def match_field(df, field_name, patt):
    """
    方法：字段正则匹配
    返回：Series类型，为匹配的账号或None
    """

    match_list = []
    for record in df[field_name]:
        ret = patt.search(record)
        if ret:
            match_list.append(ret.groups()[0])
        else:
            match_list.append(None)
    return pd.Series(match_list)


def multiMatch_field(df, field_name, patt_list):
    """
    方法：使用正则列表进行逐个匹配测试
    返回：Series对象，与df索引相应对的匹配记录，便于同df关联
    """

    match_list = []
    # for idx,record in df[field_name].items():
    for record in df[field_name]:
        flag = 0
        for patt in patt_list:
            ret = patt.search(record)
            if ret:
                flag = 1
                match_list.append(ret.groups()[0])
                break  # 匹配到一种即可
        if not flag:
            match_list.append(None)
    # 组合Series，由字典创建dataframe, 时间字符串转为时间类型
    return pd.Series(match_list, df.index)
    '''
    df_user = pd.DataFrame({'localtime':df['localtime'], 'clientip': df['clientip'], 'session_id':df['session_id'],
                            'agent':df['agent'], 'user_account': match_list},
                           columns = ['localtime','clientip','session_id','user_account','agent'])
    return df_user            
    '''


def tim2str(t):
    """
    方法: datetime类型转为固定格式字符串，用于从数据库取出时间转换
    """
    try:
        return t.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    except Exception:
        return None


if __name__ == "__main__":
    # 0. 初始化
    # mysql连接
    cn = mysql.connector.connect(user='dfhxp2p', password='powerp0p', host='localhost', port='3307',
                                 database='prod_p2p')

    # elk连接
    es = Elasticsearch(['ali.dev:9200'])

    # 用户登录行为正则列表
    patt1 = re.compile("useraccount=(.*?)&")
    # 匹配样式2： 多行，useraccount换行跟Content-Length，再换行为账号
    patt2 = re.compile('name="useraccount"\s+Content-Length: \d+\s+(\d{11})\s+--')
    # \r\n用\s+匹配
    # 匹配样式3： 多行，useraccout换行跟账号
    patt3 = re.compile('name="useraccount"\s+(\d{11})\s+--')

    patt_login = [patt1, patt2, patt3]

    # 1.连接数据库获取最新用户列表
    sql = "select user_id, user_account,user_realname, invited_by_uid,apply_time from rb_user where user_id > 1"
    db_usr = pd.read_sql_query(sql, cn)
    cn.close()
    db_usr['user_id'] = db_usr['user_id'].astype(np.object)
    db_usr['invited_by_uid'] = db_usr['invited_by_uid'].astype(np.object)
    db_usr['apply_time'] = db_usr['apply_time'].apply(tim2str)  # dataframe对象的 datetime格式转字符串

    # 2.检索时间，确定本次执行查询的时间段范围

    # 上次查询的终点时间，加1秒为本次查询的起点
    ts_last_query_end = getLastTime(es, 'user_track_*')
    if not ts_last_query_end:
        exit
    query_begin = datetime.datetime.strftime(datetime.datetime.fromtimestamp(ts_last_query_end + 1),
                                             "%Y-%m-%d %H:%M:%S")

    # 当前记录的最新时间，减去1分钟为本次查询终点
    ts_cur_record_end = getLastTime(es, 'nginx_jcj_*')
    if not ts_cur_record_end:
        exit
    query_end = datetime.datetime.strftime(datetime.datetime.fromtimestamp(ts_cur_record_end - 60), "%Y-%m-%d %H:%M:%S")

    print(query_begin, query_end)
    df = queryRecent(es, 'nginx_jcjact_*', query_begin, query_end)
    if len(df) > 0:
        df_login = filter_field(df, 'url', ['/dybuat/user/login.do','/user/login.do'])

        # 通过正则规则匹配账号,并生成账号与访问信息关联的dataframe
        s_usr = multiMatch_field(df_login, 'request_body', patt_login)
        df_usr = pd.DataFrame({"localtime": df_login['localtime'],
                               "clientip": df_login['clientip'],
                               "session_id": df_login['session_id'],
                               "agent": df_login['agent'],
                               "user_account": s_usr})
        df_usr.sort_values('localtime', inplace=True)  # 排序
        df_usr.reset_index(inplace=True)  # 重置索引
        df_usr.drop('index', axis=1, inplace=True)  # 删除重置索引后生成的index列
        df_usr.dropna(axis=0, inplace=True)  # 账号异常未匹配,删除空值行

        # 账号访问记录同数据库用户表联合生成最终记录
        df_user = df_usr.merge(db_usr, on='user_account', how='left')
        # print(df_user[['localtime','user_realname', 'user_account', 'invited_by_uid', 'apply_time']])
        abs_path = os.path.split(os.path.realpath(__file__))[0]
        writeToFile(os.path.join(abs_path, "userLogin.log"), df_user)
    else:
        print("no record")
