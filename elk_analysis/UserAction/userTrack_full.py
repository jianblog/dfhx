#coding = utf-8

import mysql.connector
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import pandas as pd
import datetime
from collections import defaultdict
import re
import json
import pprint, pickle
import os


def getLastTime(client, index):
    """
    方法：查询指定索引记录的最新时间戳
    返回：datetime.datetime格式
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
        return datetime.datetime.fromtimestamp(int(ret['aggregations']['most_recent']['value']) / 1000)
    except Exception as e:
        print("Query ELK failed for aggs max localtime")
        return None

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

def tim2str(t):
    """
    方法: datetime类型转为固定格式字符串，用于从数据库取出时间转换
    """
    try:
        return t.strftime("%Y-%m-%dT%H:%M:%S+08:00")
    except Exception:
        return None

def writeToFile(filename, df):
    """
    方法：将DataFrame记录以字典行写入文件
    """
    with open(filename, 'a') as f:
        for idx in df.index:
            f.writelines(str(df.loc[idx].to_dict()).replace("'", '"') + "\n")

def queryDB():
    """
    方法:检索本地数据库，获得当前用户清单
    """
    cn = mysql.connector.connect(host='localhost',
                                 port='3307',
                                 user='dfhxp2p',
                                 password='powerp0p',
                                 database='prod_p2p')
    sql = "select user_id, user_account, user_realname, invited_by_uid, apply_time from rb_user where user_id > 1 and invited_by_uid>=0 and user_account > 1"

    db_usr = pd.read_sql_query(sql, cn, coerce_float=False)
    cn.close()

    db_usr[['user_id']] = db_usr[['user_id']].astype(int)
    db_usr[['user_account']] = db_usr[['user_account']].astype(int)
    db_usr[['invited_by_uid']] = db_usr[['invited_by_uid']].astype(int)
    db_usr[ 'apply_time' ] = db_usr[ 'apply_time' ].apply(tim2str)
    return db_usr


def queryDSL(match_field, match_value, time_from, time_to):
    '''
    根据给定的时间，抓取该点之前若干秒内产生的记录
    timePos: datetime.datetime类型，
    可快速按如下生成：
        cur_dt = datetime.datetime.now()   #当前时间
        cust_dt = datetime.datetime.strptime("2017-07-07 09:30:00", "%Y-%m-%d %H:%M:%S")   #自定义时间串

    tim_to = timePos.strftime("%Y-%m-%d %H:%M:%S")
    tim_from = (timePos - datetime.timedelta(seconds=diff)).strftime("%Y-%m-%d %H:%M:%S")
    '''

    fields = ["localtime", "clientip", "url", "request_body", "session_id", "agent"]
    # 数据库中会取出user_id, user_account, user_realname, invited_by_uid, apply_time这些字段
    # 最终合并后保留 localtime， clientip，session_id, user_id, user_account, user_realname, invited_by_uid, apply_time

    es = Elasticsearch(['ali.dev:9200'])
    query_range = {
      "_source": fields,
         "query": {
           "bool": {
               "must":[
                   {"match": {match_field:match_value}},
                   {"range": {
                    "localtime": {
                      "from": time_from,
                      "to":   time_to,
                      "format": "yyyy-MM-dd HH:mm:ss",
                      "time_zone": "+08:00"
                    }
                  }},
                   {"exists": {"field": "session_id"}}
               ]
            }
         }
    }

    ret = elasticsearch.helpers.scan(es, query_range, index='nginx_jcj_*', scroll='1m')
    ret_generator = ( r['_source'] for r in ret)
    df = pd.DataFrame(ret_generator)
    return df


if __name__ == '__main__':
    es = Elasticsearch(['ali.dev:9200'])
    # 检索起止时刻
    query_begin_time = datetime.datetime.strftime(getLastTime(es, "userbehavior_*") + datetime.timedelta(seconds=1), "%Y-%m-%d %H:%M:%S")
    query_end_time = datetime.datetime.strftime(getLastTime(es, "nginx_jcj_*") - datetime.timedelta(seconds=60), "%Y-%m-%d %H:%M:%S")
    print(query_begin_time, query_end_time)

    # 初次执行时，手动指定如下时间窗口。之后配置定时任务间隔执行
    #query_begin_time = '2018-05-29 11:00:00'
    #query_end_time = '2018-05-29 14:00:59'

    abs_path = os.path.dirname(__file__)
    with open(os.path.join(abs_path, "track_patt.json")) as f:
        map_bau = json.load(f)

    for rule in map_bau['rules']:
        for field_k, field_v in rule['filter'].items():    # 筛选字段，字段值
            df_query = queryDSL(field_k, field_v, query_begin_time, query_end_time)     # 第一阶段： 按字段及匹配值查询记录
            df_query[rule['field']] = False    # 为当前查询结果增加需要匹配提取的列，初始值为False
            for match_field in rule['pattern'].keys():    # 每种需要匹配的字段
                match_patts = rule['pattern'][match_field]

                patt_re = []
                for p in match_patts:    # 将正则字符串预先编译
                    patt_re.append(re.compile(p))

                for idx in range(len(df_query)):    # 第二阶段：遍历结果集，使用正则匹配提取用户数据
                    flag = 0
                    for p in patt_re:    # 默认这些模式只能有一个匹配结果
                        result =  p.search(df_query.loc[idx, match_field])
                        if result:
                            df_query.loc[idx, rule['field']] = result.groups()[0]    # 将匹配得到的字段账号，user_id写入预定字段

                # 检索匹配后，剔除未找到手机的匹配的记录. 重排索引
                df_nomatch = df_query[df_query[rule['field']] == False ]
                writeToFile(os.path.join(abs_path, "nomatch.log"), df_nomatch)

                df_query = df_query[df_query[rule['field']] != False]
                # 与以往记录合并
                if os.path.exists(os.path.join(abs_path, rule['field'] + ".dump")):
                    if os.path.getsize(os.path.join(abs_path, rule['field'] + ".dump")):
                        with open(os.path.join(abs_path, rule['field'] + ".dump"), "rb") as f:
                            df_history = pickle.load(f)
                        if  len(df_history) > 0:
                            df_query = df_query.append(df_history, ignore_index=True)

                df_query = df_query.drop_duplicates()    # 去重
                df_query.reset_index(drop=True, inplace=True)    # 重排索引
                df_query[[rule['field']]] = df_query[[rule['field']]].astype(int)    # 匹配字段需要与数据库中对应字段比较，转为int

                # 第三阶段：结果与数据库进行merge比对，定位用户行为
                db_usr = queryDB()
                df_merge = pd.merge(df_query, db_usr, how='left', on=rule['field'])
                ## 未匹配的记录本地持久化保存
                df_merge_nomatch = df_merge[df_merge['invited_by_uid'].isnull()]   # 硬编码字段
                df_query_nomatch = df_query[df_query[rule['field']].isin(df_merge_nomatch[rule['field']])]
                with open(os.path.join(abs_path, rule['field'] + ".dump"), "wb") as f:
                    pickle.dump(df_query_nomatch, f)

                ## 匹配的记录写入日志文件
                df_match = df_merge.dropna()
                if len(df_match) > 0:
                    df_ba_match = df_match[['localtime', 'clientip', 'session_id', 'agent', 'user_id', 'user_account','user_realname', 'invited_by_uid', 'apply_time']]
                    df_ba_match['user_id'] = df_ba_match['user_id'].astype(int)    # 为转换类型，这里硬性写入实际字段名
                    df_ba_match['invited_by_uid'] = df_ba_match['invited_by_uid'].astype(int)
                    df_ba_match.sort_values('localtime', ascending=True, inplace=True)    # 排序
                    df_ba_match.reset_index(drop=True, inplace=True)    # 索引重排
                    writeToFile(os.path.join(abs_path, "behaviorTracks.log"), df_ba_match)

                    # 另一分支: 直接写入elk
                    action_bulks = []
                    es_beta = Elasticsearch(['ali.dev:9200'])
                    for id in df_ba_match.index:
                        yyyymm = df_ba_match.loc[id, "localtime"][:7].replace("-","")
                        dat = {"_index": "userbehavior_" + yyyymm, "_type": "login",
                                "localtime": df_ba_match.loc[id, "localtime"],
                                "clientip": df_ba_match.loc[id, "clientip"],
                                "session_id": df_ba_match.loc[id, "session_id"],
                                "agent": df_ba_match.loc[id, "agent"],
                                "user_id": str(int(df_ba_match.loc[id, "user_id"])),
                                "user_account": str(int(df_ba_match.loc[id, "user_account"])),
                                "user_realname": df_ba_match.loc[id, "user_realname"],
                                "invited_by_uid": str(df_ba_match.loc[id, "invited_by_uid"]),
                                "apply_time": df_ba_match.loc[id, "apply_time"]}
                        action_bulks.append(dat)

                    elasticsearch.helpers.bulk(es_beta, action_bulks)
