#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
这个代码主要是用来从es取聚合数据、转换为pandas中的Dataframe

"""
import copy
import time
import numpy as np
import pandas as pd

tmp = []  # 利用tmp是为了记录每一个桶的名字
result = []  # 利用result是为了记录所有结果


def dict_loop(agg_data, dest, nested):
    """
    遍历字典中元素
    :param agg_data:
    :param dest:
    :param nested:
    :return:
    """
    still_has_bucket = False
    bucket_name = None
    num_dest = len(dest)
    for i in range(len(nested)):
        if nested[i] in agg_data.keys():
            bucket_name = nested[i]
            still_has_bucket = True
            break
    if (num_dest >= 1) and (still_has_bucket is False):
        # 提取数据, 其实可以和下面的分支合并
        if 'key_as_string' in agg_data.keys():
            time_local = time.localtime(agg_data['key'] / 1000)
            key_list = [
                time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time_local)]  # 如果是按照时间分桶，取时间标识，否则提取分桶字段值
        else:
            key_list = [agg_data['key']]

        for dest_value in dest:
            if dest_value not in agg_data.keys():
                key_list.append(np.nan)
            elif dest_value == "doc_count":
                key_list.append(agg_data[dest_value])
            else:
                if 'values' in agg_data[dest_value].keys():
                    if agg_data[dest_value]['values'][0]['value'] == 'NaN':
                        key_list.append(np.nan)
                    else:
                        key_list.append(
                            agg_data[dest_value]['values'][0]['value'])
                else:
                    if agg_data[dest_value]['value'] == 'NaN':
                        key_list.append(np.nan)
                    else:
                        key_list.append(agg_data[dest_value]['value'])
        # 利用tmp是为了纪录每一个桶的名字
        tmp.extend(key_list)
        aa = copy.deepcopy(tmp)
        result.append(aa)
        for i in range(num_dest + 1):
            tmp.pop()

    elif (bucket_name is not None) and (isinstance(agg_data[bucket_name]['buckets'], list)) and (
            'key' in agg_data.keys()):
        # 调用包含时间字典的列表，其实这段代码没有用处，只要是list都要进行递归
        tmp.extend([agg_data['key']])
        list_loop(agg_data[bucket_name]['buckets'], dest, nested)
        tmp.pop()

    elif (bucket_name is not None) and (isinstance(agg_data[bucket_name]['buckets'], list)):
        # 处理第一层
        list_loop(agg_data[bucket_name]['buckets'], dest, nested)

    elif bucket_name is None:
        pass


def list_loop(list_data, dest, nested):
    """
    遍历列表中元素
    :param list_data:
    :param dest:
    :param nested:
    :return:
    """
    for i in range(len(list_data)):
        if isinstance(list_data[i], dict):
            dict_loop(list_data[i], dest, nested)


def get_list_result(agg_data, dest, nested):
    # 获取list形式的结果
    """
    [[u'\u6731\u9038\u541b', u'2016-07-27T00:00:00.000+08:00', 5.0],
    [u'\u6731\u9038\u541b', u'2016-07-28T00:00:00.000+08:00', 14.0],
    [u'\u6731\u9038\u541b', u'2016-07-29T00:00:00.000+08:00', 79.0]]
    """
    dict_loop(agg_data, dest, nested)
    result_copy = copy.deepcopy(result)
    return result_copy


def clear_global_variable():
    """
    清除中间结果
    :return:
    """
    for i in range(len(result)):
        result.pop()


def get_es_to_pandas_data(data_all, nested, dest_list):
    """
    这段代码用来将es的聚合结果转换为DataFrame结构
    :param data_all:    es搜索得到的结果
    :param nested:      by分桶字段
    :param dest_list:   待提取目标字段
    :return:
    """
    # 获取每个job_id的搜索结果
    if data_all.get("aggregations"):
        agg_data = data_all['aggregations']
    else:
        agg_data = None
    # 获取列表形式的结果
    list_result = get_list_result(agg_data, dest_list, nested)

    # 清空无用变量空间
    clear_global_variable()

    # 生成列名, 转换成DataFrame结构
    nested.extend(dest_list)
    data = pd.DataFrame(list_result, columns=nested)

    return data


if __name__ == "__main__":
    example_data = {u'status': 3, u'hits': {u'hits': [], u'total': 106316, u'max_score': 0.0},
                    u'_shards': {u'successful': 282, u'failed': 0, u'total': 282}, u'timed_out': False, u'took': 479,
                    u'id': u'fac9fc08-fdc1-11e8-9515-0242ac120005', u'aggregations': {u'3': {u'buckets': [
            {u'sum_login': {u'value': 817.0}, u'2': {u'buckets': [
                {u'sum_login': {u'value': 12.0}, u'key_as_string': u'2016-07-26T00:00:00.000+08:00',
                 u'key': 1469462400000, u'doc_count': 12},
                {u'sum_login': {u'value': 5.0}, u'doc_count': 5, u'predict_2': {u'value': 12.0}, u'key': 1469548800000,
                 u'key_as_string': u'2016-07-27T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 14.0}, u'doc_count': 14, u'predict_2': {u'value': 12.0},
                 u'key': 1469635200000, u'key_as_string': u'2016-07-28T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 79.0}, u'doc_count': 80, u'predict_2': {u'value': 12.0},
                 u'key': 1469721600000, u'key_as_string': u'2016-07-29T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 12.0}, u'key': 1469808000000,
                 u'key_as_string': u'2016-07-30T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 12.0}, u'key': 1469894400000,
                 u'key_as_string': u'2016-07-31T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 22.0}, u'doc_count': 22, u'predict_2': {u'value': 12.0},
                 u'key': 1469980800000, u'key_as_string': u'2016-08-01T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 45.0}, u'doc_count': 45, u'predict_2': {u'value': 12.0},
                 u'key': 1470067200000, u'key_as_string': u'2016-08-02T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 27.0}, u'doc_count': 28, u'predict_2': {u'value': 12.0},
                 u'key': 1470153600000, u'key_as_string': u'2016-08-03T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 10.0}, u'doc_count': 10, u'predict_2': {u'value': 12.0},
                 u'key': 1470240000000, u'key_as_string': u'2016-08-04T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 10.0}, u'doc_count': 10, u'predict_2': {u'value': 12.0},
                 u'key': 1470326400000, u'key_as_string': u'2016-08-05T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 5.0}, u'key': 1470412800000,
                 u'key_as_string': u'2016-08-06T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 14.0}, u'key': 1470499200000,
                 u'key_as_string': u'2016-08-07T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 79.0}, u'key': 1470585600000,
                 u'key_as_string': u'2016-08-08T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 3.0}, u'doc_count': 4, u'predict_2': {u'value': 0.0}, u'key': 1470672000000,
                 u'key_as_string': u'2016-08-09T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1470758400000,
                 u'key_as_string': u'2016-08-10T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 22.0}, u'key': 1470844800000,
                 u'key_as_string': u'2016-08-11T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 19.0}, u'doc_count': 19, u'predict_2': {u'value': 45.0},
                 u'key': 1470931200000, u'key_as_string': u'2016-08-12T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 27.0}, u'key': 1471017600000,
                 u'key_as_string': u'2016-08-13T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 10.0}, u'key': 1471104000000,
                 u'key_as_string': u'2016-08-14T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 33.0}, u'doc_count': 33, u'predict_2': {u'value': 10.0},
                 u'key': 1471190400000, u'key_as_string': u'2016-08-15T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 26.0}, u'doc_count': 26, u'predict_2': {u'value': 0.0}, u'key': 1471276800000,
                 u'key_as_string': u'2016-08-16T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 31.0}, u'doc_count': 31, u'predict_2': {u'value': 0.0}, u'key': 1471363200000,
                 u'key_as_string': u'2016-08-17T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1471449600000,
                 u'key_as_string': u'2016-08-18T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 3.0}, u'key': 1471536000000,
                 u'key_as_string': u'2016-08-19T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1471622400000,
                 u'key_as_string': u'2016-08-20T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1471708800000,
                 u'key_as_string': u'2016-08-21T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 36.0}, u'doc_count': 38, u'predict_2': {u'value': 19.0},
                 u'key': 1471795200000, u'key_as_string': u'2016-08-22T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 26.0}, u'doc_count': 26, u'predict_2': {u'value': 0.0}, u'key': 1471881600000,
                 u'key_as_string': u'2016-08-23T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 30.0}, u'doc_count': 30, u'predict_2': {u'value': 0.0}, u'key': 1471968000000,
                 u'key_as_string': u'2016-08-24T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 28.0}, u'doc_count': 28, u'predict_2': {u'value': 33.0},
                 u'key': 1472054400000, u'key_as_string': u'2016-08-25T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 11.0}, u'doc_count': 11, u'predict_2': {u'value': 26.0},
                 u'key': 1472140800000, u'key_as_string': u'2016-08-26T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 31.0}, u'key': 1472227200000,
                 u'key_as_string': u'2016-08-27T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1472313600000,
                 u'key_as_string': u'2016-08-28T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 13.0}, u'doc_count': 13, u'predict_2': {u'value': 0.0}, u'key': 1472400000000,
                 u'key_as_string': u'2016-08-29T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 18.0}, u'doc_count': 18, u'predict_2': {u'value': 0.0}, u'key': 1472486400000,
                 u'key_as_string': u'2016-08-30T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 20.0}, u'doc_count': 20, u'predict_2': {u'value': 0.0}, u'key': 1472572800000,
                 u'key_as_string': u'2016-08-31T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 36.0}, u'key': 1472659200000,
                 u'key_as_string': u'2016-09-01T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 3.0}, u'doc_count': 3, u'predict_2': {u'value': 26.0}, u'key': 1472745600000,
                 u'key_as_string': u'2016-09-02T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 30.0}, u'key': 1472832000000,
                 u'key_as_string': u'2016-09-03T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 28.0}, u'key': 1472918400000,
                 u'key_as_string': u'2016-09-04T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 9.0}, u'doc_count': 12, u'predict_2': {u'value': 11.0}, u'key': 1473004800000,
                 u'key_as_string': u'2016-09-05T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 8.0}, u'doc_count': 8, u'predict_2': {u'value': 0.0}, u'key': 1473091200000,
                 u'key_as_string': u'2016-09-06T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 4.0}, u'doc_count': 4, u'predict_2': {u'value': 0.0}, u'key': 1473177600000,
                 u'key_as_string': u'2016-09-07T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 26.0}, u'doc_count': 27, u'predict_2': {u'value': 13.0},
                 u'key': 1473264000000, u'key_as_string': u'2016-09-08T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 38.0}, u'doc_count': 39, u'predict_2': {u'value': 18.0},
                 u'key': 1473350400000, u'key_as_string': u'2016-09-09T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 62.0}, u'doc_count': 62, u'predict_2': {u'value': 20.0},
                 u'key': 1473436800000, u'key_as_string': u'2016-09-10T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1473523200000,
                 u'key_as_string': u'2016-09-11T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 16.0}, u'doc_count': 17, u'predict_2': {u'value': 3.0}, u'key': 1473609600000,
                 u'key_as_string': u'2016-09-12T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 35.0}, u'doc_count': 36, u'predict_2': {u'value': 0.0}, u'key': 1473696000000,
                 u'key_as_string': u'2016-09-13T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1473782400000,
                 u'key_as_string': u'2016-09-14T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 9.0}, u'key': 1473868800000,
                 u'key_as_string': u'2016-09-15T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 8.0}, u'key': 1473955200000,
                 u'key_as_string': u'2016-09-16T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 4.0}, u'key': 1474041600000,
                 u'key_as_string': u'2016-09-17T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 26.0}, u'key': 1474128000000,
                 u'key_as_string': u'2016-09-18T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 38.0}, u'key': 1474214400000,
                 u'key_as_string': u'2016-09-19T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 28.0}, u'doc_count': 29, u'predict_2': {u'value': 62.0},
                 u'key': 1474300800000, u'key_as_string': u'2016-09-20T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 39.0}, u'doc_count': 42, u'predict_2': {u'value': 0.0}, u'key': 1474387200000,
                 u'key_as_string': u'2016-09-21T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 24.0}, u'doc_count': 24, u'predict_2': {u'value': 16.0},
                 u'key': 1474473600000, u'key_as_string': u'2016-09-22T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 7.0}, u'doc_count': 11, u'predict_2': {u'value': 35.0}, u'key': 1474560000000,
                 u'key_as_string': u'2016-09-23T00:00:00.000+08:00'},
                {u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1474646400000,
                 u'key_as_string': u'2016-09-24T00:00:00.000+08:00'},
                {u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1474732800000,
                 u'key_as_string': u'2016-09-25T00:00:00.000+08:00'}]}, u'key': u'张三',
             u'doc_count': 837},
            {u'sum_login': {u'value': 817.0}, u'2': {u'buckets': [
                {u'sum_login': {u'value': 12.0}, u'key_as_string': u'2016-07-26T00:00:00.000+08:00',
                 u'key': 1469462400000, u'doc_count': 12},
                {u'sum_login': {u'value': 5.0}, u'doc_count': 5, u'predict_2': {u'value': 12.0}, u'key': 1469548800000,
                 u'key_as_string': u'2016-07-27T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 14.0}, u'doc_count': 14, u'predict_2': {u'value': 12.0},
                 u'key': 1469635200000, u'key_as_string': u'2016-07-28T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 79.0}, u'doc_count': 80, u'predict_2': {u'value': 12.0},
                 u'key': 1469721600000, u'key_as_string': u'2016-07-29T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 12.0}, u'key': 1469808000000,
                 u'key_as_string': u'2016-07-30T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 12.0}, u'key': 1469894400000,
                 u'key_as_string': u'2016-07-31T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 22.0}, u'doc_count': 22, u'predict_2': {u'value': 12.0},
                 u'key': 1469980800000, u'key_as_string': u'2016-08-01T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 45.0}, u'doc_count': 45, u'predict_2': {u'value': 12.0},
                 u'key': 1470067200000, u'key_as_string': u'2016-08-02T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 27.0}, u'doc_count': 28, u'predict_2': {u'value': 12.0},
                 u'key': 1470153600000, u'key_as_string': u'2016-08-03T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 10.0}, u'doc_count': 10, u'predict_2': {u'value': 12.0},
                 u'key': 1470240000000, u'key_as_string': u'2016-08-04T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 10.0}, u'doc_count': 10, u'predict_2': {u'value': 12.0},
                 u'key': 1470326400000, u'key_as_string': u'2016-08-05T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 5.0}, u'key': 1470412800000,
                 u'key_as_string': u'2016-08-06T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 14.0}, u'key': 1470499200000,
                 u'key_as_string': u'2016-08-07T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 79.0}, u'key': 1470585600000,
                 u'key_as_string': u'2016-08-08T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 3.0}, u'doc_count': 4, u'predict_2': {u'value': 0.0}, u'key': 1470672000000,
                 u'key_as_string': u'2016-08-09T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1470758400000,
                 u'key_as_string': u'2016-08-10T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 22.0}, u'key': 1470844800000,
                 u'key_as_string': u'2016-08-11T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 19.0}, u'doc_count': 19, u'predict_2': {u'value': 45.0},
                 u'key': 1470931200000, u'key_as_string': u'2016-08-12T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 27.0}, u'key': 1471017600000,
                 u'key_as_string': u'2016-08-13T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 10.0}, u'key': 1471104000000,
                 u'key_as_string': u'2016-08-14T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 33.0}, u'doc_count': 33, u'predict_2': {u'value': 10.0},
                 u'key': 1471190400000, u'key_as_string': u'2016-08-15T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 26.0}, u'doc_count': 26, u'predict_2': {u'value': 0.0}, u'key': 1471276800000,
                 u'key_as_string': u'2016-08-16T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 31.0}, u'doc_count': 31, u'predict_2': {u'value': 0.0}, u'key': 1471363200000,
                 u'key_as_string': u'2016-08-17T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1471449600000,
                 u'key_as_string': u'2016-08-18T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 3.0}, u'key': 1471536000000,
                 u'key_as_string': u'2016-08-19T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1471622400000,
                 u'key_as_string': u'2016-08-20T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1471708800000,
                 u'key_as_string': u'2016-08-21T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 36.0}, u'doc_count': 38, u'predict_2': {u'value': 19.0},
                 u'key': 1471795200000, u'key_as_string': u'2016-08-22T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 26.0}, u'doc_count': 26, u'predict_2': {u'value': 0.0}, u'key': 1471881600000,
                 u'key_as_string': u'2016-08-23T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 30.0}, u'doc_count': 30, u'predict_2': {u'value': 0.0}, u'key': 1471968000000,
                 u'key_as_string': u'2016-08-24T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 28.0}, u'doc_count': 28, u'predict_2': {u'value': 33.0},
                 u'key': 1472054400000, u'key_as_string': u'2016-08-25T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 11.0}, u'doc_count': 11, u'predict_2': {u'value': 26.0},
                 u'key': 1472140800000, u'key_as_string': u'2016-08-26T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 31.0}, u'key': 1472227200000,
                 u'key_as_string': u'2016-08-27T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1472313600000,
                 u'key_as_string': u'2016-08-28T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 13.0}, u'doc_count': 13, u'predict_2': {u'value': 0.0}, u'key': 1472400000000,
                 u'key_as_string': u'2016-08-29T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 18.0}, u'doc_count': 18, u'predict_2': {u'value': 0.0}, u'key': 1472486400000,
                 u'key_as_string': u'2016-08-30T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 20.0}, u'doc_count': 20, u'predict_2': {u'value': 0.0}, u'key': 1472572800000,
                 u'key_as_string': u'2016-08-31T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 36.0}, u'key': 1472659200000,
                 u'key_as_string': u'2016-09-01T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 3.0}, u'doc_count': 3, u'predict_2': {u'value': 26.0}, u'key': 1472745600000,
                 u'key_as_string': u'2016-09-02T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 30.0}, u'key': 1472832000000,
                 u'key_as_string': u'2016-09-03T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 28.0}, u'key': 1472918400000,
                 u'key_as_string': u'2016-09-04T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 9.0}, u'doc_count': 12, u'predict_2': {u'value': 11.0}, u'key': 1473004800000,
                 u'key_as_string': u'2016-09-05T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 8.0}, u'doc_count': 8, u'predict_2': {u'value': 0.0}, u'key': 1473091200000,
                 u'key_as_string': u'2016-09-06T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 4.0}, u'doc_count': 4, u'predict_2': {u'value': 0.0}, u'key': 1473177600000,
                 u'key_as_string': u'2016-09-07T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 26.0}, u'doc_count': 27, u'predict_2': {u'value': 13.0},
                 u'key': 1473264000000, u'key_as_string': u'2016-09-08T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 38.0}, u'doc_count': 39, u'predict_2': {u'value': 18.0},
                 u'key': 1473350400000, u'key_as_string': u'2016-09-09T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 62.0}, u'doc_count': 62, u'predict_2': {u'value': 20.0},
                 u'key': 1473436800000, u'key_as_string': u'2016-09-10T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1473523200000,
                 u'key_as_string': u'2016-09-11T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 16.0}, u'doc_count': 17, u'predict_2': {u'value': 3.0}, u'key': 1473609600000,
                 u'key_as_string': u'2016-09-12T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 35.0}, u'doc_count': 36, u'predict_2': {u'value': 0.0}, u'key': 1473696000000,
                 u'key_as_string': u'2016-09-13T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1473782400000,
                 u'key_as_string': u'2016-09-14T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 9.0}, u'key': 1473868800000,
                 u'key_as_string': u'2016-09-15T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 8.0}, u'key': 1473955200000,
                 u'key_as_string': u'2016-09-16T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 4.0}, u'key': 1474041600000,
                 u'key_as_string': u'2016-09-17T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 26.0}, u'key': 1474128000000,
                 u'key_as_string': u'2016-09-18T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 0.0}, u'doc_count': 0, u'predict_2': {u'value': 38.0}, u'key': 1474214400000,
                 u'key_as_string': u'2016-09-19T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 28.0}, u'doc_count': 29, u'predict_2': {u'value': 62.0},
                 u'key': 1474300800000, u'key_as_string': u'2016-09-20T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 39.0}, u'doc_count': 42, u'predict_2': {u'value': 0.0}, u'key': 1474387200000,
                 u'key_as_string': u'2016-09-21T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 24.0}, u'doc_count': 24, u'predict_2': {u'value': 16.0},
                 u'key': 1474473600000, u'key_as_string': u'2016-09-22T00:00:00.000+08:00'},
                {u'sum_login': {u'value': 7.0}, u'doc_count': 11, u'predict_2': {u'value': 35.0}, u'key': 1474560000000,
                 u'key_as_string': u'2016-09-23T00:00:00.000+08:00'},
                {u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1474646400000,
                 u'key_as_string': u'2016-09-24T00:00:00.000+08:00'},
                {u'doc_count': 0, u'predict_2': {u'value': 0.0}, u'key': 1474732800000,
                 u'key_as_string': u'2016-09-25T00:00:00.000+08:00'}]}, u'key': u'李四',
             u'doc_count': 837}, ], u'doc_count_error_upper_bound': -1,
            u'sum_other_doc_count': 97917}}}

    dest_fields = ["sum_login", "predict_2"]
    nested_fields = ["3", "2"]
    res = get_es_to_pandas_data(example_data, nested_fields, dest_fields)
    print(res)
