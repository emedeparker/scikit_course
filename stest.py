

from code.modules.csv import read_csv

import os
import sys
import sys
import datetime
import time
from pprint import pprint
from pymongo import MongoClient
import re

from tasks import query_async, get_core_list, get_additional_list


# Queries import


def run_broker():
    os.system("gnome-terminal -e 'celery -A tasks worker -c 5 --loglevel=info'")

if __name__ == '__main__':

    run_broker()

    t0 = time.time()

    db = client = MongoClient('localhost', 27017).observatory

    category_name = "mortgage/a+b+c"

    path = os.path.abspath('resources/domains/')

    keywords_list_aux = []
    if category_name != "global":
        keywords_list_aux = read_csv(path + '/' + category_name + '.csv')
    keywords_list = []
    my_list = []

    if not category_name.lower() == 'global' and keywords_list_aux == []:
        print('\nCSV not found.\nObservatory will end now.')
        sys.exit()


    for element in keywords_list_aux:
        my_list.append(element.lower())

    for x in my_list:
        keywords_list.append(re.compile('.*{}.*'.format(x)))

    '''
    Code for regex_expressions with literal strings
    for y in kwywords_list_literal:
        keywords_list.append(re.compile('.* {} .*'.format(x)))
    '''

    if not os.path.isdir('results'):
        os.makedirs('results')

    path = 'results/' + str(datetime.datetime.now().strftime('%y-%m-%d'))
    if not os.path.isdir(path):
        os.makedirs(path)

    path_key = path + '/' + category_name + '/'
    path_core =path_key + 'core/'
    path_additional = path_key + 'additional/'
    if not os.path.isdir(path_key):
        os.makedirs(path_key)
    if not os.path.isdir(path_core):
        os.makedirs(path_core)
        os.makedirs(path_core + 'CSV/')
    if not os.path.isdir(path_additional):   
        os.makedirs(path_additional)
        os.makedirs(path_additional + 'CSV/')


    #list_core_queries = [Query22]
    #list_core_queries = [Query02, Query05A, Query05B, Query06, Query08, Query09A, Query09B, Query09C, Query10A, Query10C, Query11A, Query11B, Query15, Query19, Query20A, Query20B]
    #list_core_queries = ['Query02', 'Query05A', 'Query05B', 'Query06', 'Query08', 'Query09A', 'Query09B', 'Query09C', 'Query10A', 'Query10C', 'Query11A', 'Query11B', 'Query15', 'Query19', 'Query20A', 'Query20B']
    # Faltan la 01, 19, 20A y 20B.
    #list_additional_queries = [Query03A, Query03B, Query04A, Query04B, Query07A, Query07B, Query07C, Query12A, Query13, Query14, Query16, Query17]
    # Falta la 0, 18, 22

    '''
    for pos in range(0, core_queries):  

        print(type(path_core), type(category_name), type(keywords_list), type(pos))
        now_query = query_async.delay(path_core, category_name, keywords_list, pos)

    '''

    list_core_queries = get_core_list()
    list_additional_queries = get_additional_list()
    
    for query in list_core_queries:
        print(query)
        res = query_async.delay(query, path_core, category_name, my_list)

    while not query_async.AsyncResult(res.id).ready():
        time.sleep(5)

    seconds = round(time.time() -  t0)
    print('\n\tTime taken: {} minutes approx.'.format(round(seconds/60)))

    for query in list_additional_queries:
        print(query)
        res = query_async.delay(query, path_additional, category_name, my_list)

    while not query_async.AsyncResult(res.id).ready():
        time.sleep(5)

    seconds = round(time.time() -  t0)
    print('\n\tTime taken: {} minutes approx.'.format(round(seconds/60)))