# -*- coding: utf-8 -*-

import MySQLdb
import requests
import numpy as np
import time

def spend_time_wrapper(func, func_str, *func_args, **func_kwargs):
    start_time = time.time()
    result = func(*func_args, **func_kwargs)
    print('time taken to ' + func_str + ': %s s' % (time.time() - start_time))
    return result

def connect_to_db(host, port, user, passwd, db):
    db = MySQLdb.connect(host = host, 
                         port = port,
                         user = user, 
                         passwd = passwd, 
                         db = db, 
                         use_unicode = True, 
                         charset = 'utf8')
    print('connection to database established')
    return db

def create_cursor(db):
    cursor = db.cursor()
    return cursor

def get_http_response(get_vector_url, sentence):
    quest = {'q': sentence}
    response = requests.get(get_vector_url, params = quest)
    return response.text
    
def get_k_sentences(cursor, num_sen, get_sen_sql):
    cursor.execute(get_sen_sql)
    sentences_t = cursor.fetchmany(num_sen)
    return [sentence_t[0] for sentence_t in sentences_t]

def parse_vec_str(vec_str):
    vec_list = vec_str[1: -1].split(', ')
    return map(float, vec_list)

def get_vectors(get_vector_url, sentences, num_sen, vec_len):
    vectors = np.zeros((num_sen, vec_len))
    for i, sentence in enumerate(sentences):
        vector = parse_vec_str(get_http_response(get_vector_url, sentence))
        for j in range(vec_len):
            vectors[i][j] = vector[j]
    
    return vectors

def get_vectors_partial(get_vec_url, sentences, num_seg, vec_len, index):
    len_sen = len(sentences)
    interval = len_sen // num_seg
    if index < num_seg - 1:
        end = interval
    else:
        end = len_sen - index * interval
    
    vectors = np.zeros((end, vec_len))
    for i in range(end):
        sentence = sentences[index * interval + i]
        vector = parse_vec_str(get_http_response(get_vec_url, sentence))
        for j in range(vec_len):
            vectors[i][j] = vector[j]
    
    return vectors

def wrapped_get_vec_partial(get_vec_url, sentences, num_seg, vec_len, index):
    return spend_time_wrapper(get_vectors_partial, 'compute vectors for each process', get_vec_url, sentences, num_seg, vec_len, index)
        
def get_sentences_db(host, port, user, passwd, db, num_sen, get_sen_sql):
    db = connect_to_db(host, port, user, passwd, db)
    cursor = create_cursor(db)
    sentences = spend_time_wrapper(get_k_sentences, 'retrieve sentences', cursor, num_sen, get_sen_sql)
    return sentences
            
def get_vectors_db(host, port, user, passwd, db, num_sen, get_sen_sql, get_vec_url, vec_len, start_time, get_sen = False):
    db = connect_to_db(host, port, user, passwd, db)
    cursor = create_cursor(db)
    time_con_db = time.time()
    print('time taken to connect to database: %s s' % (time_con_db - start_time))
    sentences = spend_time_wrapper(get_k_sentences, 'retrieve sentences', cursor, num_sen, get_sen_sql)
    vectors = spend_time_wrapper(get_vectors, 'compute vectors', get_vec_url, sentences, num_sen, vec_len)
    cursor.close()
    db.close()
    if get_sen:
        return vectors, sentences
    else:
        return vectors