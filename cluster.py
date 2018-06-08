# -*- coding: utf-8 -*-
from sparsehc_dm import sparsehc_dm
import numpy as np
#from scipy.cluster.hierarchy import dendrogram
#from matplotlib import pyplot as plt
#from con_sql import get_vectors_db, get_vec_sen_db
from con_sql import get_sentences_db, wrapped_get_vec_partial, spend_time_wrapper
import resource 
import time
import argparse
import codecs
from pathos.multiprocessing import ProcessingPool as Pool

def print_use(start_time):
    print('total time taken: %s seconds' % (time.time() - start_time))
    print('memory usage: %f GB' % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024. / 1024.))

def get_distance(i, j, vectors):
    return np.linalg.norm(vectors[i] - vectors[j])

def print_cl_progress(l_history, sentences, rec_sen = True):
    len_sen = len(sentences)
    assert len(l_history) == len_sen - 1
    
    with open('results.txt', 'w') as results_file:
        for i, entry in enumerate(l_history):
            results_file.write('%d %d %d %.5f\n' %(i, entry[0], entry[1], entry[2]))
    if rec_sen:
        with codecs.open('sentences.txt', 'w', encoding = 'utf-8') as sentences_file:
            for sentence in sentences:
                sentences_file.write('%s\n' %(sentence))
                
def cluster(size, sort_ram, vectors):
    distances = sparsehc_dm.InMatrix(sort_ram)
    push_sort_start = time.time()
    for i in range(size - 1):
        for j in range(i + 1, size):
            distances.push(i, j, get_distance(i, j, vectors))
    
    push_sort_end = time.time()
    print('time taken to push and sort distance matrix: %s s' % (push_sort_end - push_sort_start))
    l_history = spend_time_wrapper(sparsehc_dm.linkage, 'cluster', distances, 'complete')
#    print (l_history[: index_to_print])
    return l_history

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', action = 'store', nargs = '?', default = '127.0.0.1', type = str,
                        help = '数据库的IP地址，默认为localhost. IP address of database; default to localhost')
    parser.add_argument('--port', action = 'store', nargs = '?', default = 3306, type = int,
                        help = '数据库端口号，默认为3306. port of database; default to 3306')
    parser.add_argument('--user', action = 'store', nargs = '?', default = 'root', type = str,
                        help = '数据库的用户名，默认为root. username for database; default to root')
    parser.add_argument('--passwd', action = 'store', nargs = '?', default = 'password', type = str,
                        help = '数据库用户名对应的密码，默认为password. passowrd for database; default to password')
    parser.add_argument('--db', action = 'store', nargs = '?', default = 'database', type = str,
                        help = '数据库的名称，默认为database. name of the database; default to database')
    parser.add_argument('--size', action = 'store', nargs = '?', default = 100, type = int,
                        help = '数据点的总数量，默认为100. total number of points to be clustered; default to 100')
    parser.add_argument('--get_sen_sql', action = 'store', nargs = '?', 
                        default = u'SELECT sentence FROM table'.encode('utf-8'),
                        type = str,
                        help = '从数据库读出所有句子的SQL命令，默认为从table中读出句子，需要在句子前后加入\n英文状态下的引号. sql command to retrieve all sentences from a table; default to selecting all sentences from "table"')    
    parser.add_argument('--get_vec_url', action = 'store', nargs = '?', 
                        default = 'http:127.0.0.1/vector',
                        type = str,
                        help = '访问获取给定语句对应向量的网址，默认为http://127.0.0.1/vector. the website which will return the vector embedding form of a given sentence upon requests')
    parser.add_argument('--vec_len', action = 'store', nargs = '?', default = 256, type = int,
                        help = '一个句子对应的向量长度，默认为256. the length of the vector; default to 256')
    parser.add_argument('--sort_ram', action = 'store', nargs = '?', default = int(0.5 * 1024 * 1024 * 1024), type = int,
                        help = '给STXXL安排的内存空间大小，单位是byte，默认为0.5GB（可运行1w个点，5GB可用于运\n行10w个点）. ram space for STXXL sorting; unit is Byte and default to 0.5GB (able to run a 10k points clustering, and 5GB will suffice for clustering 100k points)')
    parser.add_argument('--from_db', action = 'store_true',
                        help = '加入这个参数将不会把任何句子记录进本地的文本文件，在之后运行vis_results时也\n需要加入此参数. adding it will not write sentences into local txt file, but one would have to run vis_results with the argument as well')
    parser.add_argument('--num_proc', action = 'store', nargs = '?', default = 1, type = int,
                        help = '获取句子对应向量的并行线程数量，默认为1. the number of parallel procedures used when visiting the website to obtain the vectors of given sentences; default to 1')
    parser.add_argument('--num_seg', action = 'store', nargs = '?', default = 1, type = int,
                        help = '表示会将获取所有句子对应向量的任务分割成多少份任务，不需要与并行线程数相同，默认为1. the number of segementations of the task of sending requests to the website for vector representations; may not be the same as the number of procedures; default to 1')
    name_space = parser.parse_args() 
    return name_space       
    
def main():
    start_time = time.time()
    name_space = parse_arguments()
#    vectors, sentences = get_vectors_db(name_space.host, name_space.user, name_space.passwd, name_space.db, name_space.size, name_space.get_sen_sql, name_space.get_vec_url, name_space.vec_len, start_time, get_sen = True)
    sentences = get_sentences_db(name_space.host, name_space.port, name_space.user, name_space.passwd, name_space.db, name_space.size, name_space.get_sen_sql)
    pool = Pool(nodes = name_space.num_proc)
    multi_proc_start = time.time()
    results = pool.map(lambda index: wrapped_get_vec_partial(name_space.get_vec_url, sentences, name_space.num_seg, name_space.vec_len, index), range(name_space.num_seg))
    vectors = np.concatenate(results, axis = 0)
    print('total time taken to complete computing vectors: %s s' % (time.time() - multi_proc_start))
    print(vectors[: 10])
    results = cluster(name_space.size, name_space.sort_ram, vectors)
    print_cl_progress(results, sentences, rec_sen = not name_space.from_db)
    print_use(start_time)
    return results

if __name__ == '__main__':
    main()