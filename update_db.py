# -*- coding: utf-8 -*-

from con_sql import spend_time_wrapper, connect_to_db, create_cursor
import argparse
import codecs
import re

def cl_from_txt(clusters_file):
    counter = 0
    clusters = {}
    with codecs.open(clusters_file, 'r', encoding = 'utf-8') as f:
        for line in f:
            if counter % 2 == 0:
                cluster = line[: -1].split(', ')
                clusters[counter // 2] = cluster
            counter += 1
    print(clusters[0][0])
    print(clusters[1][0])
    return clusters

def update_from_cl(clusters, host, port, user, passwd, db, table_name):
    db = connect_to_db(host, port, user, passwd, db)
    db.autocommit(True)
    cursor = create_cursor(db)
    counter = 1
    error_sentences = []
    for key in clusters:
        for sentence in clusters[key]:
            print(key)
            print(sentence)
            if re.match(r'^[\w\s]+$', sentence, re.UNICODE) and not re.search(r'_', sentence, re.UNICODE):
                cursor.execute(unicode('UPDATE ' + table_name, 'utf-8') + (u' SET cluster_id = %d WHERE iat = "%s"'.encode('utf-8') % (counter, sentence)))
            else:
                print('regular expression error')
                error_sentences.append(sentence)
        counter += 1
    cursor.close()
    db.close()
    
def update(host, port, user, passwd, db, clusters_file, table_name):
    clusters = cl_from_txt(clusters_file)
    update_from_cl(clusters, host, port, user, passwd, db, table_name)

# IMPORTANT: the number of lines of --sentences_file must be exactly equal to --len_sen
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
    parser.add_argument('--clusters_file', action = 'store', nargs = '?', default = 'clusters.txt', type = str,
                        help = '储存已分组的句子信息的文本文件名称，默认为clusters.txt. name of the txt file to store sentences; default to clusters.txt')
    parser.add_argument('--table_name', action = 'store', nargs = '?', default = u'table'.encode('utf-8'), type = str,
                        help = '要更新的表的名字，默认为table. name of the table to update; default to "table"')
    name_space = parser.parse_args()
    return name_space
    
def main():
    name_space = parse_arguments()
    spend_time_wrapper(update, 'update database', name_space.host, name_space.port, name_space.user, name_space.passwd, name_space.db, name_space.clusters_file, name_space.table_name)

if __name__ == '__main__':
    main()