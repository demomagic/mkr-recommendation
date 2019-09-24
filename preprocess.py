import os
import redis
import argparse
import pandas as pd

def text_create(full_path):
    file = open(full_path, 'w')
    file.close()

def read_index():
    file = './data/' + DATASET + '/'
    print('reading item index to entity id file: ' + file + '...')

    if not os.path.exists(file + 'c2code_index.txt'):
        text_create(file + 'c2code_index.txt')

    if not os.path.exists(file + 'user_index.txt'):
        text_create(file + 'user_index.txt')

    if not os.path.exists(file + 'relation_index.txt'):
        text_create(file + 'relation_index.txt')

    if not os.path.exists(file + 'entity_index.txt'):
        text_create(file + 'entity_index.txt')

    '''
    #code索引
    '''
    with open(file + 'c2code_index.txt', 'r', encoding='utf-8') as f:
        for line in f.readlines():
            array = line.strip().split("\t")
            if array[1] not in c2code_index:
                c2code_index[array[1]] = int(array[0])
    if len(c2code_index) == 0:
        index = 0
    else :
        index = max(c2code_index.values()) + 1
    for i in code_name['c2code']:
        if i not in c2code_index:
            c2code_index[i] = index
            index += 1
    with open(file + 'c2code_index.txt', 'w', encoding='utf-8') as f:
        for line in c2code_index:
            f.write('%d\t%s\n' % (c2code_index[line],line))

    '''
    #用户索引
    '''
    with open(file + 'user_index.txt', 'r', encoding='utf-8') as f:
        for line in f.readlines():
            array = line.strip().split("\t")
            if array[1] not in user_index:
                user_index[array[1]] = int(array[0])
    if len(user_index) == 0:
        index = 0
    else:
        index = max(user_index.values()) + 1
    for i in user_movies['userid']:
        if i not in user_index:
            user_index[i] = index
            index += 1
    with open(file + 'user_index.txt', 'w', encoding='utf-8') as f:
        for line in user_index:
            f.write('%d\t%s\n' % (user_index[line], line))

    '''
    #关系索引
    '''
    with open(file + 'relation_index.txt', 'r', encoding='utf-8') as f:
        for line in f.readlines():
            array = line.strip().split("\t")
            if array[1] not in relation_index:
                relation_index[array[1]] = int(array[0])
    if len(relation_index) == 0:
        index = 0
    else :
        index = max(relation_index.values()) + 1
    for i in code_relation['relation']:
        if i not in relation_index:
            relation_index[i] = index
            index += 1
    with open(file + 'relation_index.txt', 'w', encoding='utf-8') as f:
        for line in relation_index:
            f.write('%d\t%s\n' % (relation_index[line],line))

    '''
    #实体索引
    '''
    with open(file + 'entity_index.txt', 'r', encoding='utf-8') as f:
        for line in f.readlines():
            array = line.strip().split("\t")
            if array[1] not in entity_index:
                entity_index[array[1]] = int(array[0])
    if len(entity_index) == 0:
        index = 0
    else :
        index = max(entity_index.values()) + 1
    with open(file + 'c2code_index.txt', 'r', encoding='utf-8') as f:
        for line in f.readlines():
            array = line.strip().split("\t")
            if array[1] not in entity_index:
                entity_index[array[1]] = index
                index += 1
    for i in code_relation['p']:
        if i not in entity_index:
            entity_index[i] = index
            index += 1
    with open(file + 'entity_index.txt', 'w', encoding='utf-8') as f:
        for line in entity_index:
            f.write('%d\t%s\n' % (entity_index[line],line))

    print('number of users: %d' % len(user_index))
    print('number of items: %d' % len(c2code_index))
    print('number of entities (containing items): %d' % len(entity_index))
    print('number of relations: %d' % len(relation_index))

def convert_kg():
    print('converting kg.txt file ...')

    file = './data/' + DATASET + '/'

    with open(file + 'kg_final.txt', 'w', encoding='utf-8') as f:
        for i in code_relation.index:
            content = code_relation.loc[i]
            if content['c2code'] not in entity_index:
                continue
            if content['p'] not in entity_index:
                continue
            if content['relation'] not in relation_index:
                continue

            new_c2code_index = entity_index[content['c2code']]
            new_relation_index = relation_index[content['relation']]
            new_tag_index = entity_index[content['p']]
            f.write('%d\t%d\t%d\n' % (new_c2code_index, new_relation_index, new_tag_index))

def convert_rating():
    print('converting ratings.txt file ...')

    file = './data/' + DATASET + '/'

    with open(file + 'ratings_final.txt', 'w', encoding='utf-8') as f:
        for i in user_movies.index:
            content = user_movies.loc[i]
            if content['object'] not in c2code_index:
                continue
            if content['userid'] not in user_index:
                continue

            new_user_index = user_index[content['userid']]
            new_c2code_index = c2code_index[content['object']]
            new_label = content['label']

            f.write('%d\t%d\t%d\n' % (new_user_index, new_c2code_index, new_label))

def set_redis(args):
    r = redis.Redis(host=args.r_url, port=args.r_port, password=args.r_password)
    for u in user_index:
        r.set(u, str(user_index[u]))

    for c in c2code_index:
        r.set(c, str(c2code_index[c]))
    r.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--d', type=str, default='utsc', help='which dataset to preprocess')
    parser.add_argument('-sysid', '--sysid', type=str, default='t', help='Telecom: t  Mobile: m   Unicom: u')
    parser.add_argument('-r_url','--r_url', type=str, default="172.19.133.31", help='redis url')
    parser.add_argument('-r_port','--r_port', type=str, default="6379", help='redis port')
    parser.add_argument('-r_password','--r_password', type=str, default="redisredis", help='redis password')

    args = parser.parse_args()
    DATASET = args.d + args.sysid

    code_name = pd.read_csv('./data/' + DATASET + '/code_name.csv', encoding='utf-8', sep="^")
    code_relation = pd.read_csv('./data/' + DATASET + '/code_relation.csv', encoding='utf-8', sep="^")
    user_movies = pd.read_csv('./data/' + DATASET + '/user_movies.csv', dtype={'userid': str, 'object': str},encoding='utf-8', sep="^")

    entity_index = dict()
    relation_index = dict()
    c2code_index = dict()
    user_index = dict()

    read_index()
    convert_rating()
    convert_kg()

    # set_redis(args)

    print('done')
