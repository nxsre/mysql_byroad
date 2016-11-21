# -*- coding:utf-8 -*-

import os, sys, time, json, yaml
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kafka import (KafkaClient, KafkaConsumer)

class spoorerClient(object):

    def __init__(self, zookeeper_hosts, kafka_hosts, zookeeper_url='/', timeout=3, log_dir='/tmp/spoorer'):
        self.zookeeper_hosts = zookeeper_hosts
        self.kafka_hosts = kafka_hosts
        self.timeout = timeout
        self.log_dir = log_dir
        self.log_file = log_dir + '/' + 'spoorer.log'
        self.kafka_logsize = {}
        self.result = []
    self.log_day_file = log_dir + '/' + 'spoorer_day.log.' + str(time.strftime("%Y-%m-%d", time.localtime()))
        self.log_keep_day = 1

        try:
            f = file(os.path.dirname(os.path.abspath(__file__)) + '/' + 'spoorer.yaml')
            self.white_topic_group = yaml.load(f)
        except IOError as e:
            print 'Error, spoorer.yaml is not found'
            sys.exit(1)
        else:
            f.close()
            if self.white_topic_group is None:
                self.white_topic_group = {}

        if not os.path.exists(self.log_dir):     os.mkdir(self.log_dir)

    for logfile in [x for x in os.listdir(self.log_dir) if x.split('.')[-1] != 'log' and x.split('.')[-1] != 'swp']:
        if int(time.mktime(time.strptime(logfile.split('.')[-1], '%Y-%m-%d'))) &lt; int(time.time()) - self.log_keep_day * 86400:
            os.remove(self.log_dir + '/' + logfile)

    if zookeeper_url == '/':
        self.zookeeper_url = zookeeper_url
    else:
        self.zookeeper_url = zookeeper_url + '/'

def spoorer(self):
    try:
        kafka_client = KafkaClient(self.kafka_hosts, timeout=self.timeout)
    except Exception as e:
        print "Error, cannot connect kafka broker."
        sys.exit(1)
    else:
        kafka_topics = kafka_client.topics
    finally:
        kafka_client.close()

    try:
        zookeeper_client = KazooClient(hosts=self.zookeeper_hosts, read_only=True, timeout=self.timeout)
        zookeeper_client.start()
    except Exception as e:
        print "Error, cannot connect zookeeper server."
        sys.exit(1)

    try:
        groups = map(str,zookeeper_client.get_children(self.zookeeper_url + 'consumers'))
    except NoNodeError as e:
        print "Error, invalid zookeeper url."
        zookeeper_client.stop()
        sys.exit(2)
    else:
        for group in groups:
            if 'offsets' not in zookeeper_client.get_children(self.zookeeper_url + 'consumers/%s' % group): continue
            topic_path = 'consumers/%s/offsets' % (group)
            topics = map(str,zookeeper_client.get_children(self.zookeeper_url + topic_path))
            if len(topics) == 0: continue
        
            for topic in topics:
                if topic not in self.white_topic_group.keys():
                    continue 
                elif group not in self.white_topic_group[topic].replace(' ','').split(','):
                    continue
                partition_path = 'consumers/%s/offsets/%s' % (group,topic)
                partitions = map(int,zookeeper_client.get_children(self.zookeeper_url + partition_path))
        
                for partition in partitions:
                    base_path = 'consumers/%s/%s/%s/%s' % (group, '%s', topic, partition)
                    owner_path, offset_path = base_path % 'owners', base_path % 'offsets'
                    offset = zookeeper_client.get(self.zookeeper_url + offset_path)[0]
    
                    try:
                        owner = zookeeper_client.get(self.zookeeper_url + owner_path)[0]
                    except NoNodeError as e:
                        owner = 'null'

                    metric = {'datetime':time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'topic':topic, 'group':group, 'partition':int(partition), 'logsize':None, 'offset':int(offset), 'lag':None, 'owner':owner}
                    self.result.append(metric)
    finally:
        zookeeper_client.stop()

    try:
        kafka_consumer = KafkaConsumer(bootstrap_servers=self.kafka_hosts)
    except Exception as e:
        print "Error, cannot connect kafka broker."
        sys.exit(1)
    else:
        for kafka_topic in kafka_topics:
            self.kafka_logsize[kafka_topic] = {}
            partitions = kafka_client.get_partition_ids_for_topic(kafka_topic)

            for partition in partitions:
                offset = kafka_consumer.get_partition_offsets(kafka_topic, partition, -1, 1)[0]
                self.kafka_logsize[kafka_topic][partition] = offset

        with open(self.log_file,'w') as f1, open(self.log_day_file,'a') as f2:

            for metric in self.result:
                logsize = self.kafka_logsize[metric['topic']][metric['partition']]
                metric['logsize'] = int(logsize)
                metric['lag'] = int(logsize) - int(metric['offset'])
                
                f1.write(json.dumps(metric,sort_keys=True) + '\n')
                f1.flush()
                f2.write(json.dumps(metric,sort_keys=True) + '\n')
                f2.flush()
    finally:
        kafka_consumer.close()

    return ''

if __name__ == '__main__':
    check = spoorerClient(zookeeper_hosts=‘zookeeperIP地址：端口', zookeeper_url=‘znode节点', kafka_hosts=‘kafkaIP：PORT', log_dir='/tmp/log/spoorer', timeout=3)
    print check.spoorer()