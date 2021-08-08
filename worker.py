from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.recipe.queue import LockingQueue
import argparse
import os
import time
import logging
import json
import requests

# zookeeper client setting / start
kz = KazooClient(hosts='34.64.159.101:2181')

kz.start(60)

lq = LockingQueue(kz, path='/crawl/page')

# Global variables
suspend = True
interval = 0
work_status = True
is_last = False

def worker_watchers(name):
    # Change crawling time interval
    @kz.DataWatch('/setting/interval')
    def watch_controller(data, stat, event):

        global interval
        
        interval = int(data.decode('utf-8'))

    # Stop / Resume command process
    @kz.DataWatch('/setting/status')
    def watch_status(data, stat, event):

        status = data.decode('utf-8')

        global suspend
        global work_status

        if status == 'STOP':
            # stop functioning
            suspend = True
            print('The process has stopped...')
        elif status == 'RUNNING':
            # start functioning
            suspend = False
            kz.set('crawl/workers/{0}'.format(name), value=b'RUNNING')
            print('The process is now running...')
        elif status == 'DONE':
            print('The crawl job is done...')
            work_status = False
        elif status == 'QUIT':
            print('Quitting the crawl job...')
            work_status = False
        else:
            suspend = True

    @kz.DataWatch('/crawl/workers/{}'.format(name))
    def watch_myself(data, stat, event):
        global work_status
        global suspend

        my_status = data.decode('utf-8')

        if my_status == 'DONE':
            print('Work is done for {}'.format(name))
            work_status = False
        elif my_status == 'RUNNING':
            print('Worker starting process...')
            suspend = False

    # Controller is disconnected
    @kz.ChildrenWatch('/agent')
    def watch_agent(data):

        global suspend

        if kz.exists('/agent/controller') == False:
            print('The controller has been disconnected')
            print('Please reconnect or relaunch the controller')
            suspend = True

    # Listener
    @kz.add_listener
    def connection_listener(state):
        if state == KazooState.LOST:
            print('connection : LOST')
        elif state == KazooState.SUSPENDED:
            print('connection : SUSPENDED')
        else:
            print('connection : CONNECTED')

def done(name):
    global work_status
    global is_last
    kz.set('/crawl/workers/{}'.format(name), value=b'DONE')
    if is_last == True:
        kz.set('/setting/status', value=b'DONE')


def dequeue(name):
    global is_last
    page = lq.get(3)
    if page == None:
        return None

    lq.consume()

    if lq.__len__() == 0:
        is_last = True
    
    kz.set('/crawl/workers/{}/page'.format(name), value=page)
    page = int(page.decode('utf-8'))

    return page
    

def crawl_process(name, bucket):

    page = dequeue(name)

    if page == None:
        return done(name)
    
    URL = 'https://search.map.daum.net/mapsearch/map.daum'
    kz.set('/crawl/seed', value=URL.encode('utf-8'))
    params = {
			'callback': 'jQuery181043954338136052695_1593395060744',
			'q': '커피',
			'msFlag': 'S',
			'sort': 0,
			'page': page
            }
    headers = {'Referer': 'https://map.kakao.com/'}
    
    read = requests.get(URL, params=params, headers=headers)

    if read.status_code == 403:
        raise Exception('IP Blocked')

    data = read.text
    data = json.loads(data[data.find('(')+1:data.rfind(')')-1])

    place_data = data["place"]

    result = []

    for d in place_data:
        # 데이터
        store_ID = d['confirmid']
        store_name = d['name']
        store_rate = d['rating_average']

        result.append('{0},{1},{2}\n'.format(store_ID, store_name, store_rate))

    result = ''.join(result)

    f = open('/root/{}.csv'.format(page), 'w')
    f.write(result)
    f.close()

    os.system('gsutil cp /root/{0}.csv gs://{1}/crawl-data/{0}.csv'.format(page, bucket))

    return result
    

def main(name, bucket):

    kz.create('/agent/workers/{0}'.format(name), ephemeral=True)
    kz.create('/crawl/workers/{0}/page'.format(name))

    worker_watchers(name)

    try:

        while work_status == True:
            while suspend == True:
                time.sleep(1)

            crawl_process(name, bucket)
            time.sleep(interval)

    except Exception as ex:
        #mylogger.info(ex)
        print('something is wrong')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('name', help='Instance Name')
    parser.add_argument('bucket', help='Bucket Name')

    args = parser.parse_args()

    # mylogger = logging.getLogger("my")
    # mylogger.setLevel(logging.DEBUG)

    # stream_hander = logging.StreamHandler()
    # mylogger.addHandler(stream_hander)

    # file_handler = logging.FileHandler('/root/my.log')
    # mylogger.addHandler(file_handler)

    # mylogger.info('start')

    main(args.name, args.bucket)