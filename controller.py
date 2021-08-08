from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.recipe.queue import LockingQueue
import argparse
import os
import time
import logging
import sys
import instance_manager

import googleapiclient.discovery
from six.moves import input



# zookeeper client setting / start
kz = KazooClient(hosts='34.64.159.101:2181')
kz.start(60)

compute = googleapiclient.discovery.build('compute', 'v1')

lq = LockingQueue(kz, path='/crawl/page')

logging.basicConfig()

############ Zookeeper functions #################

# Set inital node formation
def initial_set_znode(interval, activeworkers, pages):
    interval = interval.encode('utf-8')
    activeworkers = activeworkers.encode('utf-8')
    pages = pages.encode('utf-8')

    # Super folder
    kz.ensure_path('/setting')
    kz.ensure_path('/agent')
    kz.ensure_path('/crawl')

    # Next nodes - setting
    kz.create('/setting/interval', value=interval)
    kz.create('/setting/activeworker', value=activeworkers)
    kz.create('/setting/status')

    # Next nodes - agent
    kz.ensure_path('/agent/workers')
    kz.create('/agent/controller', ephemeral=True)

    # Next nodes - crawl
    kz.ensure_path('/crawl/workers')
    kz.ensure_path('/crawl/pages')
    kz.create('/crawl/seed')

    for i in range(int(pages.decode('utf-8'))):
        kz.create('/crawl/pages/page', value=str(i + 1).encode('utf-8'), sequence=True)
    for i in range(int(activeworkers.decode('utf-8'))):
        kz.create('/crawl/workers/worker', value=b'CREATING', sequence=True)

def add_watchers(project, bucket, zone):

    @kz.DataWatch('/setting/status')
    def watch_status(data, stat, event):
        if data != None:
            if data.decode('utf-8') == 'DONE':
                delete_all_workers(project, bucket, zone)
                delete_all_nodes()
                print('Ending crawl process...')
            
    # Add / delete workers according to worker failure (disconnection etc)
    @kz.ChildrenWatch('/agent/workers')
    def watch_workers(workers):

        prev_num = int(kz.get('/setting/activeworker')[0].decode('utf-8'))

        working_num = len(workers)

        working_workers = kz.get_children('/crawl/workers')

        if prev_num > working_num:

            disconnected_list = list(set(working_workers) - set(workers))

            print(disconnected_list)

            for disconnected in disconnected_list:

                print('disconnected : {0}'.format(disconnected))

                path = '/crawl/workers/{0}'.format(disconnected)

                if kz.exists('/agent/workers/{0}'.format(disconnected)) is None:
                    
                    state = kz.get(path)[0].decode('utf-8')

                    print(state)

                    if state == 'DONE':

                        print('worker DONE - {0}'.format(disconnected))

                    elif state == 'RUNNING':
                        
                        kz.delete(path, recursive=True)

                        # Create new Node
                        worker_path = kz.create('/crawl/workers/worker', sequence=True)
                        new_name = worker_path.split('/')[-1]

                        # Create new Instance
                        operation = instance_manager.create_instance(compute, project, zone, new_name, bucket)

                        instance_manager.wait_for_operation(compute, project, zone, operation['name'])

        if prev_num == working_num:
            print(prev_num)
            print(working_num)
            print(working_workers)

            instances = instance_manager.list_instances(compute, project, zone)

            print('Instances in project %s and zone %s:' % (project, zone))
            for instance in instances:
                print(' - ' + instance['name'])
            

    # Change number of workers according to activeworkers -> user command
    @kz.DataWatch('/setting/activeworker')
    def watch_active_worker(data, stat, event):

        if data != None:
            was_active = len(kz.get_children('/crawl/workers'))
            is_active = int(data.decode('utf-8'))

            if was_active < is_active:
                change = is_active - was_active
                add_workers(project, bucket, zone, change)
            elif was_active > is_active:
                change = was_active - is_active
                remove_workers(project, bucket, zone, change)

def start(project, bucket, zone):
    child_list = kz.get_children('/crawl/workers')
    print(child_list)


    for worker_child in child_list:

        kz.set('/crawl/workers/{}'.format(worker_child), value=b'CREATING')

        print('Creating instance.')

        operation = instance_manager.create_instance(compute, project, zone, worker_child, bucket)
        instance_manager.wait_for_operation(compute, project, zone, operation['name'])

    instances = instance_manager.list_instances(compute, project, zone)


    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'])


def add_workers(project, bucket, zone, change):

    while change > 0:
        worker_path = kz.create('/crawl/workers/worker', sequence=True)
        add_name = worker_path.split('/')[-1]

        kz.set('crawl/workers/{0}'.format(add_name), value=b'CREATING')

        print('Adding node {0}...'.format(add_name))

        print('Creating instance {0}...'.format(add_name))

        operation = instance_manager.create_instance(compute, project, zone, add_name, bucket)

        instance_manager.wait_for_operation(compute, project, zone, operation['name'])

        change -= 1

    instances = instance_manager.list_instances(compute, project, zone)


    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'])
    

def remove_workers(project, bucket, zone, change):

    while change > 0:

        rm_name = kz.get_children('/crawl/workers')[-1]

        print('Deleting node {0}...'.format(rm_name))

        lq.put(kz.get('crawl/workers/{0}/page'.format(rm_name))[0])

        kz.delete('/crawl/workers/{0}'.format(rm_name), recursive=True)

        print('Deleting instance {0}...'.format(rm_name))

        operation = instance_manager.delete_instance(compute, project, zone, rm_name)

        instance_manager.wait_for_operation(compute, project, zone, operation['name'])

        change -= 1

    instances = instance_manager.list_instances(compute, project, zone)


    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'])

def delete_all_workers(project, bucket, zone):
    chldrn = kz.get_children('/crawl/workers')

    for child in chldrn:
        operation = instance_manager.delete_instance(compute, project, zone, child)

        instance_manager.wait_for_operation(compute, project, zone, operation['name'])

    print('Worker instances deleted!')

def delete_controller(project, bucket, zone):

    instance_manager.delete_instance(compute, project, zone, 'controller')

    print('Controller is deleted!')

def delete_all_nodes():
    kz.delete('/setting', recursive=True)
    kz.delete('/agent', recursive=True)
    kz.delete('/crawl', recursive=True)

    print('All nodes deleted!')

def enqueue(pages):
    pg_lst = []
    for i in range(int(pages)):
        pg_lst.append(str(i + 1).encode('utf-8'))
    lq.put_all(pg_lst)


# [START run]
def main(project, bucket, zone):

        command_status = 'NONE'

        print('Controller started...')

        while command_status != 'DONE':
            user_input = input('Enter a command : ')
            command_line = user_input.split(' ')
            command = command_line[0]

            if command == 'set':
                if command_status == 'NONE':
                    command_status = 'NODE'

                    pages = input('Enter pages : ')
                    interval = input('Enter crawl task interval : ')
                    activeworkers = input('Enter number of active workers : ')

                    initial_set_znode(interval, activeworkers, pages)
                    enqueue(pages)
                    print('Nodes are set and ready!')
                else:
                    print('Can not process this command : Already have nodes or an error occurred')

            elif command == 'ready':
                if command_status == 'NODE':
                    command_status = 'WATCH'

                    start(project, bucket, zone)
                    print('Workers are ready!')
                else:
                    print('Can not process this command : No nodes / Already have watchers')

            elif command == 'start':
                if command_status == 'WATCH':
                    add_watchers(project, bucket, zone)
                    print('set watchers!')
                    kz.set('/setting/status', value=b'RUNNING')
                    print('Now starting process...')
                else:
                    print('There are no nodes or watchers ready')

            elif command == 'stop':
                if command_status == 'WATCH':
                    kz.set('/setting/status', value=b'STOP')
                    print('Suspending process...')
                else:
                    print('There are no nodes or watchers ready')

            elif command == 'interval':
                if command_status == 'WATCH':
                    amount = command_line[1]
                    val = amount.encode('utf-8')
                    kz.set('/setting/interval', value=val)
                    print('Setting new crawl interval...')
                else:
                    print('There are no nodes or watchers ready')

            elif command == 'active':
                if command_status == 'WATCH':
                    amount = command_line[1]
                    val = amount.encode('utf-8')
                    kz.set('/setting/activeworker', value=val)
                    print('Changing the number of workers working...')
                else:
                    print('There are no nodes or watchers ready')

            elif command == 'reset':
                if command_status == 'WATCH': 
                    # Delete all nodes
                    delete_all_workers(project, bucket, zone)
                    delete_all_nodes()

                    command_status = 'NONE'
                    print('Nodes are reset and ready!')
                elif command_status == 'NODE':
                    delete_all_nodes()

                    command_status = 'NONE'
                    print('Nodes are reset and ready!')
                elif command_status == 'NONE':
                    if len(instance_manager.list_instances(compute, project, zone)) > 3:
                        delete_all_workers(project, bucket, zone)
                    if len(kz.get_children('/')) > 0:
                        delete_all_nodes()
                
                    command_status = 'NONE'
                    print('Nodes are reset and ready!')
                else:
                    print('There are no nodes or watchers ready')
            elif command == 'quit':
                kz.set('/setting/status', value=b'QUIT')
                delete_all_workers(project, bucket, zone)
                delete_all_nodes()
                print('Ending crawl job and deleting controller...')
                kz.close()
                command_status = 'DONE'
            else:
                print('I cannot understand the command. Please type again.')



if __name__ == '__main__':
    # parser = argparse.ArgumentParser(
    #     description=__doc__,
    #     formatter_class=argparse.RawDescriptionHelpFormatter)
    # parser.add_argument('project_id', default='fresh-electron-277802', help='Your Google Cloud project ID.')
    # parser.add_argument(
    #     'bucket_name', default='crawl-bucket2', help='Your Google Cloud Storage bucket name.')
    # parser.add_argument(
    #     '--zone',
    #     default='asia-northeast3-a',
    #     help='Compute Engine zone to deploy to.')

    # args = parser.parse_args()

    # main(args.project_id, args.bucket_name, args.zone)
    main('fresh-electron-277802', 'crawl-bucket2', 'asia-northeast3-a')