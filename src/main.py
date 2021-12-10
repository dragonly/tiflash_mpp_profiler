import argparse
import json
import logging
import os
import re
import shutil
import sys
from collections import defaultdict
from enum import Enum
from os.path import expanduser

import paramiko
import yaml

import utils
from visualize_task_json import draw_tasks_dag

HOME_DIR = expanduser("~")
FLASHPROF_DIR = os.path.join(os.path.realpath('.'), 'flashprof')
FLASHPROF_CLUSTER_DIR = os.path.join(FLASHPROF_DIR, 'cluster')


class RENDER_TYPE(Enum):
    TASK_DAG = 'task_dag'

    def __str__(self):
        return self.value


def _get_tiup_config(cluster_name):
    cluster_dir = '{}/.tiup/storage/cluster/clusters/{}/'.format(HOME_DIR, cluster_name)
    ssh_key_file = '{}/ssh/id_rsa'.format(cluster_dir)
    logging.debug('cluster_dir = {}'.format(cluster_dir))
    meta_filename = '{}/meta.yaml'.format(cluster_dir)
    fd = open(meta_filename, 'r')
    meta = yaml.safe_load(fd.read())
    fd.close()
    username = meta['user']
    tiflash_servers = meta['topology']['tiflash_servers']
    return username, ssh_key_file, tiflash_servers


def _copy_log_file(host, port, username, ssh_key_file, remote_log_dir, cluster_name):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, port, username, key_filename=ssh_key_file)
    # sftp = ssh.open_sftp()
    remote_log_filename = os.path.join(remote_log_dir, 'tiflash.log')
    local_log_filename = os.path.join(FLASHPROF_CLUSTER_DIR, cluster_name, 'log', '{}.tiflash.log'.format(host))
    # remote_log_filename = '/mnt/pingcap/tiflash_mpp_profiler/log/172.16.5.59.tiflash.log'
    command = r'grep -P "\[\"MPPTask:<query:\d+,task:\d+> mpp_task_tracing" {}'.format(remote_log_filename)
    logging.debug('executing ssh command: {}'.format(command))
    stdin, stdout, stderr = ssh.exec_command(command)
    logging.info('grep & scp {}@{}:{}/{} {}'.format(username, host, port, remote_log_filename, local_log_filename))
    # sftp.get(remote_log_filename, local_log_filename)
    with open(local_log_filename, 'wb') as fd:
        fd.write(stdout.read())
    err = stderr.read()
    if len(err) != 0:
        logging.error('error occurs: {}'.format(err))
    else:
        logging.info('success')


def _parse_log_to_file(log_dir, task_dag_json_dir):
    for log_filename in os.listdir(log_dir):
        ret = []
        logging.info('parsing {}'.format(log_filename))
        with open(os.path.join(log_dir, log_filename), 'r') as fd:
            for line in fd:
                match = re.search(r'\["MPPTask:<query:\d+,task:\d+> mpp_task_tracing (\{.+\})', line)
                if match is None:
                    continue
                json_str = match.group(1).replace('\\', '')
                try:
                    data = json.loads(json_str)
                except Exception as e:
                    logging.error('failed to load json: {}\n{}'.format(e, json_str))
                    continue
                ret.append(data)
        output_filename = os.path.join(task_dag_json_dir, log_filename + '.task_dag.json')
        logging.info('write to {}'.format(output_filename))
        with open(output_filename, 'wt') as fd:
            json.dump(ret, fd, indent=1)


def _clean_cluster(cluster_name):
    cluster_dir = os.path.join(FLASHPROF_CLUSTER_DIR, cluster_name)
    if os.path.exists(cluster_dir):
        shutil.rmtree(cluster_dir)


def _combine_json_files(json_dir, except_list):
    # TODO: potential optimization, make this stream-ish to reduce memory consumption
    combined = []
    for filename in os.listdir(json_dir):
        if filename in except_list:
            continue
        data = utils.read_file(os.path.join(json_dir, filename))
        # this must success, because it should dumped with this same script
        parsed = json.loads(data)
        combined.extend(parsed)
    with open(os.path.join(json_dir, 'cluster.json'), 'wt') as fd:
        json.dump(combined, fd, indent=1)


def _parse_cluster_log(cluster_name):
    log_dir = os.path.join(FLASHPROF_CLUSTER_DIR, cluster_name, 'log')
    task_dag_json_dir = os.path.join(FLASHPROF_CLUSTER_DIR, cluster_name, 'task_dag', 'json')
    utils.ensure_dir_exist(log_dir)
    utils.ensure_dir_exist(task_dag_json_dir)
    logging.info('parse logs in {}'.format(log_dir))
    _parse_log_to_file(log_dir, task_dag_json_dir)
    _combine_json_files(task_dag_json_dir, ['cluster.json'])


def collect(parser, args):
    _clean_cluster(args.cluster)
    username, ssh_key_file, tiflash_servers = _get_tiup_config(args.cluster)
    log_dir = os.path.join(FLASHPROF_CLUSTER_DIR, args.cluster, 'log')
    task_dag_json_dir = os.path.join(FLASHPROF_CLUSTER_DIR, args.cluster, 'task_dag', 'json')
    utils.ensure_dir_exist(log_dir)
    utils.ensure_dir_exist(task_dag_json_dir)
    for server in tiflash_servers:
        _copy_log_file(server['host'], server['ssh_port'], username, ssh_key_file, server['log_dir'], args.cluster)
    _parse_cluster_log(args.cluster)


def parse(parser, args):
    if args.cluster is None:
        # all clusters
        for cluster_name in os.listdir(FLASHPROF_CLUSTER_DIR):
            _parse_cluster_log(cluster_name)
    else:
        cluster_dir = os.path.join(FLASHPROF_CLUSTER_DIR, args.cluster)
        if not os.path.exists(cluster_dir):
            raise FileNotFoundError('cannot find cluster dir for {}, should be {}'.format(args.cluster, cluster_dir))
        _parse_cluster_log(args.cluster)


def _render_files(json_path, out_dir, type, format):
    '''
    The original json may contain multiple distinct query tso, so we need to split it into multiple input
    with a single tso in each.
    The number of output file(s) is equal to distinct(query_tso) in the original json file.
    '''
    json_data = utils.read_json(json_path)

    # split dataset according to query_tso
    tso_partitioned = defaultdict(lambda: defaultdict(list))  # {tso: {task_id: [INITIALIZING, FINISHED/CANCELLED]}}
    for data in json_data:
        query_tso = data['query_tso']
        task_id = data['task_id']
        tso_partitioned[query_tso][task_id].append(data)

    # only for debugging
    for query_tso, data in tso_partitioned.items():
        utils.ensure_dir_exist(os.path.join(out_dir, '.json_debug'))
        with open(os.path.join(out_dir, '.json_debug', '{}.json'.format(query_tso)), 'wt') as fd:
            json.dump(data, fd, indent=1)

    # overwrite task with INITIALIZING status with CANCELLED/FINISHED
    status_pruned = defaultdict(list)  # {tso: [list of tasks]}
    for query_tso, tasks_map in tso_partitioned.items():
        for task_id, tasks in tasks_map.items():
            if len(tasks) == 1:  # only INITIALIZING
                if tasks[0]['status'] != 'INITIALIZING':
                    raise ValueError(
                        'expect the only task has status INITIALIZING, tso [{}], task_id [{}]'.format(query_tso, task_id))
                status_pruned[query_tso].append(tasks[0])
            elif len(tasks) == 2:  # has INITIALIZING and FINISHED/CANCELLED
                if tasks[0]['status'] == tasks[1]['status'] == 'INITIALIZING':
                    raise ValueError(
                        '2 tasks must not all have INITIALIZING status, tso [{}], task_id [{}]'.format(query_tso, task_id))
                if tasks[0]['status'] != 'INITIALIZING' and tasks[0]['status'] != 'INITIALIZING':
                    raise ValueError(
                        'at least 1 task must have INITIALIZING status, tso [{}], task_id [{}]'.format(query_tso, task_id))
                _task = tasks[0]
                if _task['status'] == 'INITIALIZING':
                    _task = tasks[1]
                status_pruned[query_tso].append(_task)
            else:
                raise ValueError(
                    'expect only 1 or 2 tasks with status, got [{}], tso [{}], task_id [{}]'.format(len(tasks), query_tso, task_id))

    # NOTE: we can parallelize this and utilize all the cpu cores to render
    for query_tso, data in status_pruned.items():
        logging.debug('query_tso [{}], count [{}]'.format(query_tso, len(data)))
        out_path = os.path.join(out_dir, '{}.dot'.format(query_tso))
        logging.info('rendering query_tso [{}]'.format(query_tso))
        if type == RENDER_TYPE.TASK_DAG:
            draw_tasks_dag(data, out_path, format)
        else:
            raise Exception('type {} is not supported yet'.format(type))


def render_one(parser, args):
    _render_files(args.json_file, args.out_dir, args.type, args.format)


def render_cluster(parser, args):
    cluster_dirs = []
    # all clusters
    if args.cluster is None:
        for cluster_name in os.listdir(FLASHPROF_CLUSTER_DIR):
            cluster_dirs.append(os.path.join(FLASHPROF_CLUSTER_DIR, cluster_name))
    else:
        cluster_dirs = [os.path.join(FLASHPROF_CLUSTER_DIR, args.cluster)]

    for cluster_dir in cluster_dirs:
        logging.debug('start rendering for {}'.format(cluster_dir))
        json_path = os.path.join(cluster_dir, 'task_dag', 'json', 'cluster.json')
        out_dir = os.path.join(cluster_dir, 'task_dag', args.format)
        _render_files(json_path, out_dir, RENDER_TYPE.TASK_DAG, args.format)


def default(parser, args):
    parser.print_help()


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=str, default='info',
                        help='log level, use debug for more detailed logs, default to info')
    parser.set_defaults(func=default)

    subparsers = parser.add_subparsers()

    parser_collect = subparsers.add_parser(
        'collect', help='try to use tiup configuration in this machine for the cluster specified in args')
    parser_collect.add_argument('--cluster', type=str, required=True)
    parser_collect.set_defaults(func=collect)

    parser_render = subparsers.add_parser('render', help='render task dag files into graphic format')
    parser_render.add_argument('--cluster', type=str)
    parser_render.add_argument('--format', type=str, default='svg')
    parser_render.set_defaults(func=render_cluster)

    parser_parse = subparsers.add_parser('parse', help='default to parse all clusters\' logs, mainly for debugging')
    parser_parse.add_argument('--cluster', type=str)
    parser_parse.set_defaults(func=parse)

    parser_render_one = subparsers.add_parser('render_one', help='render one file, mainly for debugging')
    parser_render_one.add_argument('--json_file', type=str, required=True)
    parser_render_one.add_argument('--out_dir', type=str, required=True)
    parser_render_one.add_argument('--type', type=RENDER_TYPE, default=RENDER_TYPE.TASK_DAG, choices=list(RENDER_TYPE))
    parser_render_one.add_argument('--format', type=str, default='svg')
    parser_render_one.set_defaults(func=render_one)

    args = parser.parse_args(sys.argv[1:])

    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: {}'.format(args.log))
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s.%(msecs)03d %(levelname)s %(message)s', datefmt='%H:%M:%S')
    logging.debug('logging level is set to {}'.format(args.log.upper()))

    args.func(parser, args)

    logging.debug('done')


if __name__ == '__main__':
    cli()
