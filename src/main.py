import argparse
import json
import os
import re
import sys
from os.path import expanduser

import paramiko
import yaml

import utils
from visualize_task_json import draw_tasks_dag

HOME_DIR = expanduser("~")
FLASHPROF_DIR = os.path.join(os.path.realpath('.'), 'flashprof')
FLASHPROF_CLUSTER_DIR = os.path.join(FLASHPROF_DIR, 'cluster')


def _get_tiup_config(cluster_name):
    cluster_dir = '{}/.tiup/storage/cluster/clusters/{}/'.format(HOME_DIR, cluster_name)
    ssh_key_file = '{}/ssh/id_rsa'.format(cluster_dir)
    print('cluster_dir = {}'.format(cluster_dir))
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
    sftp = ssh.open_sftp()
    remote_log_filename = os.path.join(remote_log_dir, 'tiflash.log')
    local_log_filename = os.path.join(FLASHPROF_CLUSTER_DIR, cluster_name, 'log', '{}.tiflash.log'.format(host))
    print('scp {}@{}:{}/{} {}'.format(username, host, port, remote_log_filename, local_log_filename))
    sftp.get(remote_log_filename, local_log_filename)


def _parse_log_to_file(log_dir, task_dag_dir):
    for log_filename in os.listdir(log_dir):
        ret = []
        print('parsing {}'.format(log_filename))
        with open(os.path.join(log_dir, log_filename), 'r') as fd:
            for line in fd:
                match = re.search(r'\["MPPTask:<query:\d+,task:\d+> mpp_task_tracing (\{.+\})', line)
                if match is None:
                    continue
                json_str = match.group(1).replace('\\', '')
                try:
                    data = json.loads(json_str)
                except Exception as e:
                    print(e)
                    continue
                ret.append(data)
        output_filename = os.path.join(task_dag_dir, log_filename + '.task_dag.json')
        print('write to {}'.format(output_filename))
        with open(output_filename, 'wt') as fd:
            json.dump(ret, fd, indent=1)


def _combine_json_files(json_dir):
    # TODO: potential optimization, make this stream-ish to reduce memory consumption
    combined = []
    for filename in os.listdir(json_dir):
        data = utils.read_file(os.path.join(json_dir, filename))
        # this must success, because it should dumped with this same script
        parsed = json.loads(data)
        combined.extend(parsed)
    with open(os.path.join(json_dir, 'cluster.json'), 'wt') as fd:
        json.dump(combined, fd, indent=1)


def _parse_cluster_log(cluster_name):
    log_dir = os.path.join(FLASHPROF_CLUSTER_DIR, cluster_name, 'log')
    task_dag_dir = os.path.join(FLASHPROF_CLUSTER_DIR, cluster_name, 'task_dag')
    _parse_log_to_file(log_dir, task_dag_dir)
    _combine_json_files(task_dag_dir)


def collect(parser, args):
    username, ssh_key_file, tiflash_servers = _get_tiup_config(args.cluster)
    log_dir = os.path.join(FLASHPROF_CLUSTER_DIR, args.cluster, 'log')
    task_dag_dir = os.path.join(FLASHPROF_CLUSTER_DIR, args.cluster, 'task_dag')
    utils.ensure_dir_exist(log_dir)
    utils.ensure_dir_exist(task_dag_dir)
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


def render(parser, args):
    task_dag = utils.read_json(args.json_file)
    filename = os.path.join(args.out_dir, (os.path.basename(args.json_file)))
    if args.type == 'task_dag':
        draw_tasks_dag(task_dag, filename, args.format)
    else:
        raise Exception('type {} is not supported yet'.format(args.type))


def default(parser, args):
    parser.print_help()


def cli():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default)

    subparsers = parser.add_subparsers()

    parser_collect = subparsers.add_parser(
        'collect', help='try to use tiup configuration in this machine for the cluster specified in args')
    parser_collect.add_argument('--cluster', type=str, required=True)
    parser_collect.set_defaults(func=collect)

    parser_parse = subparsers.add_parser('parse', help='default to parse all clusters\' logs, mainly for debug')
    parser_parse.add_argument('--cluster', type=str)
    parser_parse.set_defaults(func=parse)

    parser_render = subparsers.add_parser('render', help='render task dag files into graphic format')
    parser_render.add_argument('--json_file', type=str, required=True)
    parser_render.add_argument('--out_dir', type=str, required=True)
    parser_render.add_argument('--type', type=str, default='task_dag', choices=['task_dag', 'input_stream_dag'])
    parser_render.add_argument('--format', type=str, default='png')
    parser_render.set_defaults(func=render)

    args = parser.parse_args(sys.argv[1:])
    args.func(parser, args)


if __name__ == '__main__':
    cli()
