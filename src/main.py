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
FLASHPROF_LOG_DIR = os.path.join(FLASHPROF_DIR, 'log')
FLASHPROF_JSON_DIR = os.path.join(FLASHPROF_DIR, 'json')


def get_tiup_config(cluster_name):
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


def copy_log_file(host, port, username, ssh_key_file, log_dir):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, port, username, key_filename=ssh_key_file)
    sftp = ssh.open_sftp()
    remote_log_filename = os.path.join(log_dir, 'tiflash.log')
    local_log_filename = os.path.join(FLASHPROF_LOG_DIR, '{}.tiflash.log'.format(host))
    print('scp {}@{}:{}/{} {}'.format(username, host, port, remote_log_filename, local_log_filename))
    sftp.get(remote_log_filename, local_log_filename)


def parse_log_to_file(log_dir, json_dir):
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
        output_filename = os.path.join(json_dir, log_filename + '.task_dag.json')
        print('write to {}'.format(output_filename))
        with open(output_filename, 'wt') as fd:
            json.dump(ret, fd)


# collect will try to use tiup configuration in this machine for the cluster specified in args
def collect(parser, args):
    utils.ensure_dir_exist(FLASHPROF_LOG_DIR)
    utils.ensure_dir_exist(FLASHPROF_JSON_DIR)
    username, ssh_key_file, tiflash_servers = get_tiup_config(args.cluster)
    for server in tiflash_servers:
        copy_log_file(server['host'], server['ssh_port'], username, ssh_key_file, server['log_dir'])
    parse_log_to_file(FLASHPROF_LOG_DIR, FLASHPROF_JSON_DIR)


def parse(parser, args):
    parse_log_to_file(FLASHPROF_LOG_DIR, FLASHPROF_JSON_DIR)


def draw(parser, args):
    task_dag = utils.read_json(args.json_file)
    filename = os.path.join(args.out_dir, (os.path.basename(args.json_file)))
    if args.type == 'task_dag':
        draw_tasks_dag(task_dag, filename, args.format)


def default(parser, args):
    parser.print_help()


def cli():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default)

    subparsers = parser.add_subparsers()

    parser_collect = subparsers.add_parser('collect')
    parser_collect.add_argument('--cluster', type=str, required=True)
    parser_collect.set_defaults(func=collect)

    parser_parse = subparsers.add_parser('parse')
    parser_parse.set_defaults(func=parse)

    parser_draw = subparsers.add_parser('draw')
    parser_draw.add_argument('--json_file', type=str, required=True)
    parser_draw.add_argument('--type', type=str, required=True, choices=['task_dag', 'input_stream_dag'])
    parser_draw.add_argument('--out_dir', type=str, required=True)
    parser_draw.add_argument('--format', type=str, default='png')
    parser_draw.set_defaults(func=draw)

    args = parser.parse_args(sys.argv[1:])
    args.func(parser, args)


if __name__ == '__main__':
    cli()
