import argparse
import json
import os
import sys
from os.path import expanduser
from visualize_task_json import draw_tasks
import utils

import paramiko
import yaml

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
    for filename in os.listdir(log_dir):
        ret = []
        with open(filename, 'r') as fd:
            for line in fd:
                if 'mpp_task_tracing' not in line:
                    continue
                l = line.find('{')
                r = line.rfind('}')
                if l == -1 or r == -1:
                    print('cannot parse json in "{}"'.format(line))
                    exit(-1)
                json_str = line[l:r+1].replace('\\', '')
                data = json.loads(json_str)
                ret.append(data)
        output_filename = os.path.join(json_dir, filename + '.json')
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


def draw(parser, args):
    task_dag = utils.read_json(args.json_file)
    draw_tasks(task_dag)


def default(parser, args):
    parser.print_help()


def cli():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default)

    subparsers = parser.add_subparsers()

    parser_collect = subparsers.add_parser('collect')
    parser_collect.add_argument('--cluster', type=str)
    parser_collect.set_defaults(func=collect)

    parser_draw = subparsers.add_parser('draw')
    parser_draw.add_argument('--json_file', type=str)
    parser_draw.set_defaults(func=draw)

    args = parser.parse_args(sys.argv[1:])
    args.func(parser, args)


if __name__ == '__main__':
    cli()
