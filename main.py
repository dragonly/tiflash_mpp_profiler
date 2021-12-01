import argparse
import json
import sys
from os.path import expanduser
from visualize_task_json import draw_tasks
import utils

import paramiko
import yaml

HOME_DIR = expanduser("~")


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
    remote_log_filename = '{}/tiflash.log'.format(log_dir)
    local_log_filename = '{}.tiflash.log'.format(host)
    print('scp {}@{}:{}/{} {}'.format(username, host, port, remote_log_filename, local_log_filename))
    sftp.get(remote_log_filename, local_log_filename)
    return local_log_filename


def parse_log(filename):
    with open(filename, 'r') as fd:
        ret = []
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
    return ret


# collect will try to use tiup configuration in this machine for the cluster specified in args
def collect(parser, args):
    username, ssh_key_file, tiflash_servers = get_tiup_config(args.cluster)
    for server in tiflash_servers:
        log_filename = copy_log_file(server['host'], server['ssh_port'], username, ssh_key_file, server['log_dir'])
        parse_log(log_filename)


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
