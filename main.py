import argparse
import sys
import paramiko
import yaml

from os.path import expanduser
HOME_DIR = expanduser("~")


def get_tiup_config(cluster_name):
    cluster_dir = '{}/.tiup/storage/cluster/clusters/{}/'.format(HOME_DIR, cluster_name)
    ssh_key_file = '{}/ssh/id_rsa'.format(cluster_dir)
    print('cluster_dir = {}'.format(cluster_dir))
    meta_filename = '{}/meta.yaml'.format(cluster_dir)
    file = open(meta_filename, 'r')
    meta = yaml.safe_load(file.read())
    file.close()
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


# collect will try to use tiup configuration in this machine for the cluster specified in args
def collect(parser, args):
    username, ssh_key_file, tiflash_servers = get_tiup_config(args.cluster)
    for server in tiflash_servers:
        copy_log_file(server['host'], server['ssh_port'], username, ssh_key_file, server['log_dir'])


def default(parser, args):
    parser.print_help()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default)
    subparsers = parser.add_subparsers()
    parser_collect = subparsers.add_parser('collect')
    parser_collect.add_argument('--cluster', type=str)
    parser_collect.set_defaults(func=collect)

    args = parser.parse_args(sys.argv[1:])
    args.func(parser, args)
