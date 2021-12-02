import json
import os


def read_json(filename):
    with open(filename, 'r') as fd:
        data = json.load(fd)
    return data


def ensure_dir_exist(dir_name):
    if os.path.exists(dir_name):
        return
    os.makedirs(dir_name)
