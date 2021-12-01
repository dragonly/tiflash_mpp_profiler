import json


def read_json(filename):
    with open(filename, 'r') as fd:
        data = json.load(fd)
    return data
