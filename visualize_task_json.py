import json
from typing import Dict

import graphviz

OUTPUT_DIR = 'output'


def read_json(filename):
    with open(filename, 'r') as fd:
        data = json.load(fd)
    return data


class Graph:
    def __init__(self):
        self._g = graphviz.Digraph(comment='main graph')

    def addTaskGraph(self, task_graph):
        self._g.subgraph(task_graph._g)

    def render(self):
        path = '{}/query_task.dot'.format(OUTPUT_DIR)
        self._g.render(path, format='png')


class TaskGraph:
    def __init__(self, task):
        self.serial = 0
        self._g = graphviz.Digraph(
            name='cluster_'+str(task['task_id']), comment='task graph')
        self._g.attr(label=_gen_label_task(task), labeljust='l')
        self.details = task['executors']['details']

    def draw_executors(self):
        root = task['executors']['structure']
        self._draw_executor_nodes(root)
        self._draw_executor_edges(root)

    def _draw_executor_nodes(self, node):
        detail = self.details[node['id']]
        label = '{}({})\l{}'.format(
            detail['type'], detail['id'], _gen_label_executor(detail))
        self._g.node(node['id'], label, shape='box')
        for child in node['children']:
            self._draw_executor_nodes(child)

    def _draw_executor_edges(self, node):
        for child in node['children']:
            self._g.edge(child['id'], node['id'])
            self._draw_executor_edges(child)


def _gen_label_executor(detail):
    not_keys = {'id', 'type'}
    return _gen_label(detail, not_keys)


def _gen_label_task(task):
    not_keys = not_keys = {'executors', 'upstream_task_ids'}
    return _gen_label(task, not_keys)


def _gen_label(data: Dict, not_keys, join_char='\l'):
    labels = []
    for k, v in data.items():
        if k not in not_keys:
            labels.append('{}: {}'.format(k, v))
    return join_char.join(labels) + join_char


def gen_task_graph(task):
    task_graph = TaskGraph(task)
    task_graph.draw_executors()
    return task_graph


if __name__ == '__main__':
    data = read_json('tracing.json')
    graph = Graph()
    for task in data:
        task_graph = gen_task_graph(task)
        graph.addTaskGraph(task_graph)
    graph.render()
