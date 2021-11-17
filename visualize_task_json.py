import json

import graphviz

OUTPUT_DIR = 'output'


def read_json(filename):
    with open(filename, 'r') as fd:
        data = json.load(fd)
    return data


class Graph:
    def __init__(self):
        self._g = graphviz.Digraph(comment="main graph")

    def addTaskGraph(self, task_graph):
        self._g.subgraph(task_graph._g)

    def render(self):
        path = '{}/query_task'.format(OUTPUT_DIR)
        self._g.render(path, format='png')


class TaskGraph:
    def __init__(self, task_id, details):
        self.serial = 0
        self._g = graphviz.Digraph(
            name="cluster_"+str(task_id), comment="task graph")
        self.details = details

    def draw_executors(self, node):
        self.draw_executor_nodes(node)
        self.draw_executor_edges(node)

    def draw_executor_nodes(self, node):
        detail = self.details[node['id']]
        label = '{}({})\n{}'.format(
            detail['type'], detail['id'], self.gen_label_details(detail))
        self._g.node(node['id'], label)
        for child in node['children']:
            self.draw_executor_nodes(child)

    def draw_executor_edges(self, node):
        for child in node['children']:
            self._g.edge(child['id'], node['id'])
            self.draw_executor_edges(child)

    @staticmethod
    def gen_label_details(detail):
        labels = []
        for k, v in detail.items():
            if k != 'id' and k != 'type':
                labels.append('{}: {}'.format(k, v))
        return '\n'.join(labels)


def gen_task_graph(task):
    task_id = task['task_id']
    root = task['executors']['structure']
    details = task['executors']['details']
    print(root)
    print(details)
    task_graph = TaskGraph(task_id, details)
    task_graph.draw_executors(root)
    return task_graph


if __name__ == '__main__':
    data = read_json('tracing.json')
    graph = Graph()
    for task in data:
        task_graph = gen_task_graph(task)
        graph.addTaskGraph(task_graph)
    graph.render()
