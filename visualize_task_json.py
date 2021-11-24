import json
from typing import Any, Dict, List

import graphviz

OUTPUT_DIR = 'output'


def read_json(filename):
    with open(filename, 'r') as fd:
        data = json.load(fd)
    return data


def _gen_label_executor(detail):
    not_keys = {'type', 'id', 'children', 'sender_target_task_ids', 'receiver_source_task_ids'}
    return _gen_label(detail, not_keys)


def _gen_label_executor_task(task):
    not_keys = {'executors', 'sender_executor_id'}
    return _gen_label(task, not_keys)


def _gen_label_input_stream(task):
    not_keys = {'id', 'name', 'stat'}
    outter = _gen_label(task, not_keys)
    not_keys_inner = {'timeline'}
    inner = _gen_label(task['stat'], not_keys_inner)
    return outter + inner

def _gen_label_input_stream_task(task):
    not_keys = {'input_streams'}
    return _gen_label(task, not_keys)


def _gen_label(data: Dict, not_keys, join_char='\l'):
    labels = []
    for k, v in data.items():
        if k not in not_keys:
            labels.append('{}: {}'.format(k, v))
    return join_char.join(labels) + join_char


def _trans_list_to_id_map(l: List) -> Dict[int, Any]:
    ret = {}
    for i in l:
        ret[i['id']] = i
    return ret


class InputStreamTaskGraph:
    def __init__(self, task):
        self._g = graphviz.Digraph(name='cluster_'+str(task['task_id']), comment='input stream graph')
        self._g.attr(label=_gen_label_input_stream_task(task), labeljust='l', labelloc='b')
        self._task_id = task['task_id']
        self._input_streams = _trans_list_to_id_map(task['input_streams'])

    @property
    def g(self):
        return self._g

    def draw_input_streams(self):
        self._draw_nodes()
        self._draw_edges()

    def _draw_nodes(self):
        for id, s in self._input_streams.items():
            label = '{}_{}\l{}'.format(s['name'], id, _gen_label_input_stream(s))
            self._g.node(self.get_node_id(id), label, shape='box')

    def _draw_edges(self):
        for id, s in self._input_streams.items():
            for child_id in s['children']:
                self._g.edge(self.get_node_id(child_id), self.get_node_id(id))

    def get_node_id(self, node_id):
        return '{}-{}'.format(self._task_id, node_id)


class InputStreamGraph:
    def __init__(self):
        self._g = graphviz.Digraph(comment='main graph')
        self._g.attr(rankdir='BT')

    def addTaskGraph(self, task_graph: InputStreamTaskGraph):
        self._g.subgraph(task_graph.g)

    def render(self):
        path = '{}/input_stream_task.dot'.format(OUTPUT_DIR)
        self._g.render(path, format='png')


class TaskGraph:
    def __init__(self, task):
        self._g = graphviz.Digraph(name='cluster_'+str(task['task_id']), comment='task graph')
        self._g.attr(label=_gen_label_executor_task(task), labeljust='l', labelloc='b')
        self._task = task
        self._executors = _trans_list_to_id_map(task['executors'])
        self._task_id = task['task_id']
        self._receiver_sources: Dict[int, List[int]] = {}

    @property
    def g(self):
        return self._g

    @property
    def id(self):
        return self._task_id

    @property
    def sender_executor_id(self):
        if 'sender_executor_id' in self._task:
            return self._task['sender_executor_id']
        return None

    @property
    def sender_executor(self):
        sender_executor_id = self._task['sender_executor_id']
        return self._executors[sender_executor_id]

    @property
    def receiver_sources(self):
        return self._receiver_sources

    def draw_executors(self):
        self._draw_executor_nodes()
        self._draw_executor_edges()
        self._collect_receiver_sources()

    def _collect_receiver_sources(self):
        for eid, e in self._executors.items():
            if 'receiver_source_task_ids' in e:
                self._receiver_sources[eid] = e['receiver_source_task_ids']

    def _draw_executor_nodes(self):
        for eid, e in self._executors.items():
            label = '{}_{}\l{}'.format(e['type'], e['id'], _gen_label_executor(e))
            self._g.node(self.get_node_id(eid), label, shape='box')

    def _draw_executor_edges(self):
        for eid, e in self._executors.items():
            for child_id in e['children']:
                self._g.edge(self.get_node_id(child_id), self.get_node_id(eid))

    def get_node_id(self, node_id):
        return '{}-{}'.format(self._task_id, node_id)


class Graph:
    def __init__(self):
        self._g = graphviz.Digraph(comment='main graph')
        self._g.attr(rankdir='BT')
        self._task_graphs: Dict[str, TaskGraph] = {}

    def addTaskGraph(self, task_graph: TaskGraph):
        self._g.subgraph(task_graph.g)
        self._task_graphs[task_graph.id] = task_graph

    # draw executor references corssing task boundaries
    def _draw_task_references(self):
        for _, receiver_task_graph in self._task_graphs.items():
            for receiver_executor_id, sender_task_ids in receiver_task_graph.receiver_sources.items():
                receiver_node_id = receiver_task_graph.get_node_id(receiver_executor_id)
                for sender_task_id in sender_task_ids:
                    sender_task_graph = self._task_graphs[sender_task_id]
                    sender_executor_id = sender_task_graph.sender_executor_id
                    sender_node_id = sender_task_graph.get_node_id(sender_executor_id)
                    self._g.edge(sender_node_id, receiver_node_id)

    def render(self):
        path = '{}/query_task.dot'.format(OUTPUT_DIR)
        self._draw_task_references()
        self._g.render(path, format='png')


def draw_tasks():
    data = read_json('tracing.json')
    graph = Graph()
    for task in data:
        task_graph = TaskGraph(task)
        task_graph.draw_executors()
        graph.addTaskGraph(task_graph)
    graph.render()


def draw_input_streams():
    data = read_json('/Users/dragonly/Downloads/test-input-streams.json')
    graph = InputStreamGraph()
    for task in data:
        task_graph = InputStreamTaskGraph(task)
        task_graph.draw_input_streams()
        graph.addTaskGraph(task_graph)
    graph.render()


if __name__ == '__main__':
    # draw_tasks()
    draw_input_streams()
