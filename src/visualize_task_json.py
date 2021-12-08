import json
import logging
from collections import defaultdict
from typing import Any, Dict, List

import graphviz

from utils import read_json

OUTPUT_DIR = 'output'


def _gen_label_executor(detail):
    not_keys = {'type', 'id', 'children', 'sender_target_task_ids', 'receiver_source_task_ids', 'connection_details'}
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


# translate input json list [task1, task2] to {id1: task1} for better performance and code structure
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
        self._task = task
        self._executors = _trans_list_to_id_map(task['executors'])
        self._task_id = task['task_id']
        self._receiver_sources: Dict[int, List[int]] = {}
        self._g = graphviz.Digraph(name='cluster_'+str(task['task_id']), comment='task')
        if self._task['status'] == 'FINISHED' and self._task['error_message'] == '':
            self._g.attr(label=_gen_label_executor_task(task), labeljust='l', labelloc='b', style='solid')
        else:
            self._g.attr(label=_gen_label_executor_task(task), labeljust='l',
                         labelloc='b', style='solid', color='red', penwidth='3')

    @property
    def g(self):
        return self._g

    @property
    def id(self):
        return self._task_id

    @property
    def sender_executor_id(self):
        return self._task['sender_executor_id']

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
                self._g.edge(self.get_node_id(child_id), self.get_node_id(eid), weight='0')

    def get_node_id(self, node_id):
        return '{}-{}'.format(self._task_id, node_id)


class Graph:
    def __init__(self):
        self._g = graphviz.Digraph(comment='main')
        self._g.attr(rankdir='BT', splines='line')
        self._task_graphs: Dict[int, TaskGraph] = {}
        self._stages: Dict[int, List[TaskGraph]] = defaultdict(list)

    def addTaskGraph(self, task_graph: TaskGraph):
        # self._g.subgraph(task_graph.g)
        self._stages[task_graph.sender_executor_id].append(task_graph)
        self._task_graphs[task_graph.id] = task_graph

    def _draw_stages(self):
        for sender_executor_id, task_graphs in self._stages.items():
            stage_g = graphviz.Digraph(name='cluster_{}'.format(sender_executor_id), comment='stage')
            stage_g.attr(label='stage', labeljust='c', labelloc='b', style='dashed', rank='same')
            for task_graph in task_graphs:
                stage_g.subgraph(task_graph.g)
            self._g.subgraph(stage_g)

    # draw executor references corssing task boundaries
    def _draw_task_references(self):
        for receiver_task_id, receiver_task_graph in self._task_graphs.items():
            for receiver_executor_id, sender_task_ids in receiver_task_graph.receiver_sources.items():
                receiver_node_id = receiver_task_graph.get_node_id(receiver_executor_id)
                for sender_task_id in sender_task_ids:
                    if sender_task_id not in self._task_graphs:
                        logging.error('failed to get source task with task_id [{}] for receiver task [{}]'.format(
                            sender_task_id, receiver_task_id))
                        continue
                    sender_task_graph = self._task_graphs[sender_task_id]
                    sender_executor_id = sender_task_graph.sender_executor_id
                    sender_node_id = sender_task_graph.get_node_id(sender_executor_id)
                    self._g.edge(sender_node_id, receiver_node_id, color='red', style='dashed')

    def render(self, filename, format):
        self._draw_stages()
        self._draw_task_references()
        self._g.render(filename, format=format)


def draw_tasks_dag(data, filename, format='png'):
    if filename is None:
        filename = '{}/query_task.dot'.format(OUTPUT_DIR)
    graph = Graph()
    for task in data:
        task_graph = TaskGraph(task)
        task_graph.draw_executors()
        graph.addTaskGraph(task_graph)
    graph.render(filename, format)


def draw_input_streams():
    data = read_json('/Users/dragonly/Downloads/test-input-streams.json')
    graph = InputStreamGraph()
    for task in data:
        task_graph = InputStreamTaskGraph(task)
        task_graph.draw_input_streams()
        graph.addTaskGraph(task_graph)
    graph.render()


# TODO: query tso should be fixed
def draw_input_streams_timeline():
    data = read_json('/Users/dragonly/Downloads/test-input-streams(1).json')
    events = []
    task_id_set = set()

    # first round, collect metadata events (M)
    # for the moment, we use ui.perfetto.dev to draw the timeline
    # using task as process/stream as thread is good for drawing
    for task in data:
        task_id = task['query_tso']
        if task_id not in task_id_set:
            task_id_set.add(task_id)
            events.append({
                'pid': task_id,
                'ph': 'M',
                'name': 'process_name',
                'args': {'name': 'task {}'.format(task_id)}
            })
        input_streams = task['input_streams']
        for stream in input_streams:
            stream_id = stream['id']
            events.append({
                'pid': task_id,
                'tid': stream_id,
                'ph': 'M',
                'name': 'thread_name',
                'args': {'name': 'stream {}'.format(stream_id)}
            })

    # second round, collect counter events
    for task in data:
        task_id = task['query_tso']
        input_streams = task['input_streams']
        for stream in input_streams:
            stream_id = stream['id']
            name = '[{}] {}({})'.format(stream_id, stream['name'], stream['executor'])
            stats = stream['stat']
            timeline_push = stats['timeline']['push']
            timeline_pull = stats['timeline']['pull']
            timeline_self = stats['timeline']['self']
            events_push = _gen_counter_events(task_id, stream_id, name, timeline_push, timeline_pull, timeline_self)
            events.extend(events_push)

    with open('output/timeline.json', 'w') as fd:
        json.dump(events, fd)

# currently we assume that each


def _gen_counter_events(pid, tid, name, push, pull, self):
    ret = []
    ts = 1
    for c1, c2, c3 in zip(push, pull, self):
        ret.append({
            'pid': pid,
            'tid': tid,
            'ph': 'C',
            'name': name,
            'ts': ts,
            'args': {'push': c1, 'pull': c2, 'self': c3}
        })
        ts += 100*1000  # microsecond
    return ret


if __name__ == '__main__':
    # TODO: consider multiple query tso
    draw_tasks_dag(read_json('/Users/dragonly/Downloads/multi_machine_mpp_task_tracing.json'))
    # draw_input_streams()
    # draw_input_streams_timeline()
