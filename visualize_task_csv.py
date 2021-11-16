import json
from collections import defaultdict
from typing import Any, Dict

import graphviz
import pandas as pd

OUTPUT_DIR = 'output'


def draw_test():
    g = graphviz.Graph(comment='The Round Table')
    dot = graphviz.Graph(name='cluster_sub1')
    dot.attr(label='sub1')
    # dot.attr(bgcolor='lightgreen')
    dot.node('A', 'A')
    dot.node('B', 'B')
    dot.node('L', 'L')
    dot.edge('A', 'B')
    dot.edge('A', 'L')
    dot.edge('B', 'L', constraint='false')
    g.subgraph(dot)
    dot = graphviz.Graph(name='cluster_sub2')
    dot.node('A1', 'A1')
    dot.node('B1', 'B1')
    dot.node('L1', 'L1')
    dot.edge('A1', 'B1')
    dot.edge('A1', 'L1')
    dot.edge('B1', 'L1', constraint='false')
    g.subgraph(dot)
    g.render('{}/draw_test'.format(OUTPUT_DIR), format='png')
    print(g.source)


TaskSets = Dict[str, Dict[str, Any]]


def draw(task_sets: TaskSets):
    g = graphviz.Digraph()
    subgraph_index = 0
    for q_id, tasks in task_sets.items():
        q_id = str(q_id)
        s = graphviz.Digraph(name='cluster_'+q_id)
        s.attr(label='q'+q_id)
        print(tasks)
        for t_id, payload in tasks.items():
            parent_ids = payload['parents']
            outputThroughput = payload['outputThroughput']
            t_id_label = 'task{}\noutput throuput: {}'.format(t_id, outputThroughput)
            t_id = 'q{}t{}'.format(subgraph_index, t_id)
            s.node(t_id, t_id_label)
            for parent in parent_ids:
                # skip root node
                if parent == -1:
                    continue
                parent_id = 'q{}t{}'.format(subgraph_index, parent)
                s.edge(t_id, parent_id)
        g.subgraph(s)
        subgraph_index += 1

    print(g.source)
    g.render('{}/query_task'.format(OUTPUT_DIR), format='png')


def parse(df: pd.DataFrame) -> TaskSets:
    task_sets = defaultdict(lambda: {})
    for label, row in df.iterrows():
        queryTso = row['queryTso']
        taskId = row['taskId']
        parents = json.loads(row['upstreamTaskIds'])
        outputThroughput = row['outputThroughput']
        task_sets[queryTso][taskId] = {
            'parents': parents,
            'outputThroughput': outputThroughput
        }
    return dict(task_sets)


def read_csv(filename: str) -> pd.DataFrame:
    df = pd.read_csv(filename)
    df = df[['queryTso', 'taskId', 'upstreamTaskIds', 'outputThroughput']]
    return df


if __name__ == '__main__':
    # draw_test()
    df = read_csv('tracing.csv')
    task_sets = parse(df)
    draw(task_sets)
