# Usage

```bash
# install from pypi
pip3 install flashprof
# collect tiflash logs from tiup cluster to current directory, which will also be parsed to json
flashprof collect --cluster $CLUSTER_NAME
# draw dag using parsed json file, support task/input_stream DAG
flashprof draw --json_file $JSON_FILE --out_dir $OUT_DIR --type $DAG_TYPE
```

## data layout

The collected/generated artifacts have the following layout

```
flashprof
└── cluster
    ├── cluster1_name
    │   ├── log (collected from tiflash log dir)
    │   │   ├── ip1.tiflash.log
    │   │   └── ip2.tiflash.log
    │   └── task_dag (parsed and combined task dag)
    │       ├── json
    │       │   ├── ip1.tiflash.log.task_dag.json
    │       │   ├── ip2.tiflash.log.task_dag.json
    │       │   └── cluster.task_dag.json
    │       ├── png (rendered png files)
    │       └── svg (rendered svg files)
    └── cluster2_name
...
```

# Development

```bash
# install a local dev version of python package, then we can call flashprof
# rerun this when code is changed
# it internally creates a symbolic link to the current source code
pip3 install -e .
# remove if you want
pip3 uninstall flashprof
```

# Packaging

## TL;DR

```bash
pip3 install build
python3 -m build
twine check dist/*
twine upload dist/*
# upload to test.pypi.org for package publish related test
# twine upload --repository testpypi dist/*
```

## Instructions

Please refer to https://packaging.python.org/guides/distributing-packages-using-setuptools/ for detaild instructions.


## Internals

`collect` command collects tiflash logs according to the tiup configurations for the specified `--cluster $CLUSTER_NAME`, and logs are named `$IP.tiflash.log` in `flashprof/cluster/$CLUSTER_NAME/log`.

`parse` command parses all the tiflash logs collected above to the json format, which only contains task DAGs for now. The json files a then merged into a `cluster.json` in `flashprof/cluster/$CLUSTER_NAME/task_dag/json`.

`render` command renders `cluster.json` into dag graphs per `query_tso` in `flashprof/cluster/$CLUSTER_NAME/$FORMAT`.
