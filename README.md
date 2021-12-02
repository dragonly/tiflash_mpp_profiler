# Usage

```bash
# collect tiflash logs from tiup cluster to current directory, which will also be parsed to json
flashprof collect --cluster $CLUSTER_NAME
# draw dag using parsed json file, support task/input_stream DAG
flashprof draw --json_file $JSON_FILE --out_dir $OUT_DIR --type $DAG_TYPE
```

# Packaging

## TL;DR

```bash
pip3 install build
python3 -m build
twine upload --repository testpypi dist/*
```

## Instructions

Please refer to https://packaging.python.org/guides/distributing-packages-using-setuptools/ for detaild instructions.
