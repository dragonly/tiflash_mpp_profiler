# Usage

```bash
# install from pypi
pip3 install flashprof
# collect tiflash logs from tiup cluster to current directory, which will also be parsed to json
flashprof collect --cluster $CLUSTER_NAME
# draw dag using parsed json file, support task/input_stream DAG
flashprof draw --json_file $JSON_FILE --out_dir $OUT_DIR --type $DAG_TYPE
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
twine upload --repository testpypi dist/*
```

## Instructions

Please refer to https://packaging.python.org/guides/distributing-packages-using-setuptools/ for detaild instructions.
