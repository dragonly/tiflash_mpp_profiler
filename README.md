# Usage

```bash
# collect tiflash logs from tiup cluster to current directory
flashprof collect --cluster $CLUSTER_NAME
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
