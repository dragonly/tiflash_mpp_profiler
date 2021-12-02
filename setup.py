from setuptools import setup


def read_file(filename):
    with open(filename, 'r') as fd:
        return fd.read()


# see examples in https://docs.python.org/3/distutils/examples.html
setup(
    name='flashprof',
    version='0.0.2',
    description='a tool that collects and visualizes TiFlash runtime infomation',
    author='dragonly',
    author_email='liyilongko@gmail.com',
    url='https://github.com/dragonly/tiflash_mpp_profiler',
    license=read_file('LICENSE'),
    package_dir={'': 'src'},
    packages=[''],
    entry_points={
        'console_scripts': [
            'flashprof = main:cli'
        ]
    },
    install_requires=[
        'graphviz',
        'paramiko',
        'pyyaml'
    ]
)
