from setuptools import setup

def read_file(filename):
    with open(filename, 'r') as fd:
        return fd.read()

setup(
    name='flashprof',
    version='0.0.1',
    description='a tool that collects and visualizes TiFlash runtime infomation',
    author='dragonly',
    author_email='liyilongko@gmail.com',
    url='https://github.com/dragonly/tiflash_mpp_profiler',
    license=read_file('LICENSE'),
    py_modules=['main'],
    entry_points={
        'console_scripts': [
            'flashprof = main:cli'
        ]
    }
)
