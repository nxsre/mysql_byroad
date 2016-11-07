#!/usr/bin/env python
#coding:utf-8


import argparse
from string import Template


config_template = """#byroad dispatcher supervisor config
[program:byroad-dispatcher-$dispatcher]
directory=/home/www/byroad2/build
command=/home/www/byroad2/build/byroad-dispatcher -c conf/$dispatcher.toml
startretries=9999
stdout_logfile=/home/www/byroad2/$dispatcher.log
stderr_logfile=/home/www/byroad2/$dispatcher.err
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser("byroad dispatcher supervisor config generator")
    parser.add_argument("--dispatcher", default="dispatcher")
    args = parser.parse_args()
    s = Template(config_template)
    print s.substitute(dispatcher=args.dispatcher)
