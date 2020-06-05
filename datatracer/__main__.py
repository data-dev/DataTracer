import argparse
import json
import logging
import sys

import hug
import yaml

from datatracer import api

API_CONFIG_KEYS = (
    'host',
    'port',
    'primary_key_solver',
    'foreign_key_solver',
    'column_map_solver',
    'column_map_threshold',
)


def print_box(message, strong=False):
    char = '#' if strong else '*'
    line = char * max(len(line) for line in message.split('\n'))
    print('\n'.join((line, message, line)))


def _api_serve(args):
    if args.config:
        with open(args.config) as config_file:
            if args.config.endswith('yaml') or args.config.endswith('yml'):
                config = yaml.safe_load(config_file)
            else:
                config = json.load(config_file)

            for key in config:
                if key not in API_CONFIG_KEYS:
                    raise ValueError('Unknown option `{}` in {}'.format(key, args.config))

        api.PRIMARY_KEY_SOLVER = config.get('primary_key_solver', api.PRIMARY_KEY_SOLVER)
        api.FOREIGN_KEY_SOLVER = config.get('foreign_key_solver', api.FOREIGN_KEY_SOLVER)
        api.COLUMN_MAP_SOLVER = config.get('column_map_solver', api.COLUMN_MAP_SOLVER)
        api.COLUMN_MAP_THRESHOLD = config.get('column_map_threshold', api.COLUMN_MAP_THRESHOLD)

        host = config.get('host', args.host)
        port = config.get('port', args.port)
    else:
        host = args.host
        port = args.port

    print_box('Starting the DataTracer REST API', True)
    hug.API(api).http.serve(host, port, display_intro=False)


def _get_parser():
    # Logging
    logging_args = argparse.ArgumentParser(add_help=False)
    logging_args.add_argument('-v', '--verbose', action='count', default=0)

    parser = argparse.ArgumentParser(
        prog='datatracer',
        description='DataTracer Command Line Interface',
        parents=[logging_args]
    )

    subparsers = parser.add_subparsers(title='command', help='Command to execute')
    parser.set_defaults(command=None)

    api_parser = subparsers.add_parser(
        'api',
        parents=[logging_args],
        help='Start a development server for the DataTracer REST API.'
    )
    api_parser.set_defaults(command=_api_serve)
    api_parser.add_argument(
        '--host', default='127.0.0.1',
        help='IP at which to listen to. Defaults to 127.0.0.1. Use 0.0.0.0 for all interfaces.'
    )
    api_parser.add_argument(
        '-p', '--port', default=8000, type=int,
        help='Port at which to listen to. Defaults to 8000.'
    )
    api_parser.add_argument(
        '-c', '--config',
        help='Path to a configuration JSON or YAML file.'
    )

    return parser


def main():
    # Parse args
    parser = _get_parser()
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    # Logger setup
    log_level = (3 - args.verbose) * 10
    fmt = '%(asctime)s - %(process)d - %(levelname)s - %(name)s - %(module)s - %(message)s'
    logging.basicConfig(level=log_level, format=fmt)

    args.command(args)


if __name__ == '__main__':
    main()
