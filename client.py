"""
Simple client for a distributed file system.
Supports requesting storage (get) or retrieval (put) of a file
to the server in the distributed file system.
"""

import argparse
import configparser
import hashlib
import logging
import os
import socket
import sys
import tqdm
import utilities


BUFFER_SIZE = 4096
DEFAULT_SERVER_HOST = 'localhost'
DEFAULT_SERVER_PORT = 9999
LOG_DIR = 'logs'
LOG_FILE = 'client.log'
SCRIPT_NAME = os.path.basename(__file__)
SEPARATOR = "<>"
STORAGE_DIR="received_files"
GET_REQUEST = "<GET_REQUEST>"
PUT_REQUEST = "<PUT_REQUEST>"
NOTIFY_SUCCESS = "<NOTIFY_SUCCESS>"
NOTIFY_FAILURE = "<NOTIFY_FAILURE>"

logger = logging.getLogger(SCRIPT_NAME)


def parse_cmd_args():
    parser = argparse.ArgumentParser(prog=SCRIPT_NAME)
    parser.add_argument("--verbose", help="increase output verbosity",
                        action="store_true")

    subparsers = parser.add_subparsers(help='file get/put help', required=True)

    # create the parser for the "get" sub-command
    parser_get = subparsers.add_parser('get', help='get help')
    parser_get.add_argument('get_filename', help="filepath to retrieve")

    # create the parser for the "put" sub-command
    parser_put = subparsers.add_parser('put', help='put help')
    parser_put.add_argument('put_filepath', help="filepath to store")

    return parser.parse_args()

def put_file_at_server(host, port, filepath):
    # Create a socket (SOCK_STREAM means a TCP socket)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Connect to server and send data
        logger.debug("Connecting to {host}:{port}".format(host=host, port=port))
        sock.connect((host, int(port)))
        logger.debug("Connected.")
        response_message = utilities.send_file(
            sock=sock,
            src_filepath=filepath,
            logger=logger,
            want_server_response=True
        )
    return response_message

def request_file_from_server(host, port, filename):
    try:
        # Create a socket (SOCK_STREAM means a TCP socket)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to server and send data
            logger.debug("Connecting to {host}:{port}".format(host=host, port=port))
            sock.connect((host, int(port)))
            logger.debug("Connected")
            response_type, response_message = utilities.receive_file(
                sock=sock,
                dest_filepath=f"{STORAGE_DIR}/{filename}",
                logger=logger
            )
    except Exception as e:
        response_type, response_message = (NOTIFY_FAILURE, str(e))

    return response_message


def main():
    utilities.setup_logging(log_dir=LOG_DIR,
        log_file=LOG_FILE,
        is_print_on_console=True,
        logger=logger
    )
    args = parse_cmd_args()
    HOST, PORT = utilities.get_master_host_port()
    if hasattr(args, 'get_filename') and hasattr(args, 'put_filepath'):
        logging.error("Incorrect usage")
        parser.print_help()
        return
    if hasattr(args, 'get_filename'):
        response = request_file_from_server(host=HOST, port=PORT, filename=args.get_filename)
        logger.info("DATA RECEIVED: {response}".format(response=response))
    if hasattr(args, 'put_filepath'):
        utilities.check_filepath_sanity(args.put_filepath)
        response = put_file_at_server(host=HOST, port=PORT, filepath=args.put_filepath)
        logger.info("DATA RECEIVED: {response}".format(response=response))

if __name__ == "__main__":
    main()
