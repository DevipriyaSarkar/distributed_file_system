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


BUFFER_SIZE = 1024
CONFIG_FILE = 'machines.cfg'
DEFAULT_SERVER_HOST = 'localhost'
DEFAULT_SERVER_PORT = 9999
LOG_DIR = 'logs'
LOG_FILE = 'client.log'
SCRIPT_NAME = os.path.basename(__file__)
SEPARATOR = "<>"

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

def setup_logging():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logging.basicConfig(
        filename='{folder}/{file}'.format(folder=LOG_DIR, file=LOG_FILE),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.DEBUG
    )

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # add ch to logger
    logger.addHandler(ch)


def get_server_host_port():
    # Read host, port to run the server on from config file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    master_config = config['master']
    HOST = master_config.get('server_ip', DEFAULT_SERVER_HOST)
    PORT = master_config.getint('server_port', DEFAULT_SERVER_PORT)
    return (HOST, PORT)

def check_filepath_sanity(filepath):
    if not os.path.isfile(filepath):
            raise Exception("File not valid.")

def calc_file_md5(filepath):
    md5_hash = hashlib.md5()
    with open(filepath, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()

def send_data_to_server(host, port, filepath):
    # Create a socket (SOCK_STREAM means a TCP socket)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Connect to server and send data
        logger.debug("Connecting to {host}:{port}".format(host=host, port=port))
        sock.connect((host, port))
        logger.debug("Connected.")
        file_size = os.path.getsize(filepath)   # for the progress bar
        file_hash = calc_file_md5(filepath)     # for integrity
        
        # SEPARATOR here just to separate the data fields.
        # We can just use send() thrice, but why simply do that.
        sock.sendall(f"{filepath}{SEPARATOR}{file_size}{SEPARATOR}{file_hash}".encode())

        progress = tqdm.tqdm(
            range(file_size),
            f"Sending {filepath}", unit="B",
            unit_scale=True, unit_divisor=1024
        )

        with open(filepath, "rb") as f:
            for _ in progress:
                # read the bytes from the file
                bytes_read = f.read(BUFFER_SIZE)
                if not bytes_read:
                    # file transmitting is done
                    break
                sock.sendall(bytes_read)
                # update the progress bar
                progress.update(len(bytes_read))

        # Receive data from the server and shut down
        received_response = str(sock.recv(1024), "utf-8")
        return received_response


def main():
    setup_logging()
    args = parse_cmd_args()
    HOST, PORT = get_server_host_port()
    if hasattr(args, 'get_filename') and hasattr(args, 'put_filepath'):
        logging.error("Incorrect usage")
        parser.print_help()
        return
    if hasattr(args, 'get_filename'):
        check_filepath_sanity(args.get_filename)
        pass
    if hasattr(args, 'put_filepath'):
        check_filepath_sanity(args.put_filepath)
        response = send_data_to_server(host=HOST, port=PORT, filepath=args.put_filepath)
        logger.info("DATA RECEIVED: {response}".format(response=response))

if __name__ == "__main__":
    main()
