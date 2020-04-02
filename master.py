"""
Simple server for a distributed file system.
"""

import configparser
import hashlib
import logging
import os
import socketserver
import tqdm


BUFFER_SIZE = 1024
CONFIG_FILE = 'machines.cfg'
DEFAULT_SERVER_HOST = 'localhost'
DEFAULT_SERVER_PORT = 9999
INTERMEDIATE_FILE_DIR = 'interm'
LOG_DIR = 'logs'
LOG_FILE = 'master.log'
SCRIPT_NAME = os.path.basename(__file__)
SEPARATOR = "<>"

logger = logging.getLogger(SCRIPT_NAME)


class DistributedFSHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        response_message = "Operation failed."
        # self.request is the TCP socket connected to the client
        received = self.request.recv(BUFFER_SIZE).decode()
        filename, file_size, file_hash = received.split(SEPARATOR)
        # remove absolute path if there is
        filename = os.path.basename(filename)
        # convert to integer
        file_size = int(file_size)

        try:
            # start receiving the file from the socket
            # and writing to the file stream
            progress = tqdm.tqdm(
                range(file_size),
                f"Receiving {filename}", unit="B",
                unit_scale=True, unit_divisor=1024
            )

            if not os.path.exists(INTERMEDIATE_FILE_DIR):
                os.makedirs(INTERMEDIATE_FILE_DIR)
            inter_filepath = f"{INTERMEDIATE_FILE_DIR}/{filename}"

            total_bytes_read = 0
            with open(inter_filepath, "wb") as f:
                for _ in progress:
                    # read 1024 bytes from the socket (receive)
                    bytes_read = self.request.recv(BUFFER_SIZE)
                    if not bytes_read:    
                        # nothing is received
                        # file transmitting is done
                        break
                    total_bytes_read += len(bytes_read)
                    # write to the file the bytes we just received
                    f.write(bytes_read)
                    # update the progress bar
                    progress.update(len(bytes_read))
                    # done reading the entire file
                    if total_bytes_read == file_size:
                        break

            is_file_valid = is_file_integrity_matched(inter_filepath, file_hash)
            if is_file_valid:
                response_message = "Operation successful."
        except Exception as e:
            response_message = str(e)
        # send client response
        self.request.sendall(bytes(response_message + "\n", "utf-8"))


def is_file_integrity_matched(filepath, recvd_hash):
    new_hash = calc_file_md5(filepath)
    if new_hash != recvd_hash:
        raise Exception("File integrity check failed!")
    return True

def calc_file_md5(filepath):
    md5_hash = hashlib.md5()
    with open(filepath, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()

def setup_logging():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logging.basicConfig(
        filename='{folder}/{file}'.format(folder=LOG_DIR, file=LOG_FILE),
        format='%(asctime)s : %(levelname)s : %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.DEBUG
    )

def get_server_host_port():
    # Read host, port to run the server on from config file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    master_config = config['master']
    HOST = master_config.get('server_ip', DEFAULT_SERVER_HOST)
    PORT = master_config.getint('server_port', DEFAULT_SERVER_PORT)
    return (HOST, PORT)


def main():
    setup_logging()
    HOST, PORT = get_server_host_port()

    try:
        # Create the server, binding to HOST on PORT
        #import pdb; pdb.set_trace()
        with socketserver.ForkingTCPServer(
                (HOST, PORT), DistributedFSHandler) as server:
            # Activate the server; this will keep running until you
            # interrupt the program with Ctrl-C
            logger.info("Starting server")
            server.serve_forever()
    except Exception as e:
        logger.exception(str(e))


if __name__ == "__main__":
    main()
