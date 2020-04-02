"""
Simple server for a distributed file system.
"""

import configparser
import hashlib
import logging
import os
import random
import socket
import socketserver
import sqlite3
import tqdm
import utilities


BUFFER_SIZE = 1024
INTERMEDIATE_FILE_DIR = 'interm'
LOG_DIR = 'logs'
LOG_FILE = 'master.log'
MAX_RETRY_FIND_HEALTHY_SERVER_COUNT = 3
MAX_RETRY_FILE_TRANSFER_TO_SN_COUNT = 3
SCRIPT_NAME = os.path.basename(__file__)
SEPARATOR = "<>"

GET_REQUEST = "<GET_REQUEST>"
PUT_REQUEST = "<PUT_REQUEST>"
STATUS_REQUEST = "<STATUS_REQUEST>"
SERVER_AVAILABLE_CODE = "200"
TRANSFER_SUCCESSFUL_CODE = "TRANSFER_SUCCESSFUL"

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
        info_list = received.split(SEPARATOR)
        request_type = info_list[0]

        if request_type == GET_REQUEST:
            logger.debug("Received get request")
            response_message = self.do_get_handler(info_list[1:])
        elif request_type == PUT_REQUEST:
            logger.debug("Received put request")
            response_message = self.do_put_handler(info_list[1:])
        else:
            response_message = "Request type not supported yet!"

        # send client response
        self.request.sendall(bytes(response_message, "utf-8"))


    def do_put_handler(self, recvd_info_list):
        filename, file_size, file_hash = recvd_info_list
        # remove absolute path if there is
        filename = os.path.basename(filename)
        # convert to integer
        file_size = int(file_size)
        response_message = "Operation failed!"

        try:
            # start receiving the file from the socket
            # and writing to the file stream
            progress = tqdm.tqdm(
                range(file_size),
                f"Receiving {filename}", unit="B",
                unit_scale=True, unit_divisor=1024
            )

            # filename is the primary key; avoid collision
            filename = generate_unique_filename(filename)

            if not os.path.exists(INTERMEDIATE_FILE_DIR):
                os.makedirs(INTERMEDIATE_FILE_DIR)
            inter_filepath = f"{INTERMEDIATE_FILE_DIR}/{filename}"

            logger.debug(f"Initiating file transfer to master from {self.client_address}")
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

            is_file_valid = utilities.is_file_integrity_matched(
                filepath=inter_filepath,
                recvd_hash=file_hash
            )
            if is_file_valid:
                logger.debug(f"{inter_filepath} saved successfully on master. Integrity check passed.")

                retry_count = 0
                while retry_count < MAX_RETRY_FILE_TRANSFER_TO_SN_COUNT:
                    logger.debug(f"Trying to find available storage node. Count: {retry_count + 1}")
                    sn_host, sn_port = select_healthy_server()
                    logger.debug(f"Initiating file tranfer to {sn_host}:{sn_port}")
                    response = self.transfer_file_to_sn(sn_host,
                        sn_port, inter_filepath, file_size, file_hash)
                    logger.debug(f"File transfer response from SN {sn_host}:{sn_port}: {response}")
                    if response == TRANSFER_SUCCESSFUL_CODE:
                        logger.debug(f"File successfully tranferred to {sn_host}:{sn_port}")
                        logger.debug("Updating master table")
                        update_master_table(
                            filename=filename,
                            primary_node=f"{sn_host}:{sn_port}"
                        )
                        logger.debug("Updated master table")
                        break
                    retry_count += 1

                if response == TRANSFER_SUCCESSFUL_CODE:
                    response_message = f"Operation successful: Stored as {filename}"

        except Exception as e:
            logger.error(str(e))
            response_message = str(e)
        return response_message

    def do_get_handler(self, recvd_info_list):
        pass

    def transfer_file_to_sn(self, sn_host, sn_port, filepath, file_size, file_hash):
        # Create a socket (SOCK_STREAM means a TCP socket)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to server and send data
            logger.debug(f"Connecting to storage node {sn_host}:{sn_port}")
            sock.connect((sn_host, int(sn_port)))
            logger.debug("Connected.")

            # SEPARATOR here just to separate the data fields.
            # We can just use send() multiple times, but why simply do that.
            sock.sendall(f"{PUT_REQUEST}{SEPARATOR}{filepath}{SEPARATOR}{file_size}{SEPARATOR}{file_hash}".encode())

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

def select_healthy_server():
    all_storage_nodes = utilities.get_storage_nodes()
    retry_count = 0
    received_response = None
    # try only MAX_RETRY_FIND_HEALTHY_SERVER_COUNT times to get a server
    while retry_count < MAX_RETRY_FIND_HEALTHY_SERVER_COUNT:
        random_server = random.choice(all_storage_nodes)
        sn_host, sn_port = random_server.split(':')

        logger.debug(f"Count {retry_count + 1}. Selected {sn_host}:{sn_port}. Checking health.")

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((sn_host, int(sn_port)))
                sock.sendall(f"{STATUS_REQUEST}{SEPARATOR}".encode())
                received_response = str(sock.recv(1024), "utf-8")
                logger.debug(f"{sn_host}:{sn_port} health check response: {received_response}")

            if received_response == SERVER_AVAILABLE_CODE:
                logger.debug(f"{sn_host}:{sn_port} is healthy. Selected.")
                break
        except Exception as e:
            logger.debug(f"Health check failed: {str(e)}")
        retry_count += 1

    if received_response != SERVER_AVAILABLE_CODE:
        raise Exception("No available servers.")

    return sn_host, sn_port

def update_master_table(filename, primary_node):
    sql_stmt = f"""
        INSERT INTO master_node (filename, primary_node)
        VALUES ("{filename}", "{primary_node}");
    """
    conn = sqlite3.connect(utilities.get_db_name())
    with conn:
        conn.execute(sql_stmt)
    conn.close()

def generate_unique_filename(filename):
    sql_stmt = f"""
        SELECT filename FROM master_node
        WHERE filename="{filename}";
    """
    conn = sqlite3.connect(utilities.get_db_name())
    cur = conn.cursor()
    with conn:
        cur.execute(sql_stmt)
        data = cur.fetchone()
        if data:
            filename, ext = filename.rsplit('.', 1)
            filename = f"{filename}_{utilities.generate_random_str(5)}.{ext}"
    conn.close()
    return filename


def main():
    utilities.setup_logging(log_dir=LOG_DIR, log_file=LOG_FILE)
    HOST, PORT = utilities.get_master_host_port()

    server = None
    try:
        # Create the server, binding to HOST on PORT
        with socketserver.ForkingTCPServer(
                (HOST, PORT), DistributedFSHandler) as server:
            # Activate the server; this will keep running until you
            # interrupt the program with Ctrl-C
            logger.info("Starting server")
            server.serve_forever()
    except Exception as e:
        logger.exception(str(e))
        if server:
            server.shutdown()


if __name__ == "__main__":
    main()
