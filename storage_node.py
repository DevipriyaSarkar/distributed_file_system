"""
Simple storage node for a distributed file system.
"""

import configparser
import hashlib
import logging
import os
import socketserver
import sys
import tqdm
import utilities


HOST = "0.0.0.0"
BUFFER_SIZE = 4096
LOG_DIR = 'logs'
SCRIPT_NAME = os.path.basename(__file__)
SEPARATOR = "<>"
NOTIFY_SUCCESS = "<NOTIFY_SUCCESS>"
NOTIFY_FAILURE = "<NOTIFY_FAILURE>"

GET_REQUEST = "<GET_REQUEST>"
PUT_REQUEST = "<PUT_REQUEST>"
STATUS_REQUEST = "<STATUS_REQUEST>"
SERVER_AVAILABLE_CODE = "200"
TRANSFER_SUCCESSFUL_CODE = "TRANSFER_SUCCESSFUL"

logger = logging.getLogger(SCRIPT_NAME)


class DistributedNodeHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def setup(self):
        self.HOST, self.PORT = self.server.server_address
        self.STORAGE_DIR = f'storage_{self.HOST}_{self.PORT}'
        self.LOG_FILE = f'node_{self.HOST}_{self.PORT}.log'

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
        elif request_type == STATUS_REQUEST:
            logger.debug("Received status request")
            response_message = SERVER_AVAILABLE_CODE
        else:
            response_message = "Request type not supported yet!"

        logger.debug(f"Sending response to master: {str(response_message)}")
        # send client response
        self.request.sendall(bytes(str(response_message), "utf-8"))


    def do_put_handler(self, recvd_info_list):
        response_message = (NOTIFY_FAILURE, "File transfer to storage node failed!")
        filename, file_size, file_hash = recvd_info_list
        # remove absolute path if there is
        filename = os.path.basename(filename)
        # convert to integer
        file_size = int(file_size)

        storage_filepath = f"{self.STORAGE_DIR}/{filename}"

        try:
            logger.debug(f"Receiving {filename} from {self.client_address}")
            is_received_file = utilities.receive_file_from_sock(
                sock=self.request,
                dest_filepath=storage_filepath,
                file_size=file_size,
                file_hash=file_hash,
                logger=logger
            )
            if not is_received_file:
                msg = f"Receiving file from {self.client_address} failed."
                response_message = (NOTIFY_FAILURE, msg)
                return response_message

            logger.debug(f"{filename} received. Checking integrity.")
            is_file_valid = utilities.is_file_integrity_matched(
                filepath=storage_filepath,
                recvd_hash=file_hash
            )
            if is_file_valid:
                msg = f"{storage_filepath} saved successfully. Integrity check passed."
                logger.debug(msg)
                response_message = (NOTIFY_SUCCESS, msg)
        except Exception as e:
            logger.error(str(e))
            response_message = (NOTIFY_FAILURE, str(e))
        return response_message

    def do_get_handler(self, recvd_info_list):
        response_message = (NOTIFY_FAILURE, "File transfer from storage node failed!")
        filename = recvd_info_list[0]
        # remove absolute path if there is
        filename = os.path.basename(filename)
        storage_filepath = f"{self.STORAGE_DIR}/{filename}"
        file_size = int(os.path.getsize(storage_filepath))
        file_hash = utilities.calc_file_md5(storage_filepath)

        req_str = f"{PUT_REQUEST}{SEPARATOR}{filename}{SEPARATOR}{file_size}{SEPARATOR}{file_hash}"
        logger.debug(f"Sending request from {self.server.server_address} to {self.client_address}: {req_str}")
        self.request.sendall(req_str.encode())

        try:
            logger.debug(f"Sending {filename} to {self.client_address}")
            is_sent_file = utilities.send_file_to_sock(
                sock=self.request,
                src_filepath=storage_filepath,
                file_size=file_size,
                logger=logger
            )
            if is_sent_file:
                response_message = (NOTIFY_SUCCESS, f"File {storage_filepath} sent.")
            logger.debug(f"Response: {response_message}")
        except Exception as e:
            logger.error(str(e))
            response_message = (NOTIFY_FAILURE, str(e))
        return response_message


def main():
    NODE, PORT = utilities.get_sn_node_port(sn_num=int(sys.argv[1]))
    LOG_FILE = f'node_{NODE}_{PORT}.log'
    utilities.setup_logging(log_dir=LOG_DIR, log_file=LOG_FILE)

    server = None
    try:
        # Create the server, binding to HOST on PORT
        with socketserver.ForkingTCPServer(
                (HOST, PORT), DistributedNodeHandler) as server:
            # Activate the server; this will keep running until you
            # interrupt the program with Ctrl-C
            logger.info(f"Starting server on {HOST}:{PORT}")
            server.serve_forever()
    except Exception as e:
        logger.exception(str(e))
        if server:
            server.shutdown()


if __name__ == "__main__":
    main()
