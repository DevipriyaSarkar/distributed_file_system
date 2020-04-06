import configparser
import hashlib
import logging
import os
import random
import string
import tqdm

CONFIG_FILE = 'machines.cfg'
BUFFER_SIZE = 4096
GET_REQUEST = "<GET_REQUEST>"
PUT_REQUEST = "<PUT_REQUEST>"
NOTIFY_SUCCESS = "<NOTIFY_SUCCESS>"
NOTIFY_FAILURE = "<NOTIFY_FAILURE>"
SEPARATOR = "<>"


def get_sn_table_name_from_ip(ip_addr):
    # 0.0.0.0:5000 -> sn__0_0_0_0__5000
    ip, port = ip_addr.split(':')
    ip = ip.replace('.', '_')
    return f"sn__{ip}__{port}"

def get_ip_from_sn_table_name(table_name):
    # sn__0_0_0_0__5000 -> 0.0.0.0:5000
    ip_addr = table_name.split('__')[1:]
    return f"{ip_addr[0]}:{ip_addr[1]}"

def get_db_name():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config['default']['database']

def get_all_storage_nodes():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    storage_nodes = config['storage_nodes']['machine_list_docker'].split(',\n')
    return storage_nodes

def get_sn_node_port(sn_num):
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    storage_nodes = config['storage_nodes']['machine_list_docker'].split(',\n')
    node, port = storage_nodes[sn_num].split(':')
    return (node, int(port))

def setup_logging(log_dir, log_file, is_print_on_console=False, logger=None):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.basicConfig(
        filename='{folder}/{file}'.format(folder=log_dir, file=log_file),
        format='%(asctime)s : %(levelname)s : %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.DEBUG
    )

    if is_print_on_console:
        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # add ch to logger
        logger.addHandler(ch)

def get_master_host_port():
    # Read host, port to run the server on from config file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    master_config = config['master']
    host = master_config.get('server_ip')
    port = master_config.getint('server_port')
    return (host, int(port))

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

def generate_random_str(str_len):
    allowed_chars = string.ascii_letters + string.digits
    return ''.join(random.choice(allowed_chars) for i in range(str_len))

def check_filepath_sanity(filepath):
    if not os.path.isfile(filepath):
            raise Exception("File not valid.")

def receive_file(sock, dest_filepath, logger):
    response_message = (NOTIFY_FAILURE, "Operation failed!")

    filename = os.path.basename(dest_filepath)

    req_str = f"{GET_REQUEST}{SEPARATOR}{filename}"
    logger.debug(f"Sending request to {sock.getpeername()}: {req_str}")
    sock.sendall(req_str.encode())

    file_info_recvd = sock.recv(BUFFER_SIZE).decode()
    file_info_recvd = file_info_recvd.split(SEPARATOR)
    logger.debug(f"Response received from {sock.getpeername()}: {file_info_recvd}")
    response_type = file_info_recvd[0]

    if response_type == NOTIFY_FAILURE:
        response_message = (NOTIFY_FAILURE, file_info_recvd[1])
    elif response_type == PUT_REQUEST:
        logger.debug(f"Received put request for {filename}")
        filename = file_info_recvd[1]
        file_size = int(file_info_recvd[2])
        file_hash = file_info_recvd[3]

        try:
            is_received_file = receive_file_from_sock(sock, dest_filepath,
                file_size, file_hash, logger)
            if is_received_file:
                logging.debug(f"File {dest_filepath} received. Going for integrity check.")
                is_file_valid = is_file_integrity_matched(
                    filepath=dest_filepath,
                    recvd_hash=file_hash
                )
                if is_file_valid:
                    msg = f"{dest_filepath} saved successfully on {sock.getsockname()}. Integrity check passed."
                    response_message = (NOTIFY_SUCCESS, msg)
                    logger.debug(msg)
            else:
                response_message = (NOTIFY_FAILURE, "File transfer failed!")
        except Exception as e:
            response_message = (NOTIFY_FAILURE, str(e))
            return response_message
    else:
        msg = "Operation not supported"
        response_message = (NOTIFY_FAILURE, msg)

    logging.debug(f"Returning response from {sock.getsockname()} to {sock.getpeername()}: {response_message}")
    return response_message

def receive_file_from_sock(sock, dest_filepath, file_size, file_hash, logger):
    filename = os.path.basename(dest_filepath)
    try:
        # start receiving the file from the socket
        # and writing to the file stream
        progress = tqdm.tqdm(
            range(file_size),
            f"Receiving {filename}", unit="B",
            unit_scale=True, unit_divisor=1024
        )

        logger.debug(f"Initiating bare file transfer from {sock.getpeername()} to {sock.getsockname()}")
        total_bytes_read = 0

        storage_dir = os.path.dirname(dest_filepath)
        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir)

        with open(dest_filepath, "wb") as f:
            for _ in progress:
                # read 1024 bytes from the socket (receive)
                bytes_read = sock.recv(BUFFER_SIZE)
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
                    logger.debug("Received entire file.")
                    break
    except Exception as e:
        raise e
    logger.error(f"Total read: {total_bytes_read} \t File size: {file_size}")
    if total_bytes_read == file_size:
        return True
    return False


def send_file(sock, src_filepath, logger, want_server_response=False):
    response = (NOTIFY_FAILURE, "Operation failed!")

    filename = os.path.basename(src_filepath)
    file_size = os.path.getsize(src_filepath)   # for the progress bar
    file_hash = calc_file_md5(src_filepath)     # for integrity

    # SEPARATOR here just to separate the data fields.
    # We can just use send() multiple times, but why simply do that
    req_str = f"{PUT_REQUEST}{SEPARATOR}{filename}{SEPARATOR}{file_size}{SEPARATOR}{file_hash}"
    logger.debug(f"Sending request from {sock.getsockname()} to {sock.getpeername()}: {req_str}")
    sock.sendall(req_str.encode())

    try:
        is_sent_file = send_file_to_sock(
            sock=sock, src_filepath=src_filepath, file_size=file_size, logger=logger
        )
        if is_sent_file:
            response = (NOTIFY_SUCCESS, f"File {src_filepath} sent.")
    except Exception as e:
        response = (NOTIFY_FAILURE, str(e))
        logger.debug(f"Exception: {response}")

    if want_server_response:
        logger.debug(f"Waiting for response from {sock.getpeername()}")
        # Receive data from the server and shut down — (status_code, msg)
        response = sock.recv(BUFFER_SIZE).decode()
        logger.debug(f"Received response from {sock.getpeername()}: {response}")

    logging.debug(f"Returning response from {sock.getsockname()} to {sock.getpeername()}: {response}")
    return response


def send_file_to_sock(sock, src_filepath, file_size, logger):
    logger.debug(f"Initiating bare file transfer from {sock.getsockname()} to {sock.getpeername()}")
    try:
        progress = tqdm.tqdm(
            range(file_size),
            f"Sending {src_filepath}", unit="B",
            unit_scale=True, unit_divisor=1024
        )

        total_bytes_read = 0
        with open(src_filepath, "rb") as f:
            for _ in progress:
                # read the bytes from the file
                bytes_read = f.read(BUFFER_SIZE)
                total_bytes_read += len(bytes_read)
                if not bytes_read:
                    # file transmitting is done
                    break
                sock.sendall(bytes_read)
                # update the progress bar
                progress.update(len(bytes_read))
                if total_bytes_read == file_size:
                    logger.debug("Sent entire file.")
                    break
    except Exception as e:
        raise e
    if total_bytes_read == file_size:
        return True
    return False