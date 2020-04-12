import argparse
import requests
import configparser
import hashlib
import json
import os
import random
import string

CONFIG_FILE = 'dfs.cfg'
HEALTH_CHECK_ENDPOINT = "http://{node_ip}/health"
HEALTH_CHECK_TIMEOUT = 10
MAX_RETRY_FIND_HEALTHY_SERVER_COUNT = 3

def get_all_storage_nodes():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    storage_nodes = config['storage_nodes']['machine_list_docker'].split(',\n')
    return storage_nodes

def get_replication_factor():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config['default'].getint('replication_factor')

def get_master_endpoint():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    master_config = config['master']
    return master_config['server_endpoint']

def get_db_name():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config['default']['database']

def generate_random_str(str_len):
    allowed_chars = string.ascii_letters + string.digits
    return ''.join(random.choice(allowed_chars) for i in range(str_len))

def is_sn_healthy(sn_ip):
    health_check_url = HEALTH_CHECK_ENDPOINT.format(node_ip=sn_ip)
    try:
        resp = requests.get(url=health_check_url, timeout=HEALTH_CHECK_TIMEOUT)
        if resp.status_code == requests.codes.ok:
            return True
    except Exception as e:
        pass
    return False

def select_healthy_sn(exclude_sns=None):
    from flask import current_app
    all_storage_nodes = get_all_storage_nodes()
    available_sns = list(set(all_storage_nodes) - set(exclude_sns))
    retry_count = 0

    # try only MAX_RETRY_FIND_HEALTHY_SERVER_COUNT times to get a server
    while retry_count < MAX_RETRY_FIND_HEALTHY_SERVER_COUNT:
        random_server = random.choice(available_sns)
        available_sns.remove(random_server)

        current_app.logger.debug(f"Count {retry_count + 1}. Selected {random_server}. Checking health.")

        if is_sn_healthy(random_server):
            return random_server
        retry_count += 1

    raise Exception("No available storage nodes.")

def parse_cmd_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--node', help="Docker container name")
    parser.add_argument('--port', type=int, help="Port on which to run")

    args = parser.parse_args()
    return args

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

def check_filepath_sanity(filepath):
    if not os.path.isfile(filepath):
            raise Exception("File not valid.")

def create_storage_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)