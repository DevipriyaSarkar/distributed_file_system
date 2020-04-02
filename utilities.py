import configparser
import hashlib
import logging
import os
import random
import string

CONFIG_FILE = 'machines.cfg'


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

def get_storage_nodes():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    storage_nodes = config['storage_nodes']['machine_list'].split(',\n')
    return storage_nodes

def setup_logging(log_dir, log_file):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.basicConfig(
        filename='{folder}/{file}'.format(folder=log_dir, file=log_file),
        format='%(asctime)s : %(levelname)s : %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.DEBUG
    )

def get_master_host_port():
    # Read host, port to run the server on from config file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    master_config = config['master']
    host = master_config.get('server_ip')
    port = master_config.getint('server_port')
    return (host, int(port))

def get_sn_host_port(sn_num):
    # Read host, port to run the server on from config file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    machine_list = config['storage_nodes']['machine_list'].split(',\n')
    host, port = machine_list[sn_num].split(':')
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
