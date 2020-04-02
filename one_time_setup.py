import configparser
import sqlite3

CONFIG_FILE = 'machines.cfg'


def get_sql_create_master_table():
    return """
        CREATE TABLE master_node (
            filename        VARCHAR(100)    PRIMARY KEY     NOT NULL,
            primary_node    VARCHAR(100)    NOT NULL
        );
    """

def get_sql_create_storage_node_table(sn):
    # 0.0.0.0:5000 -> sn_0_0_0_0__5000
    table_name = get_sn_table_name_from_ip(sn)
    return f"""
        CREATE TABLE {table_name} (
            filename            VARCHAR(100)    PRIMARY KEY     NOT NULL,
            replicated_node     VARCHAR(100)    NOT NULL
        );
    """

def get_sn_table_name_from_ip(ip_addr):
    ip, port = ip_addr.split(':')
    ip = ip.replace('.', '_')
    return f"sn_{ip}__{port}"


def get_db_name():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config['default']['database']

def get_storage_nodes():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    storage_nodes = config['storage_nodes']['machine_list'].split(',\n')
    return storage_nodes

def main():
    db_name = get_db_name()
    conn = sqlite3.connect(db_name)
    print(f"Created database {db_name}.")
    conn.execute(get_sql_create_master_table())
    print("Table created for master node.")
    for sn in get_storage_nodes():
        conn.execute(get_sql_create_storage_node_table(sn))
    print("Table created for all the storage nodes.")
    conn.close()
    print("Setup done!")

if __name__ == "__main__":
    main()
