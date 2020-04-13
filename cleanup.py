import argparse
import os
import shutil
import sqlite3
import flask_utilities

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
CLIENT_RECIEVED_FILES_DIR = "received_files"
LOGS_DIR = "logs"
SCRIPT_NAME = os.path.basename(__file__)
STORAGE_DIR = 'storage_{NODE}_{PORT}'
DB_FILE = "dfs.db"

def parse_cmd_args():
    parser = argparse.ArgumentParser(prog=SCRIPT_NAME)
    parser.add_argument("--all", help="Clean logs, storage files and db entries",
                        action="store_true")
    parser.add_argument("--flush-logs", help="Flush all logs",
                        action="store_true")
    args = parser.parse_args()
    return args

def silent_dir_delete(dir_path):
    print(f"Deleting dir: {dir_path}")
    shutil.rmtree(dir_path, ignore_errors=True)

def delete_logs():
    logs_path = os.path.join(PROJECT_ROOT, LOGS_DIR)
    silent_dir_delete(logs_path)

def get_all_table_names():
    table_names = []
    sql_stmt = """
        SELECT name FROM sqlite_master WHERE type='table';
    """
    conn = sqlite3.connect(flask_utilities.get_db_name())
    cur = conn.cursor()
    with conn:
        cur.execute(sql_stmt)
        data = cur.fetchall() # -> [('master_node',), ('sn__sn1__5000',), ...]
        table_names = [row[0] for row in data] # -> ['master_node', 'sn__sn1__5000', ...]
    conn.close()
    return table_names

def delete_from_all_tables():
    table_names = get_all_table_names()
    conn = sqlite3.connect(flask_utilities.get_db_name())
    cur = conn.cursor()
    with conn:
        for table in table_names:
            print(f"Deleting table: {table}")
            cur.execute(f"DELETE FROM {table};")
    conn.close()

def clean_all_sn_files():
    for sn in flask_utilities.get_all_storage_nodes():
        sn_node, sn_port = sn.split(':')
        sn_local_filepath = STORAGE_DIR.format(NODE=sn_node, PORT=sn_port)
        abs_sn_fs_dir_path = os.path.join(PROJECT_ROOT, sn_local_filepath)
        silent_dir_delete(abs_sn_fs_dir_path)

def clean_received_files():
    dir_path = os.path.join(PROJECT_ROOT, CLIENT_RECIEVED_FILES_DIR)
    silent_dir_delete(dir_path)

def clean_db_fs():
    delete_from_all_tables()
    clean_all_sn_files()
    clean_received_files()

def flush_logs():
    log_dir_path = os.path.join(PROJECT_ROOT, LOGS_DIR)
    log_files = [os.path.join(log_dir_path, f) for f in os.listdir(log_dir_path)
                    if os.path.isfile(os.path.join(log_dir_path, f))]
    for log_file_path in log_files:
        print(f"Flushing file: {log_file_path}")
        with open(log_file_path, "r+") as fp:
            fp.truncate(0)

def main():
    args = parse_cmd_args()
    if args.all:
        delete_logs()
        clean_db_fs()
    elif args.flush_logs:
        flush_logs()


if __name__ == "__main__":
    main()