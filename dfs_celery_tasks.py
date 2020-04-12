import shutil
import sqlite3
import flask_utilities

from celery import Celery

CELERY_BROKER_URL = 'redis://redis:6379/0'
CELERY_RESULT_BACKEND = 'redis://redis:6379/0'

celery = Celery('tasks',
                broker=CELERY_BROKER_URL,
                backend=CELERY_RESULT_BACKEND)


def update_replication_table(filename, replicated_node):
    sql_stmt = f"""
        INSERT INTO replication_data (filename, replicated_node)
        VALUES ("{filename}", "{replicated_node}");
    """
    conn = sqlite3.connect(flask_utilities.get_db_name())
    with conn:
        conn.execute(sql_stmt)
    conn.close()

@celery.task(name='dfs_tasks.replicate')
def replicate(filename, src_node_addr, dest_node_addr):
    src_node, src_port = src_node_addr.split(':')
    dest_node, dest_port = dest_node_addr.split(':')

    src_filepath = f"/storage_{src_node}_{src_port}/{filename}"
    dest_filepath = f"/storage_{dest_node}_{dest_port}/{filename}"
    shutil.copy(src=src_filepath, dst=dest_filepath)

    update_replication_table(filename, dest_node_addr)

    return f"Successful. {filename} : {src_node_addr} —> {dest_node_addr}"