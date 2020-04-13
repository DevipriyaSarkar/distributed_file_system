import os
import random

import flask_utilities
from celery import Celery
from flask import Flask, jsonify, request, send_from_directory

MY_NODE = os.environ['NODE']
MY_PORT = os.environ['PORT']
STORAGE_DIR = f"storage_{MY_NODE}_{MY_PORT}"
CELERY_BROKER_URL = 'redis://redis:6379/0'
CELERY_RESULT_BACKEND = 'redis://redis:6379/0'

app = Flask(__name__)
celery = Celery('tasks',
                broker=CELERY_BROKER_URL,
                backend=CELERY_RESULT_BACKEND)


@app.route('/health', methods=['GET'])
def health():
    data = {
        'message': 'Server is running'
    }
    resp = jsonify(data)
    resp.status_code = 200
    return resp


@app.route('/upload', methods=['POST'])
def upload():
    try:
        flask_utilities.create_storage_dir(STORAGE_DIR)
        fp = request.files['input_file']
        file_hash = request.form['file_hash']
        filename = request.form['filename']
        storage_filepath = os.path.join(STORAGE_DIR, filename)
        fp.save(storage_filepath)
        is_file_valid = flask_utilities.is_file_integrity_matched(
            filepath=storage_filepath,
            recvd_hash=file_hash
        )
        if is_file_valid:
            app.logger.debug(f"File {storage_filepath} saved and integrity verified.")
            resp = jsonify({'message': f"File {storage_filepath} saved successfully."})
            resp.status_code = 200
            add_replication_to_queue(filename)
    except Exception as e:
        resp = jsonify({
            'message': f"Error while saving {storage_filepath}: {str(e)}"
        })
        resp.status_code = 500
    return resp

@app.route('/download', methods=['GET'])
def download():
    try:
        flask_utilities.create_storage_dir(STORAGE_DIR)
        filename = request.args['filename']
        storage_filepath = os.path.join(STORAGE_DIR, filename)
        resp = send_from_directory(
            directory=STORAGE_DIR,
            filename=filename
        )
        file_hash = flask_utilities.calc_file_md5(filepath=storage_filepath)
        resp.headers['file_hash'] = file_hash
    except Exception as e:
        resp = jsonify({
            'message': f"Error while saving {storage_filepath}: {str(e)}"
        })
        resp.status_code = 500
    return resp

def add_replication_to_queue(filename):
    replication_factor = flask_utilities.get_replication_factor()
    all_storage_nodes = flask_utilities.get_all_storage_nodes()
    cur_storage_node = f"{MY_NODE}:{MY_PORT}"
    available_sns = list(set(all_storage_nodes) - {cur_storage_node})
    selected_sns = random.sample(available_sns, k=replication_factor)
    app.logger.debug(f"Asynchronously replicate {filename} from {cur_storage_node} to {selected_sns}")
    for count, dest_sn in enumerate(selected_sns):
        task = celery.send_task(
            'dfs_tasks.replicate',
            args=[filename, cur_storage_node, dest_sn]
        )
        app.logger.debug(f"#{count+1}: Added task {task.id} for replication of {filename} to {dest_sn}")

if __name__ == '__main__':
    flask_utilities.create_storage_dir(dir_path=STORAGE_DIR)
    app.config['NODE'] = MY_NODE
    app.config['PORT'] = MY_PORT
    app.config['STORAGE_DIR'] = f"storage_{app.config['NODE']}_{app.config['PORT']}"
    app.run(host="0.0.0.0", port=MY_PORT)
