import os
import sqlite3

import flask_utilities
import requests
from flask import Flask, Response, jsonify, request, stream_with_context

MY_NODE = os.environ['NODE']
MY_PORT = os.environ['PORT']
FILE_DOWNLOAD_ENDPOINT = "http://{node_ip}/download"
FILE_UPLOAD_ENDPOINT = "http://{node_ip}/upload"
MAX_RETRY_FILE_SAVE_TO_SN_COUNT = 3

app = Flask(__name__)


# test URL
@app.route('/test')
def test():
    return 'Master is working!'


@app.route('/upload', methods=['POST'])
def upload():
    try:
        fp = request.files['input_file']
        file_hash = request.form['file_hash']
        filename = fp.filename
        # filename is the primary key; avoid collision
        filename = generate_unique_filename(filename)
        data = {
            'file_hash': file_hash,
            'filename': filename
        }

        resp_code = 500
        resp_msg = "Something went wrong while processing."
        retry_count = 0

        exclude_sns = []
        while retry_count < MAX_RETRY_FILE_SAVE_TO_SN_COUNT:
            sn_node = flask_utilities.select_healthy_sn(exclude_sns=exclude_sns)
            app.logger.debug(f"Attempt {retry_count+1}: Trying to store {filename} to SN {sn_node}.")
            file_upload_url = FILE_UPLOAD_ENDPOINT.format(node_ip=sn_node)
            files = {'input_file': fp}
            resp = requests.post(url=file_upload_url, files=files, data=data)
            resp_code = resp.status_code
            resp_msg = resp.json()['message']
            if resp_code == requests.codes.ok:
                app.logger.info(f"{filename} saved to {sn_node}")
                app.logger.debug("Updating master table")
                update_master_table(
                    filename=filename,
                    primary_node=f"{sn_node}"
                )
                app.logger.debug("Updated master table")
                break
            retry_count += 1
            exclude_sns.append(sn_code)

        if resp_code != requests.codes.ok:
            if not resp_code:   resp_code = 500
            if not resp_msg:    resp_msg = f"{MAX_RETRY_FILE_SAVE_TO_SN_COUNT} attempts to save file to SN failed."
    except Exception as e:
        resp_code = 500
        resp_msg = str(e)

    resp = jsonify({'message': resp_msg})
    resp.status_code = resp_code
    return resp

@app.route('/download', methods=['GET'])
def download():
    try:
        filename = request.args['filename']
        filename = os.path.basename(filename)
        payload = {'filename': filename}

        resp_code = 500
        resp_msg = "Something went wrong while processing."

        pnode = return_pnode_of_file(filename)

        if not pnode:
            resp_code = 404
            resp_msg = f"{filename} does not exist in the file system."
            app.logger.error(resp_msg)
            resp = jsonify({'message': resp_msg})
            resp.status_code = resp_code
            return resp
        
        else:
            app.logger.debug(f"Checking if primary node {pnode} containing {filename} is healthy.")
            is_pnode_healthy = flask_utilities.is_sn_healthy(pnode)
            if not is_pnode_healthy:
                app.logger.error(f"Primary node {pnode} unhealthy. Trying to retrieve {filename} from replicated copies.")
                node = find_healthy_sn_with_file(filename)
                if not node:
                    resp_code = 500
                    resp_msg = f"No healthy SN containing {filename} found!"
                    app.logger.error(msg)
                    resp = jsonify({'message': resp_msg})
                    resp.status_code = resp_code
                    return resp
            else:
                app.logger.debug(f"Primary node {pnode} found healthy for {filename}.")
                node = pnode

            file_retrieve_url = FILE_DOWNLOAD_ENDPOINT.format(node_ip=node)
            resp = requests.get(url=file_retrieve_url, params=payload, stream=True)
            new_resp = Response(stream_with_context(resp.iter_content(chunk_size=2048)))
            new_resp.headers['Content-Length'] = resp.headers['Content-Length']
            new_resp.headers['Content-Type'] = resp.headers['Content-Type']
            new_resp.headers['file_hash'] = resp.headers['file_hash']
            return new_resp
    except Exception as e:
        resp_code = 500
        resp_msg = str(e)
        resp = jsonify({'message': resp_msg})
        resp.status_code = resp_code
        return resp

def update_master_table(filename, primary_node):
    sql_stmt = f"""
        INSERT INTO master_node (filename, primary_node)
        VALUES ("{filename}", "{primary_node}");
    """
    conn = sqlite3.connect(flask_utilities.get_db_name())
    with conn:
        conn.execute(sql_stmt)
    conn.close()

def generate_unique_filename(filename):
    sql_stmt = f"""
        SELECT filename FROM master_node
        WHERE filename="{filename}";
    """
    conn = sqlite3.connect(flask_utilities.get_db_name())
    cur = conn.cursor()
    with conn:
        cur.execute(sql_stmt)
        data = cur.fetchone()
        if data:
            filename, ext = filename.rsplit('.', 1)
            filename = f"{filename}_{flask_utilities.generate_random_str(5)}.{ext}"
    conn.close()
    return filename

"""
Return primary node of file if it exists
Else return None
"""
def return_pnode_of_file(filename):
    pnode = None
    sql_stmt = f"""
        SELECT primary_node FROM master_node
        WHERE filename="{filename}";
    """
    conn = sqlite3.connect(flask_utilities.get_db_name())
    cur = conn.cursor()
    with conn:
        cur.execute(sql_stmt)
        data = cur.fetchone()
        if data:
            pnode = data[0]
    conn.close()
    return pnode

def get_sns_with_file_copy(filename):
    all_sns_with_replica = []
    sql_stmt = f"""
        SELECT replicated_node FROM replication_data
        WHERE filename="{filename}";
    """
    conn = sqlite3.connect(flask_utilities.get_db_name())
    cur = conn.cursor()
    with conn:
        cur.execute(sql_stmt)
        data = cur.fetchall() # -> [('sn0:5000',), ('sn3:6050',)]
        if data:
            all_sns_with_replica = [row[0] for row in data] # -> ['sn0', 'sn3']
    conn.close()
    return all_sns_with_replica

def find_healthy_sn_with_file(filename):
    all_sns_with_replica = get_sns_with_file_copy(filename)
    app.logger.debug(f"Found servers with replica of {filename}: {all_sns_with_replica}")
    for count, sn_ip in enumerate(all_sns_with_replica):
        app.logger.debug(f"Attempt {count+1}. Checking health for {sn_ip}. Has file replica for {filename}.")
        if flask_utilities.is_sn_healthy(sn_ip):
            return sn_ip
    return None

if __name__ == '__main__':
    flask_utilities.create_storage_dir(dir_path=STORAGE_DIR)
    app.config['NODE'] = MY_NODE
    app.config['PORT'] = MY_PORT
    app.config['STORAGE_DIR'] = f"storage_{app.config['NODE']}_{app.config['PORT']}"
    app.run(host="0.0.0.0", port=MY_PORT)
