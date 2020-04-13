import argparse
import pprint
import requests
import flask_utilities
import os
from contextlib import closing

MASTER_URL = flask_utilities.get_master_endpoint()
UPLOAD_FILE_ENDPOINT = f'http://{MASTER_URL}/upload'
DOWNLOAD_FILE_ENDPOINT = f'http://{MASTER_URL}/download'
SCRIPT_NAME = os.path.basename(__file__)
STORAGE_DIR = 'received_files'

def parse_cmd_args():
    parser = argparse.ArgumentParser(prog=SCRIPT_NAME)
    parser.add_argument("--verbose", help="increase output verbosity",
                        action="store_true")

    subparsers = parser.add_subparsers(help='file get/put help', required=True)

    # create the parser for the "get" sub-command
    parser_get = subparsers.add_parser('get', help='get help')
    parser_get.add_argument('get_filename', help="filepath to retrieve")

    # create the parser for the "put" sub-command
    parser_put = subparsers.add_parser('put', help='put help')
    parser_put.add_argument('put_filepath', help="filepath to store")

    return parser.parse_args()

def request_file_from_server(filename):
    try:
        resp_code = 400; resp_msg = "Operation failed."
        print(f"Initiating retrive {filename} from server.")
        payload = {
            'filename': filename
        }

        flask_utilities.create_storage_dir(dir_path=STORAGE_DIR)
        storage_filepath = os.path.join(STORAGE_DIR, filename)

        with open(storage_filepath, "wb") as fp:
            with closing(requests.get(url=DOWNLOAD_FILE_ENDPOINT, params=payload, stream=True)) as r:
                file_hash = r.headers['file_hash']
                resp_code = r.status_code
                if resp_code == requests.codes.ok:
                    for chunk in r.iter_content(chunk_size=2048):
                        fp.write(chunk)
                    print("File retrieved successfully!")

        if resp_code == requests.codes.ok:
            print("Checking file integrity.")
            is_file_valid = flask_utilities.is_file_integrity_matched(
                    filepath=storage_filepath,
                    recvd_hash=file_hash
            )
            if is_file_valid:
                resp_code = 200
                resp_msg = f"{storage_filepath} received. File integrity validated."
            else:
                resp_code = 500
                resp_msg = f"{storage_filepath} received. File integrity does not match."
        else:
            resp_msg = resp.json().get('message', None)

    except Exception as e:
        resp_code = 400; resp_msg = str(e)

    return {
        "status_code": resp_code,
        "message": resp_msg
    }

def put_file_at_server(filepath):
    try:
        print(f"Initiating store {filepath} to server.")
        file_hash = flask_utilities.calc_file_md5(filepath)
        files = {'input_file': open(filepath,'rb')}
        data = {'file_hash': file_hash}
        resp = requests.post(url=UPLOAD_FILE_ENDPOINT, data=data, files=files)
        if resp.status_code == requests.codes.ok:
            print("File stored successfully!")
        resp_code = resp.status_code
        resp_msg = resp.json().get('message', None)
    except Exception as e:
        resp_code = 400
        resp_msg = str(e)

    return {
        "status_code": resp_code,
        "message": resp_msg
    }

def main():
    flask_utilities.create_storage_dir(dir_path=STORAGE_DIR)
    args = parse_cmd_args()
    if hasattr(args, 'get_filename') and hasattr(args, 'put_filepath'):
        logging.error("Incorrect usage")
        parser.print_help()
        return
    if hasattr(args, 'get_filename'):
        response = request_file_from_server(filename=args.get_filename)
        pprint.pprint(response)
    if hasattr(args, 'put_filepath'):
        flask_utilities.check_filepath_sanity(args.put_filepath)
        response = put_file_at_server(filepath=args.put_filepath)
        pprint.pprint(response)

if __name__ == "__main__":
    main()