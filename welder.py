import subprocess, os
import re
from flask import (Flask, request, abort, jsonify)

app = Flask(__name__)  # create the application instance


@app.route('/api/delocalize', methods=['POST'])
def sync_to():
    if not request or 'destination' not in request.json:
        return abort(400)
    destination = request.json['destination']
    source = os.getenv('WELDERVOLUME', '')
    if not validate_bucket(destination):
        return jsonify({'Destination is not a valid bucket': destination}), 400
    if source == '':
        return 'Source not defined', 400
    command = ['gsutil', '-m', 'rsync', '-p', '-r', source, destination]
    return process_rsync_output(run_command(command))


@app.route('/api/localize', methods=['POST'])
def sync_from():
    if not request or 'source' not in request.json:
        return abort(400)
    source = request.json['source']
    destination = os.getenv('WELDERVOLUME', '')
    if not validate_bucket(source):
        return 'Source {0} is not a valid GCS bucket'.format(source), 400
    if destination == '':
        return 'Destination not defined', 400
    rsync_command = ['gsutil', '-m', 'rsync', '-p', '-r', source, destination]
    rsync_output = process_rsync_output(run_command(rsync_command))
    chown_command = ['chown', '-R', '1000:1000', destination]
    run_command(chown_command)
    return rsync_output


def validate_bucket(bucket_name):
    gcs_scheme = "gs"
    gcs_bucket_name_pattern_base = """[a-z0-9][a-z0-9-_\\.]{1,61}[a-z0-9]"""

    # Regex for a full GCS path which captures the bucket name.

    gcs_path_pattern = '(?x)^{0}:\/\/({1})\/?(?:.*)?'.format(gcs_scheme, gcs_bucket_name_pattern_base)
    
    return re.match(gcs_path_pattern, bucket_name)


def run_command(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return list((iter(p.stdout.readline, b'')))


def process_rsync_output(output):
    if 'exception' in output[-2].decode('utf-8').lower():
        return jsonify({'message': output[-2].decode('utf-8').strip()}), 412
    else:
        if 'starting synchronization...' in output[-1].decode('utf-8').lower():
            return jsonify({'message': 'Files are up to date.'}), 200
        else:
            return jsonify({'message': output[-1].decode('utf-8').strip()}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0')
