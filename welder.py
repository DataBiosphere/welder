import subprocess, os
from flask import (Flask, request, abort, jsonify)

app = Flask(__name__)  # create the application instance


@app.route('/welder/api/syncTo', methods=['POST'])
def sync_to():
    if not request or 'destination' not in request.json:
        return abort(400)
    destination = request.json['destination']
    source = os.getenv('WELDERVOLUME','')
    if not validate_bucket(destination):
        return jsonify({'Destination is not a valid bucket':destination}), 400
    if (source == ''):
        return 'Source not defined', 400
    return run_command(source, destination)


@app.route('/welder/api/syncFrom', methods=['POST'])
def sync_from():
    if not request or 'source' not in request.json:
        return abort(400)
    source = request.json['source']
    destination = os.getenv('WELDERVOLUME','')
    if not validate_bucket(source):
        return 'Source {0} is not a valid GCS bucket'.format(source), 400
    if (destination == ''):
        return 'Destination not defined', 400
    return run_command(source, destination)


def validate_bucket(bucket_name):
    return bucket_name.startswith('gs://')


def run_command(source, destination):
    command = ['gsutil', '-m', 'rsync', '-r', source, destination]
    output = ''
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output= list((iter(p.stdout.readline, b'')))
    if('exception' in output[-2].decode('utf-8').lower()):
        return jsonify({'output':output[-2].decode('utf-8').strip()}), 403
    else:
        if('Starting synchronization...' in output[-1].decode('utf-8').lower()):
            return '', 204
        else:
            return jsonify({'output':output[-1].decode('utf-8').strip()}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0')
