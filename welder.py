import subprocess, os
from flask import (Flask, request, abort, jsonify)

app = Flask(__name__)  # create the application instance


@app.route('/welder/api/syncTo', methods=['POST'])
def sync_to():
    if not request or 'destination' not in request.json:
        abort(400)
    destination = request.json['destination']
    source = os.getenv('WELDERVOLUME','')
    if not validate_bucket(destination):
        return jsonify({"Destination is not a valid bucket":destination}), 400
    if (source == ""):
        return "Source not defined", 400
    command = ['gsutil', '-m', 'rsync', '-r', source, destination]
    output = ""
    for line in run_command(command):
        output += line 
    return jsonify({'output':output}), 200


@app.route('/welder/api/syncFrom', methods=['POST'])
def sync_from():
    if not request or 'source' not in request.json:
        abort(400)
    source = request.json['source']
    destination = os.getenv('WELDERVOLUME','')
    if not validate_bucket(source):
        return 'Source {0} is not a valid GCS bucket'.format(source), 400
    if (destination == ""):
        return "Destination not defined", 400
    command = ['gsutil', '-m', 'rsync', '-r', source, destination]
    output = ""
    for line in run_command(command):
        output += line 
    return jsonify({'output':output}), 200


def validate_bucket(bucket_name):
    return bucket_name.startswith('gs:')


def run_command(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return iter(p.stdout.readline, b'')

if __name__ == "__main__":
    app.run(host='0.0.0.0')
