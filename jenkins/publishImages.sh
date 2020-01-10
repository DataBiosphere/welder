#!/bin/bash

# This file is being used by https://fc-jenkins.dsp-techops.broadinstitute.org/view/Leonardo/job/welder-publish-image/

# the chown is necessary because something on the jenkins instance keeps touching the logs with sudo and changing the owner to root, which causes gcloud auth command to fail
sudo chown -R jenkins:jenkins /home/jenkins/.config
docker run --rm  -v /etc/vault-token-dsde:/root/.vault-token:ro broadinstitute/dsde-toolbox:latest vault read --format=json secret/dsde/dsp-techops/common/dspci-wb-gcr-service-account.json | jq .data > dspci-wb-gcr-service-account.json
gcloud auth activate-service-account --key-file=dspci-wb-gcr-service-account.json
gcloud auth configure-docker --quiet
sbt server/docker:publish
rm -f /home/jenkins/.docker/config.json

# Push to dockerhub
HASH=\$\(git rev-parse --short HEAD\)
git rev-parse --short HEAD
docker tag us.gcr.io/broad-dsp-gcr-public/welder-server:$HASH broadinstitute/welder-server:$HASH
docker push broadinstitute/welder-server:$HASH