# Try it out

* Start hamm-api-server `sbt server/run`
* Run automation tests against the running server `sbt automation/test`

# Publish container image to Google container registry
* Set up auth for publishing docker image to GCR
`gcloud auth configure-docker`
* Publish welder
`sbt server/docker:publish` (or `sbt server/docker:publishLocal` for local development)

# Development

## Using git secrets
Make sure git secrets is installed:
```bash
brew install git-secrets
```
Ensure git-secrets is run:
<i>If you use the rsync script to run locally you can skip this step</i>
```bash
cp -r hooks/ .git/hooks/
chmod 755 .git/hooks/apply-git-secrets.sh
```
