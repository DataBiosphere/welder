# Welder

Welder is a lightweight webservice intended to be run on Dataproc Clusters and GCE VMs spun up by [Leonardo](https://github.com/DataBiosphere/leonardo). Compute spun up by Leonardo allows users to run interactive tools, such as Jupyter, in the cloud. Welder's primary purpose is to facilitate the persistence of files generated by these tools to a bucket in Google Cloud Storage. See the API section below for a list of endpoints and functions available in Welder.

# Try it out

Setup env vars and auth
```bash
export OWNER_EMAIL="fake@gmail.com"
export STAGING_BUCKET="jc-sample-bucket"
export SHOULD_BACKGROUND_SYNC=false # false if you don't want the file syncing background process to run
export CLOUD_PROVIDER=gcp
export WSM_URL="dummy" # if doesn't work use the real WSM url
export WORKSPACE_ID="dummy"
export STORAGE_CONTAINER_RESOURCE_ID="dummy"
export PORT=8080
export LOCKING_ENABLED=true
gcloud auth application-default login
```

* Start welder-api-server `sbt server/run`
** you may want to create `server/src/main/resources/application.conf` with the following content
```bash
path-to-storage-links-json = "storage_links.json"
path-to-gcs-metadata-json = "gcs_metadata.json"
object-service.working-directory = "/tmp"
```


# Publish container image to Google container registry
* Set up auth for publishing docker image to GCR
`gcloud auth configure-docker`
* Publish welder. 
   * **IMPORTANT** make sure you have a new commit so that you don't override the HEAD hash in gcr
   * For local development: `sbt server/docker:publishLocal`
   * If you'd like to share the image:  `sbt server/docker:publish` 

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

## After merging your PR
Make sure all builds are green after you merge PR. Look for `welder-update-hash` notification in `dsp-callisto-internal` channel, and open the PR for updating welder hash in leonardo. Make sure to include relevant Jira ticket number in the PR title if necessary.

## Run unit tests
Set up environment variables
```
export OWNER_EMAIL="fake@gmail.com"
export STAGING_BUCKET="jc-sample-bucket"
export SHOULD_BACKGROUND_SYNC=false # false if you don't want the file syncing background process to run
export CLOUD_PROVIDER=gcp
export WSM_URL="dummy" # if doesn't work use the real WSM url
export WORKSPACE_ID="dummy"
export STORAGE_CONTAINER_RESOURCE_ID="dummy"
```
Run tests
```
sbt test
```

# UI workflow
![UI workflow](UI_Interaction.png)

# How to modify the workflow image?
* Enable [IntelliJ plugin](https://plugins.jetbrains.com/plugin/7017-plantuml-integration)
* modify [UI_Interaction.puml](server/src/main/resources/UI_Interaction.puml)
* Save rendered image as `UI_Interaction.png`

# For Clients

## Hashed Metadata
For the the sake of privacy, some of the fields stored on the GCS object metadata are hashed. Specifically, the `lastLockedBy` field is hashed in combination with the GCS bucket name. The formula for this hash is `sha256(my-bucket-name:my-email-address@gmail.com)`. Note that `my-bucket-name` does not contain the `gs://` prefix. You can use this formula to check whether or not known email addresses hold locks in known buckets.
