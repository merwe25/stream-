export GOOGLE_APPLICATION_CREDENTIALS="https://storage.cloud.google.com/stream-135/stream--d910eff8dae1.json"
export PROJECT="stream-267306"
export BUCKET="gs://stream-135"

apt-get install python-pip
sudo pip install apache-beam[gcp] oauth2client==3.0.0
sudo pip install -U pip
 