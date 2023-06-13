# spotify_rec

to run the orchestration code and see flow in prefect cloud simply run:

prefect cloud login

python playlist_popularity.py

then login to app.prefect.cloud to see flow runs

to build docker image:
docker image build -t arunvsuresh/prefect:spotify .

to push docker image up to dockerhub:
docker image push arunvsuresh/prefect:spotify

to see prefect profiles:
prefect profile ls

instead of using local ephemeral url, can point to 
url endpoint:
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

look for work from default work queue:
prefect agent start -q default
 note* make sure docker is up and running!

run the flow from the command line:

