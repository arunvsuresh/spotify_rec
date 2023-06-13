from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from etl_gcs_to_bq import etl_parent_gcs_to_bq_flow

docker_block = DockerContainer.load("zoomcamp")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_gcs_to_bq_flow,
    name='spotify-docker-flow',
    infrastructure=docker_block
)

if __name__ == '__main__':
    docker_dep.apply()