from prefect.deployments import Deployment
# from prefect.infrastructure.docker import DockerContainer
from etl_gcs_to_bq import etl_parent_gcs_to_bq_flow

# docker_block = DockerContainer.load("zoomcamp")

deployment = Deployment.build_from_flow(
    flow=etl_parent_gcs_to_bq_flow,
    name='prefect-cloud-deployment',
    # infrastructure=docker_block
)

if __name__ == "__main__":
    deployment.apply()