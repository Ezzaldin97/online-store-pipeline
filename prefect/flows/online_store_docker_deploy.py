from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from parent_flow import parent_flow

docker_container_block = DockerContainer.load("online-store-docker")
docker_dep = Deployment.build_from_flow(
    flow = parent_flow,
    name = "online-store-pipeline",
    infrastructure = docker_container_block
)

if __name__ == "__main__":
    docker_dep.apply()