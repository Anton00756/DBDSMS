import docker

from utils import helper

LOGGER = helper.get_logger()


class DockerEngineException(Exception):
    pass


class DockerEngine:
    def __init__(self, flink_container_name: str):
        self.client = docker.from_env()
        try:
            flink_id = self.client.containers.list(filters={'name': flink_container_name})[0].id
            self.flink = self.client.containers.get(flink_id)
        except IndexError:
            raise DockerEngineException('Flink-контейнер не найден!')

    def start_job(self) -> str:
        result = self.flink.exec_run('/opt/flink/bin/flink run --detached -py /work_dir/service/processor.py')
        job_id = result.output.decode('utf-8').split('\n')[-2].split(' ')[-1]
        return job_id

    def stop_job(self, job_id: str):
        self.flink.exec_run(f'/opt/flink/bin/flink cancel {job_id}')
