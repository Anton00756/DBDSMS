import os
import json
from utils import helper
from utils.entities import Settings, Job, Source, Sink, JobConfigException

LOGGER = helper.get_logger()


class ConfigManager:
    def __init__(self, path: str):
        self.config_path = path
        if os.path.exists(path):
            with open(self.config_path, 'r') as file:
                try:
                    self.job = Job(json.load(file))
                    return
                except json.JSONDecodeError as error:
                    LOGGER.error(f'Ошибка при чтении файла конфигурации: {error}')
                except JobConfigException as error:
                    LOGGER.error(f'Ошибка конфигурации: {error}')
        self.job = Job()

    def set_job(self, job: Job):
        self.job = job
        self.update_json()

    def add_source(self, name: str, source: Source):
        self.job.add_source(name, source)
        self.update_json()

    def delete_source(self, name: str):
        self.job.delete_source(name)
        self.update_json()

    def add_sink(self, name: str, sink: Sink):
        self.job.add_sink(name, sink)
        self.update_json()

    def delete_sink(self, name: str):
        self.job.delete_sink(name)
        self.update_json()

    def set_settings(self, settings: Settings):
        self.job.settings = settings
        self.update_json()

    def update_json(self):
        with open(self.config_path, 'w') as file:
            json.dump(self.job.to_json(), file, indent=4)
