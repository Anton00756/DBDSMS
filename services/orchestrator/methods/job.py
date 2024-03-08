import os

import marshmallow
import yaml
from flask import Blueprint
from flask import request, make_response

from utils.config_manager import ConfigManager
from utils.data_manager import DataManager
from utils.docker_engine import DockerEngine
from utils.entities import JobConfigException
from utils.schemas import *


def get_blueprint(data: DataManager, configer: ConfigManager, docker: DockerEngine):
    api = Blueprint('job', __name__)

    @api.route('/job/status', methods=['GET'])
    def get_job_status():
        """
        ---
        get:
            summary: Получить статус задания
            responses:
                '200':
                    description: Статус задания
                    content:
                        application/json:
                            schema: JobStatusSchema
            tags:
                - job
        """
        schema = JobStatusSchema()
        schema.status = 'not_running' if data['job_id'] is None else 'running'
        return make_response(schema.dump(schema), 200)

    @api.route('/job/start', methods=['POST'])
    def start_job():
        """
        ---
        post:
            summary: Запустить задание
            responses:
                '201':
                    description: Задание успешно запущено
                '409':
                    description: Задание уже запущено
            tags:
                - job
        """
        if data['job_id'] is not None:
            return make_response('Задание уже запущено!', 409)
        data['job_id'] = docker.start_job()
        return make_response('OK', 201)

    @api.route('/job/stop', methods=['POST'])
    def stop_job():
        """
        ---
        post:
            summary: Остановить задание
            responses:
                '201':
                    description: Задание успешно остановлено
                '409':
                    description: Задание не запущено
            tags:
                - job
        """
        if data['job_id'] is None:
            return make_response('Задание не запущено!', 409)
        docker.stop_job(data['job_id'])
        data['job_id'] = None
        return make_response('OK', 201)

    @api.route('/job/get_chains', methods=['GET'])
    def get_job_chains():
        """
        ---
        get:
            summary: Отобразить содержимое задания в виде цепочек
            responses:
                '200':
                    description: Содержимое задания
            tags:
                - job
        """
        return make_response(configer.job.get_chains(), 200)

    @api.route('/job/get_json', methods=['GET'])
    def get_job_json():
        """
        ---
        get:
            summary: Отобразить содержимое задания в JSON-формате
            responses:
                '200':
                    description: Содержимое задания
                    content:
                        application/json:
                            schema: JobSchema
            tags:
                - job
        """
        return make_response(JobSchema().dump(configer.job.to_json()), 200)

    @api.route('/job/get_yaml', methods=['GET'])
    def get_job_yaml():
        """
        ---
        get:
            summary: Скачать содержимое задания в YAML-формате
            responses:
                '200':
                    description: Содержимое задания
                    content:
                        application/x-yaml:
                            schema: JobSchema
            tags:
                - job
        """
        return make_response(yaml.dump(JobSchema().dump(configer.job.to_json())), 200)

    @api.route('/job/post_json', methods=['POST'])
    def post_job_json():
        """
        ---
        post:
            summary: Загрузить содержимое задания в JSON-формате
            parameters:
                - in: query
                  name: params
                  required: true
                  content:
                    application/json:
                      schema: JobSchema
            responses:
                '201':
                    description: Задание успешно загружено
                '400':
                    description: Некорректные входные данные
                    content:
                        application/json:
                            schema: ErrorSchema
                '409':
                    description: Ошибка при обработке данных
                    content:
                        application/json:
                            schema: ErrorSchema
            tags:
                - job
        """
        try:
            configer.set_job(JobSchema().loads(request.args.get('params')))
            return make_response('OK', 201)
        except marshmallow.exceptions.ValidationError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except ValueError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    @api.route('/job/post_yaml', methods=['POST'])
    def post_yaml_json():
        """
        ---
        post:
            summary: Загрузить содержимое задания в YAML-формате
            parameters:
                - in: query
                  name: params
                  required: true
                  content:
                    application/x-yaml:
                      schema: JobSchema
            responses:
                '201':
                    description: Задание успешно загружено
                '400':
                    description: Некорректные входные данные
                    content:
                        application/json:
                            schema: ErrorSchema
                '409':
                    description: Ошибка при обработке данных
                    content:
                        application/json:
                            schema: ErrorSchema
            tags:
                - job
        """
        try:
            configer.set_job(JobSchema().load(yaml.load(request.args.get('params'), Loader=yaml.FullLoader)))
            return make_response('OK', 201)
        except marshmallow.exceptions.ValidationError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except ValueError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    @api.route('/job/get_grafana', methods=['GET'])
    def get_grafana_json():
        """
        ---
        get:
            summary: Отобразить шаблон для Grafana в JSON-формате
            responses:
                '200':
                    description: Содержимое шаблона
                    content:
                        application/json:
                            schema:
                                type: object
                '404':
                    description: Файл не найден
            tags:
                - job
        """
        if not os.path.exists('shared_data/grafana.json'):
            return make_response('Файл не найден!', 404)
        with open('shared_data/grafana.json', 'r') as file:
            return make_response(file.read(), 200)

    return api
