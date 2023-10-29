import inspect
import os
import sys

import marshmallow
from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
from flask import Flask, request, json, make_response
from flask_swagger_ui import get_swaggerui_blueprint

from utils import helper
from utils.config_manager import ConfigManager
from utils.data_manager import DataManager
from utils.docker_engine import DockerEngine
from utils.entities import JobConfigException
from utils.schemas import *
import yaml


api = Flask(__name__)
api.config.from_object(__name__)
docker_engine = DockerEngine(os.environ['FLINK_CONTAINER_NAME'])
data = DataManager(['job_id'])
configer = ConfigManager(os.environ['CONFIG_FOR_JOB_PATH'])
LOGGER = helper.get_logger()

SWAGGER_URL = '/docs'
API_URL = '/swagger'

swagger_ui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': 'CDAPS API'
    }
)
api.register_blueprint(swagger_ui_blueprint)


# @api.route('/job/start', methods=['POST'])
# def power():
#     """
#     ---
#     get:
#      summary: Возводит число в степень
#      parameters:
#        - in: query
#          schema: InputSchema
#      responses:
#        '200':
#          description: Результат возведения в степень
#          content:
#            application/json:
#              schema: OutputSchema
#        '400':
#          description: Не передан обязательный параметр
#          content:
#            application/json:
#              schema: ErrorSchema
#      tags:
#        - math
#     """
#     args = request.args
#
#     number = args.get('number')
#     if number is None:
#         return current_app.response_class(
#             response=json.dumps(
#                 {'error': 'Не передан параметр number'}
#             ),
#             status=400,
#             mimetype='application/json'
#         )
#
#     power = args.get('power')
#     if power is None:
#         return current_app.response_class(
#             response=json.dumps(
#                 {'error': 'Не передан параметр power'}
#             ),
#             status=400,
#             mimetype='application/json'
#         )
#
#     return current_app.response_class(
#         response=json.dumps(
#             {'response': int(number) ** int(power)}
#         ),
#         status=200,
#         mimetype='application/json'
#     )


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
    data['job_id'] = docker_engine.start_job()
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
    docker_engine.stop_job(data['job_id'])
    data['job_id'] = None
    return make_response('OK', 201)


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
    return make_response(JobSchema().dump(configer.job), 200)


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
    return make_response(yaml.dump(JobSchema().dump(configer.job)), 200)


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
        tags:
            - job
    """
    try:
        configer.set_job(JobSchema().loads(request.args.get('params')))
        return make_response('OK', 201)
    except marshmallow.exceptions.ValidationError as error:
        return make_response(ErrorSchema().dump({'error': error}), 400)


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
        tags:
            - job
    """
    try:
        configer.set_job(JobSchema().load(yaml.load(request.args.get('params'), Loader=yaml.FullLoader)))
        return make_response('OK', 201)
    except marshmallow.exceptions.ValidationError as error:
        return make_response(ErrorSchema().dump({'error': error}), 400)


@api.route('/job/set_settings', methods=['POST'])
def set_job_settings():
    """
    ---
    post:
        summary: Установить параметры выполнения задания
        parameters:
            - in: query
              schema: JobSettingsSchema
        responses:
            '201':
                description: Настройки установлены
            '400':
                description: Некорректные входные данные
                content:
                    application/json:
                        schema: ErrorSchema
        tags:
            - job_setting
    """
    try:
        settings = JobSettingsSchema().load(request.args)
        configer.set_settings(settings)
        return make_response('OK', 201)
    except marshmallow.exceptions.ValidationError as error:
        return make_response(ErrorSchema().dump({'error': error}), 400)


@api.route('/storage/source/add_kafka', methods=['POST'])
def add_kafka_source():
    """
    ---
    post:
        summary: Добавить источник данных из Kafka
        parameters:
            - in: query
              name: params
              required: true
              content:
                application/json:
                  schema: PostSourceSchema
        responses:
            '201':
                description: Источник успешно добавлен
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
            - storage
    """
    try:
        name, source = PostSourceSchema().loads(request.args.get('params'))
        configer.add_source(name, source)
        return make_response('OK', 201)
    except marshmallow.exceptions.ValidationError as error:
        return make_response(ErrorSchema().dump({'error': error}), 400)
    except JobConfigException as error:
        return make_response(ErrorSchema().dump({'error': error}), 409)


@api.route('/storage/source/<source_name>/delete', methods=['DELETE'])
def delete_source(source_name):
    """
    ---
    delete:
        summary: Удалить источник данных
        parameters:
            - in: path
              name: source_name
              required: true
              schema:
                type: string
              description: Название источника
        responses:
            '201':
                description: Источник успешно удалён
            '409':
                description: Ошибка при обработке данных
                content:
                    application/json:
                        schema: ErrorSchema
        tags:
            - storage
    """
    try:
        configer.delete_source(source_name)
        return make_response('OK', 201)
    except JobConfigException as error:
        return make_response(ErrorSchema().dump({'error': error}), 409)


@api.route('/storage/sink/add_kafka', methods=['POST'])
def add_kafka_sink():
    """
    ---
    post:
        summary: Добавить источник данных из Kafka
        parameters:
            - in: query
              name: params
              required: true
              content:
                application/json:
                  schema: PostKafkaSinkSchema
        responses:
            '201':
                description: Выходное хранилище успешно добавлено
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
            - storage
    """
    try:
        name, sink = PostKafkaSinkSchema().loads(request.args.get('params'))
        configer.add_sink(name, sink)
        return make_response('OK', 201)
    except marshmallow.exceptions.ValidationError as error:
        return make_response(ErrorSchema().dump({'error': error}), 400)
    except JobConfigException as error:
        return make_response(ErrorSchema().dump({'error': error}), 409)


@api.route('/storage/sink/add_greenplum', methods=['POST'])
def add_greenplum_sink():
    """
    ---
    post:
        summary: Добавить источник данных из Greenplum
        parameters:
            - in: query
              name: params
              required: true
              content:
                application/json:
                  schema: PostGreenplumSinkSchema
        responses:
            '201':
                description: Выходное хранилище успешно добавлено
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
            - storage
    """
    try:
        name, sink = PostGreenplumSinkSchema().loads(request.args.get('params'))
        configer.add_sink(name, sink)
        return make_response('OK', 201)
    except marshmallow.exceptions.ValidationError as error:
        return make_response(ErrorSchema().dump({'error': error}), 400)
    except JobConfigException as error:
        return make_response(ErrorSchema().dump({'error': error}), 409)


@api.route('/storage/sink/add_minio', methods=['POST'])
def add_minio_sink():
    """
    ---
    post:
        summary: Добавить источник данных из Minio
        parameters:
            - in: query
              name: params
              required: true
              content:
                application/json:
                  schema: PostMinioSinkSchema
        responses:
            '201':
                description: Выходное хранилище успешно добавлено
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
            - storage
    """
    try:
        name, sink = PostMinioSinkSchema().loads(request.args.get('params'))
        configer.add_sink(name, sink)
        return make_response('OK', 201)
    except marshmallow.exceptions.ValidationError as error:
        return make_response(ErrorSchema().dump({'error': error}), 400)
    except JobConfigException as error:
        return make_response(ErrorSchema().dump({'error': error}), 409)


@api.route('/storage/sink/<sink_name>/delete', methods=['DELETE'])
def delete_sink(sink_name):
    """
    ---
    delete:
        summary: Удалить выходное хранилище данных
        parameters:
            - in: path
              name: sink_name
              required: true
              schema:
                type: string
              description: Название выходного хранилища
        responses:
            '201':
                description: Хранилище успешно удалено
            '409':
                description: Ошибка при обработке данных
                content:
                    application/json:
                        schema: ErrorSchema
        tags:
            - storage
    """
    try:
        configer.delete_sink(sink_name)
        return make_response('OK', 201)
    except JobConfigException as error:
        return make_response(ErrorSchema().dump({'error': error}), 409)


@api.route("/", methods=['GET'])
def main():
    return "CDAPS main page: go to '/docs/' to see API"


def create_tags(spec):
    """ Создание тэгов
    :param spec: объект APISpec
    """
    tags = [{'name': 'job', 'description': 'Управление job`ой'},
            {'name': 'storage', 'description': '[JobConfig] Управление хранилищами'},
            {'name': 'operator', 'description': '[JobConfig] Управление операторами'},
            {'name': 'job_setting', 'description': '[JobConfig] Управление настройками при выполнении задания'}]

    for tag in tags:
        spec.tag(tag)


def load_docstrings(spec, app):
    """ Загрузка описания API
   :param spec: объект APISpec для выгрузки описания функций
   :param app: экземпляр Flask
   """
    for fn_name in app.view_functions:
        if fn_name != 'static':
            view_fn = app.view_functions[fn_name]
            spec.path(view=view_fn)


@api.route('/swagger')
def create_swagger_spec():
    spec = APISpec(title="CDAPS API", version="1.0.0", openapi_version="3.0.3", plugins=[FlaskPlugin(),
                                                                                         MarshmallowPlugin()])
    create_tags(spec)
    schemas = inspect.getmembers(sys.modules['utils.schemas'], inspect.isclass)
    for schema in schemas:
        spec.components.schema(schema[0], schema=schema[1])
    load_docstrings(spec, api)
    return json.dumps(spec.to_dict())


if __name__ == '__main__':
    api.run(host='0.0.0.0', debug=True, threaded=True)
