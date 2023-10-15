import inspect
import os
import sys

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
from flask import Flask
from flask import json, make_response
from flask_swagger_ui import get_swaggerui_blueprint

from schemas import *
from utils import helper
from utils.data_manager import DataManager
from utils.docker_engine import DockerEngine

api = Flask(__name__)
api.config.from_object(__name__)
docker_engine = DockerEngine(os.environ['FLINK_CONTAINER_NAME'])
data = DataManager(['job_id'])
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
#
#
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
def job_status():
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
def job_start():
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
def job_stop():
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


@api.route("/", methods=['GET'])
def main():
    return "CDAPS main page: go to '/docs/' to see API"


def create_tags(spec):
    """ Создание тэгов
    :param spec: объект APISpec
    """
    tags = [{'name': 'job', 'description': 'Управление job`ой'},
            {'name': 'config', 'description': 'Настройка процесса обработки данных'}]

    for tag in tags:
        spec.tag(tag)


def load_docstrings(spec, app):
    """ Загрузка описания API
   :param spec: объект APISpec для выгрузки описания функций
   :param app: экземпляр Flask
   """
    for fn_name in app.view_functions:
        if fn_name == 'static':
            continue
        view_fn = app.view_functions[fn_name]
        spec.path(view=view_fn)


@api.route('/swagger')
def create_swagger_spec():
    spec = APISpec(title="CDAPS API", version="1.0.0", openapi_version="3.0.3", plugins=[FlaskPlugin(),
                                                                                         MarshmallowPlugin()])
    create_tags(spec)
    schemas = inspect.getmembers(sys.modules['schemas'], inspect.isclass)
    for schema in schemas:
        if schema[1].__module__ == 'schemas':
            spec.components.schema(schema[0], schema=schema[1])
    load_docstrings(spec, api)
    return json.dumps(spec.to_dict())


if __name__ == '__main__':
    api.run(host='0.0.0.0', debug=True, threaded=True)
