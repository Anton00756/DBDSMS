import inspect
import os
import sys

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
from flask import Flask, json
from flask_swagger_ui import get_swaggerui_blueprint

from methods import job as job_methods, settings as settings_methods, operators as operator_methods, \
    sinks as sinks_methods, sources as sources_methods
from utils import helper
from utils.config_manager import ConfigManager
from utils.data_manager import DataManager
from utils.docker_engine import DockerEngine

if (env_data := os.environ.get('IGNORE_EXCEPTIONS_DURING_VALIDATION')) is not None:
    setattr(sys.modules['utils.entities.operators'], 'IGNORE_EXCEPTIONS', env_data.split(','))
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

api.register_blueprint(job_methods.get_blueprint(data, configer, docker_engine))
api.register_blueprint(settings_methods.get_blueprint(configer))
api.register_blueprint(sources_methods.get_blueprint(configer))
api.register_blueprint(operator_methods.get_blueprint(configer))
api.register_blueprint(sinks_methods.get_blueprint(configer))


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
