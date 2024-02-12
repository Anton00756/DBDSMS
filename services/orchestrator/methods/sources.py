import marshmallow
from flask import Blueprint
from flask import request, make_response

from utils.config_manager import ConfigManager
from utils.entities import JobConfigException
from utils.schemas import *


def get_blueprint(configer: ConfigManager):
    api = Blueprint('sources', __name__)

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

    return api
