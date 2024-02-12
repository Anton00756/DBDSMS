import marshmallow
from flask import Blueprint
from flask import request, make_response

from utils.config_manager import ConfigManager
from utils.entities import JobConfigException
from utils.schemas import *


def get_blueprint(configer: ConfigManager):
    api = Blueprint('sinks', __name__)

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

    return api
