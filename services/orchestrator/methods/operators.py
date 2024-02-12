import marshmallow
from flask import Blueprint
from flask import request, make_response

from utils.config_manager import ConfigManager
from utils.entities import JobConfigException
from utils.schemas import *


def get_blueprint(configer: ConfigManager):
    api = Blueprint('operators', __name__)

    @api.route('/operator/add_deduplicator', methods=['POST'])
    def add_deduplicator():
        """
        ---
        post:
            summary: Добавить дедупликатор
            parameters:
                - in: query
                  schema: DeduplicatorSchema
            responses:
                '201':
                    description: Оператор добавлен
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
                - operator
        """
        try:
            source, operator = DeduplicatorSchema().load(request.args)
            configer.add_operator(source, operator)
            return make_response('OK', 201)
        except marshmallow.exceptions.ValidationError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    @api.route('/operator/add_filter', methods=['POST'])
    def add_filter():
        """
        ---
        post:
            summary: Добавить фильтр
            parameters:
                - in: query
                  schema: FilterSchema
            responses:
                '201':
                    description: Оператор добавлен
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
                - operator
        """
        try:
            source, operator = FilterSchema().load(request.args)
            configer.add_operator(source, operator)
            return make_response('OK', 201)
        except marshmallow.exceptions.ValidationError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except ValueError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    @api.route('/operator/add_output', methods=['POST'])
    def add_output():
        """
        ---
        post:
            summary: Добавить вывод в хранилище
            parameters:
                - in: query
                  schema: OutputSchema
            responses:
                '201':
                    description: Оператор добавлен
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
                - operator
        """
        try:
            source, operator = OutputSchema().load(request.args)
            configer.add_operator(source, operator)
            return make_response('OK', 201)
        except marshmallow.exceptions.ValidationError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except ValueError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    @api.route('/operator/add_clone', methods=['POST'])
    def add_clone():
        """
        ---
        post:
            summary: Добавить копию
            parameters:
                - in: query
                  schema: CloneSchema
            responses:
                '201':
                    description: Оператор добавлен
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
                - operator
        """
        try:
            source, operator = CloneSchema().load(request.args)
            configer.add_operator(source, operator)
            return make_response('OK', 201)
        except marshmallow.exceptions.ValidationError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except ValueError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    @api.route('/operator/add_field_changer', methods=['POST'])
    def add_field_changer():
        """
        ---
        post:
            summary: Добавить изменение поля
            parameters:
                - in: query
                  schema: FieldChangerSchema
            responses:
                '201':
                    description: Оператор добавлен
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
                - operator
        """
        try:
            source, operator = FieldChangerSchema().load(request.args)
            configer.add_operator(source, operator)
            return make_response('OK', 201)
        except marshmallow.exceptions.ValidationError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except ValueError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    @api.route('/operator/add_field_deleter', methods=['POST'])
    def add_field_deleter():
        """
        ---
        post:
            summary: Добавить удаление поля
            parameters:
                - in: query
                  schema: FieldDeleterSchema
            responses:
                '201':
                    description: Оператор добавлен
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
                - operator
        """
        try:
            source, operator = FieldDeleterSchema().load(request.args)
            configer.add_operator(source, operator)
            return make_response('OK', 201)
        except marshmallow.exceptions.ValidationError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except ValueError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    @api.route('/operator/add_field_enricher', methods=['POST'])
    def add_field_enricher():
        """
        ---
        post:
            summary: Добавить обогащение поля
            parameters:
                - in: query
                  schema: FieldEnricherSchema
            responses:
                '201':
                    description: Оператор добавлен
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
                - operator
        """
        try:
            source, operator = FieldEnricherSchema().load(request.args)
            configer.add_operator(source, operator)
            return make_response('OK', 201)
        except marshmallow.exceptions.ValidationError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except ValueError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    @api.route('/operator/add_stream_joiner', methods=['POST'])
    def add_stream_joiner():
        """
        ---
        post:
            summary: Добавить объединение источников
            parameters:
                - in: query
                  schema: StreamJoinerSchema
            responses:
                '201':
                    description: Оператор добавлен
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
                - operator
        """
        try:
            source, operator = StreamJoinerSchema().load(request.args)
            configer.add_operator(source, operator)
            return make_response('OK', 201)
        except marshmallow.exceptions.ValidationError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except ValueError as error:
            return make_response(ErrorSchema().dump({'error': error}), 400)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    @api.route('/operator/<source_name>/delete', methods=['DELETE'])
    def delete_operator(source_name):
        """
        ---
        delete:
            summary: Удалить оператор
            parameters:
                - in: path
                  name: source_name
                  required: true
                  schema:
                    type: string
                  description: Название источника
                - in: query
                  name: pos
                  required: false
                  schema:
                    type: int
                  description: Индекс оператора
            responses:
                '201':
                    description: Оператор успешно удалён
                '409':
                    description: Ошибка при обработке данных
                    content:
                        application/json:
                            schema: ErrorSchema
            tags:
                - operator
        """
        try:
            configer.delete_operator(source_name, int(request.args.get('pos')) if request.args.get('pos') else None)
            return make_response('OK', 201)
        except JobConfigException as error:
            return make_response(ErrorSchema().dump({'error': error}), 409)

    return api
