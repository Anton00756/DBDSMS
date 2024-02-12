import marshmallow
from flask import Blueprint
from flask import request, make_response

from utils.config_manager import ConfigManager
from utils.schemas import *


def get_blueprint(configer: ConfigManager):
    api = Blueprint('settings', __name__)

    @api.route('/job_setting/set_settings', methods=['POST'])
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

    return api
