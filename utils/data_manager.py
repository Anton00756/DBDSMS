import os
import pickle
from typing import Optional

from utils import helper

LOGGER = helper.get_logger()
SAVE_FILE_PATH = '/home/user/server_save.pk'


class DataManagerException(Exception):
    pass


class DataManager:
    def __init__(self, keys: Optional[list] = None):
        if os.path.exists(SAVE_FILE_PATH):
            with open(SAVE_FILE_PATH, 'rb') as file:
                self.data = pickle.load(file)
        elif keys is None:
            self.data = {}
        else:
            self.data = {key: None for key in keys}

    def __getitem__(self, item):
        if item not in self.data:
            raise DataManagerException(f'Ключ "{item}" не найден!')
        return self.data[item]

    def __setitem__(self, key, value):
        self.data[key] = value
        with open(SAVE_FILE_PATH, 'wb') as file:
            pickle.dump(self.data, file)
