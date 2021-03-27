# Copyright (C) 2021 Intel Corporation
#
# SPDX-License-Identifier: MIT

import json
import os
from abc import ABC, abstractmethod
from collections import OrderedDict
from contextlib import closing
from PIL import Image
from .utils import md5_hash, rotate_image

class DatasetImagesReader:
    def __init__(self, sources, is_sorted=True, use_image_hash=False, *args, **kwargs):
        self._sources = sources if is_sorted else sorted(sources)
        self._content = []
        self._data_dir = kwargs.get('data_dir', None)
        self._use_image_hash = use_image_hash

    def __iter__(self):
        for image in self._sources:
            img = Image.open(image, mode='r')
            img_name = os.path.relpath(image, self._data_dir) if self._data_dir \
                else os.path.basename(image)
            name, extension = os.path.splitext(img_name)
            image_properties = {
                'name': name,
                'extension': extension,
                'width': img.width,
                'height': img.height,
            }
            if self._use_image_hash:
                image_properties['checksum'] = md5_hash(img)
            yield image_properties

    def create(self):
        for item in self:
            self._content.append(item)

    @property
    def content(self):
        return self._content

class _Manifest:
    FILE_NAME = 'manifest.jsonl'
    VERSION = '1.0'

    def __init__(self, path, is_created=False):
        assert path, 'A path to manifest file not found'
        self._path = os.path.join(path, self.FILE_NAME) if os.path.isdir(path) else path
        self._is_created = is_created

    @property
    def path(self):
        return self._path

    @property
    def is_created(self):
        return self._is_created

    @is_created.setter
    def is_created(self, value):
        assert isinstance(value, bool)
        self._is_created = value

# Needed for faster iteration over the manifest file, will be generated to work inside CVAT
# and will not be generated when manually creating a manifest
class _Index:
    FILE_NAME = 'index.json'

    def __init__(self, path):
        assert path and os.path.isdir(path), 'No index directory path'
        self._path = os.path.join(path, self.FILE_NAME)
        self._index = {}

    @property
    def path(self):
        return self._path

    def dump(self):
        with open(self._path, 'w') as index_file:
            json.dump(self._index, index_file,  separators=(',', ':'))

    def load(self):
        with open(self._path, 'r') as index_file:
            self._index = json.load(index_file,
                object_hook=lambda d: {int(k): v for k, v in d.items()})

    def create(self, manifest, skip):
        assert os.path.exists(manifest), 'A manifest file not exists, index cannot be created'
        with open(manifest, 'r+') as manifest_file:
            while skip:
                manifest_file.readline()
                skip -= 1
            image_number = 0
            position = manifest_file.tell()
            line = manifest_file.readline()
            while line:
                if line.strip():
                    self._index[image_number] = position
                    image_number += 1
                    position = manifest_file.tell()
                line = manifest_file.readline()

    def partial_update(self, manifest, number):
        assert os.path.exists(manifest), 'A manifest file not exists, index cannot be updated'
        with open(manifest, 'r+') as manifest_file:
            manifest_file.seek(self._index[number])
            line = manifest_file.readline()
            while line:
                if line.strip():
                    self._index[number] = manifest_file.tell()
                    number += 1
                line = manifest_file.readline()

    def __getitem__(self, number):
        assert 0 <= number < len(self), \
            'A invalid index number: {}\nMax: {}'.format(number, len(self))
        return self._index[number]

    def __len__(self):
        return len(self._index)

class _ManifestManager(ABC):
    BASE_INFORMATION = {
        'version' : 1,
        'type': 2,
    }
    def __init__(self, path, *args, **kwargs):
        self._manifest = _Manifest(path)

    def _parse_line(self, line):
        """ Getting a random line from the manifest file """
        with open(self._manifest.path, 'r') as manifest_file:
            if isinstance(line, str):
                assert line in self.BASE_INFORMATION.keys(), \
                    'An attempt to get non-existent information from the manifest'
                for _ in range(self.BASE_INFORMATION[line]):
                    fline = manifest_file.readline()
                return json.loads(fline)[line]
            else:
                assert self._index, 'No prepared index'
                offset = self._index[line]
                manifest_file.seek(offset)
                properties = manifest_file.readline()
                return json.loads(properties)

    def init_index(self):
        self._index = _Index(os.path.dirname(self._manifest.path))
        if os.path.exists(self._index.path):
            self._index.load()
        else:
            self._index.create(self._manifest.path, 3 if self._manifest.TYPE == 'video' else 2)
            self._index.dump()

    @abstractmethod
    def create(self, content, **kwargs):
        pass

    @abstractmethod
    def partial_update(self, number, properties):
        pass

    def __iter__(self):
        with open(self._manifest.path, 'r') as manifest_file:
            manifest_file.seek(self._index[0])
            image_number = 0
            line = manifest_file.readline()
            while line:
                if not line.strip():
                    continue
                yield (image_number, json.loads(line))
                image_number += 1
                line = manifest_file.readline()

    @property
    def manifest(self):
        return self._manifest

    def __len__(self):
        if hasattr(self, '_index'):
            return len(self._index)
        else:
            return None

    def __getitem__(self, item):
        return self._parse_line(item)

    @property
    def index(self):
        return self._index

#TODO: add generic manifest structure file validation
class ManifestValidator:
    def validate_base_info(self):
        with open(self._manifest.path, 'r') as manifest_file:
            assert self._manifest.VERSION != json.loads(manifest_file.readline())['version']
            assert self._manifest.TYPE != json.loads(manifest_file.readline())['type']

class ImageManifestManager(_ManifestManager):
    def __init__(self, manifest_path):
        super().__init__(manifest_path)
        setattr(self._manifest, 'TYPE', 'images')

    def create(self, content, **kwargs):
        """ Creating and saving a manifest file"""
        with open(self._manifest.path, 'w') as manifest_file:
            base_info = {
                'version': self._manifest.VERSION,
                'type': self._manifest.TYPE,
            }
            for key, value in base_info.items():
                json_item = json.dumps({key: value}, separators=(',', ':'))
                manifest_file.write(f'{json_item}\n')

            for item in content:
                json_item = json.dumps({
                    key: value for key, value in item.items()
                }, separators=(',', ':'))
                manifest_file.write(f"{json_item}\n")
        self._manifest.is_created = True

    def partial_update(self, number, properties):
        pass

    @staticmethod
    def prepare_meta(sources, **kwargs):
        meta_info = DatasetImagesReader(sources=sources, **kwargs)
        meta_info.create()
        return meta_info