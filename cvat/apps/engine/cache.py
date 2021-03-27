# Copyright (C) 2020-2021 Intel Corporation
#
# SPDX-License-Identifier: MIT

import os
from io import BytesIO

from diskcache import Cache
from django.conf import settings

from cvat.apps.engine.media_extractors import (ZipChunkWriter, ZipCompressedChunkWriter,
    ImageDatasetManifestReader)
from cvat.apps.engine.models import DataChoice, StorageChoice
from cvat.apps.engine.models import DimensionType

class CacheInteraction:
    def __init__(self, dimension=DimensionType.DIM_2D):
        self._cache = Cache(settings.CACHE_ROOT)
        self._dimension = dimension

    def __del__(self):
        self._cache.close()

    def get_buff_mime(self, chunk_number, quality, db_data):
        chunk, tag = self._cache.get('{}_{}_{}'.format(db_data.id, chunk_number, quality), tag=True)

        if not chunk:
            chunk, tag = self.prepare_chunk_buff(db_data, quality, chunk_number)
            self.save_chunk(db_data.id, chunk_number, quality, chunk, tag)
        return chunk, tag

    def prepare_chunk_buff(self, db_data, quality, chunk_number):
        from cvat.apps.engine.frame_provider import FrameProvider # TODO: remove circular dependency
        writer_classes = {
            FrameProvider.Quality.COMPRESSED : ZipCompressedChunkWriter,
            FrameProvider.Quality.ORIGINAL : ZipChunkWriter,
        }

        image_quality = 100 if writer_classes[quality] in [ZipChunkWriter] else db_data.image_quality
        mime_type = 'application/zip'

        kwargs = {}
        if self._dimension == DimensionType.DIM_3D:
            kwargs["dimension"] = DimensionType.DIM_3D
        writer = writer_classes[quality](image_quality, **kwargs)

        images = []
        buff = BytesIO()
        upload_dir = {
                StorageChoice.LOCAL: db_data.get_upload_dirname(),
                StorageChoice.SHARE: settings.SHARE_ROOT
            }[db_data.storage]

        reader = ImageDatasetManifestReader(manifest_path=db_data.get_manifest_path(),
            chunk_number=chunk_number, chunk_size=db_data.chunk_size,
            start=db_data.start_frame, stop=db_data.stop_frame,
            step=db_data.get_frame_step())
        for item in reader:
            source_path = os.path.join(upload_dir, f"{item['name']}{item['extension']}")
            images.append((source_path, source_path, None))

        writer.save_as_chunk(images, buff)
        buff.seek(0)
        return buff, mime_type

    def save_chunk(self, db_data_id, chunk_number, quality, buff, mime_type):
        self._cache.set('{}_{}_{}'.format(db_data_id, chunk_number, quality), buff, tag=mime_type)
