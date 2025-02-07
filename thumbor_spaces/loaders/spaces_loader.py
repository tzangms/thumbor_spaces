from os import fstat
from datetime import datetime
from os.path import join, exists, abspath

from six.moves.urllib.parse import unquote
from tornado import gen

from thumbor.loaders import http_loader
from thumbor.loaders import LoaderResult

from thumbor.utils import logger

import boto3
from botocore.client import Config


@gen.coroutine
def load(context, path):
    logger.debug("INSIDE SPACES LOADER")
    logger.debug(path)
    logger.debug(context.request.url)
    chker1 = context.config.SPACES_BUCKET+'.'+context.config.SPACES_ENDPOINT+'.digitaloceanspaces.com'
    chker2 = '/'+context.config.SPACES_LOADER_FOLDER+'/'
    if chker1 not in context.request.url and chker2 not in context.request.url:
        logger.debug(path)
        result = yield http_loader.load(context, path)
        raise gen.Return(result)
    else:
        key = get_key_name(context, context.request.url)
        session = boto3.session.Session()
        client = session.client('s3',
                                    region_name=context.config.SPACES_REGION,
                                    endpoint_url='https://'+context.config.SPACES_ENDPOINT+'.digitaloceanspaces.com',
                                    aws_access_key_id=context.config.SPACES_KEY,
                                    aws_secret_access_key=context.config.SPACES_SECRET)
        url = client.generate_presigned_url(ClientMethod='get_object', 
                                                Params={
                                                    'Bucket': context.config.SPACES_BUCKET,
                                                    'Key': key
                                                }, ExpiresIn=300)
        logger.debug(url)
        result = yield http_loader.load(context, url)
        raise gen.Return(result)

def get_key_name(context, path):
    path_segments = path.lstrip('/').split("/")
    storage_index = index_containing_substring(path_segments, context.config.SPACES_LOADER_FOLDER)
    return '/'.join(path_segments[storage_index:])

def index_containing_substring(the_list, substring):
    for i, s in enumerate(the_list):
        if substring in s:
              return i
    return -1