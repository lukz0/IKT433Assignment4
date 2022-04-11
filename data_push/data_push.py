import zipfile
import re
import random
import os

import pika
from PIL import Image
import bson


MAIN_EXCHANGE_NAME = os.getenv('MAIN_EXCHANGE_NAME')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')

GET_LABEL_RE = re.compile(r'\d+')


def get_zipinfos(zf: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    return list(
        zi for zi in zf.filelist if not zi.is_dir() and zi.filename.startswith('training/')
    )


def send_file_loop(
    zf: zipfile.ZipFile,
    zipinfos: list[zipfile.ZipInfo],
    chan  # BlockingConnection
) -> None:
    while True:
        zipinfo = random.sample(zipinfos, 1)[0]
        with zf.open(zipinfo, 'r', force_zip64=True) as img_reader, \
                Image.open(img_reader).convert('RGB') as img:
            chan.basic_publish(
                exchange=MAIN_EXCHANGE_NAME,
                routing_key='',
                body=bson.dumps({
                    'img': img.tobytes(),
                    'label': int(GET_LABEL_RE.search(zipinfo.filename).group()),
                    'img_mode': img.mode,
                    'img_size': img.size
                }),
                properties=pika.BasicProperties(
                    headers={
                        'is_rotated': 'false',
                        'is_grayscale': 'false',
                        'is_mirrored': 'false',
                        'is_resized': 'false'
                    }
                )
            )


def run(zf: zipfile.ZipFile, conn: pika.BlockingConnection) -> None:
    zipinfos: list[zipfile.ZipInfo] = get_zipinfos(zf)
    with conn.channel() as chan:
        chan.exchange_declare(
            exchange=MAIN_EXCHANGE_NAME,
            exchange_type='headers'
        )
        send_file_loop(zf, zipinfos, chan)


if __name__ == '__main__':
    while True:
        try:
            with zipfile.ZipFile('food-11.zip', allowZip64=True) as zf, \
                pika.BlockingConnection(
                    pika.ConnectionParameters(
                        RABBITMQ_HOST
                    )
            ) as conn:
                run(zf, conn)
        except pika.exceptions.AMQPConnectionError as err:
            print(err)
