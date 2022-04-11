import zipfile
import re
import random
import os

import pika
from PIL import Image, ImageOps
import bson

MAIN_EXCHANGE_NAME = os.getenv('MAIN_EXCHANGE_NAME')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
PROCESS_QUEUE_NAME = os.getenv('PROCESS_QUEUE_NAME')
TARGET_WIDTH = int(os.getenv('TARGET_WIDTH'))
TARGET_HEIGHT = int(os.getenv('TARGET_HEIGHT'))


def resize_img(img: Image.Image) -> Image.Image:
    return img.resize((TARGET_WIDTH, TARGET_HEIGHT))


def rotate_img(img: Image.Image) -> Image.Image:
    return img.rotate(random.uniform(0, 360))


def grayscale_img(img: Image.Image) -> Image.Image:
    return img.convert('L')


def mirror_img(img: Image.Image) -> Image.Image:
    if random.sample(range(2), 1) == 0:
        return ImageOps.mirror(img)
    return img


def callback(chan, method, properties, body):
    headers: dict = properties.headers
    body = bson.loads(body)
    img_mode = body['img_mode']
    img_size = body['img_size']
    img = Image.frombytes(mode=img_mode, size=img_size, data=body['img'])
    label = body['label']
    if headers.get('is_resized', 'false') != 'true':
        img = resize_img(img)
        headers['is_resized'] = 'true'
    if headers.get('is_rotated', 'false') != 'true':
        img = rotate_img(img)
        headers['is_rotated'] = 'true'
    if headers.get('is_grayscale', 'false') != 'true':
        img = grayscale_img(img)
        headers['is_grayscale'] = 'true'
    if headers.get('is_mirrored', 'false') != 'true':
        img = mirror_img(img)
        headers['is_mirrored'] = 'true'

    chan.basic_publish(
        exchange=MAIN_EXCHANGE_NAME,
        routing_key='',
        body=bson.dumps({
            'img': img.tobytes(),
            'label': label,
            'img_mode': img.mode,
            'img_size': img.size
        }),
        properties=pika.BasicProperties(
            headers=headers
        )
    )


def run(chan) -> None:
    chan.exchange_declare(
        exchange=MAIN_EXCHANGE_NAME,
        exchange_type='headers'
    )
    chan.basic_qos(prefetch_count=1)
    chan.queue_declare(
        queue=PROCESS_QUEUE_NAME,
        arguments={
            'x-max-length': 10
        }
    )
    chan.queue_bind(
        queue=PROCESS_QUEUE_NAME,
        exchange=MAIN_EXCHANGE_NAME,
        routing_key='',
        arguments={
            'x-match': 'any',
            'is_resized': 'false',
            'is_rotated': 'false',
            'is_grayscale': 'false',
            'is_mirrored': 'false',
        }
    )

    chan.basic_consume(
        queue=PROCESS_QUEUE_NAME,
        auto_ack=True,  # An image getting lost shouldn't be dangerous...
        on_message_callback=callback
    )
    chan.start_consuming()


if __name__ == '__main__':
    while True:
        try:
            with pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST
                )
            ) as conn, \
                    conn.channel() as chan:
                run(chan)
        except pika.exceptions.AMQPConnectionError as err:
            print(err)
