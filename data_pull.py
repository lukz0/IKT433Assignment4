import zipfile
import re
import random
import os

import pika
from PIL import Image, ImageOps
import bson


def callback(chan, method, properties, body):
    headers: dict = properties.headers
    body = bson.loads(body)
    img_mode = body['img_mode']
    img_size = body['img_size']
    img = Image.frombytes(mode=img_mode, size=img_size, data=body['img'])
    label = body['label']

    print(
        f'Received img:\n\tlabel: {label}\n\tmode: {img.mode}\n\tsize:{img.size}\n\theaders: {headers}'
    )


def run(chan) -> None:
    chan.exchange_declare(
        exchange='Exchange',
        exchange_type='headers'
    )
    chan.basic_qos(prefetch_count=1)
    chan.queue_declare(
        queue='process_completed',
        arguments={
            'x-max-length': 10
        }
    )
    chan.queue_bind(
        queue='process_completed',
        exchange='Exchange',
        routing_key='',
        arguments={
            'x-match': 'all',
            'is_resized': 'true',
            'is_rotated': 'true',
            'is_grayscale': 'true',
            'is_mirrored': 'true',
        }
    )

    chan.basic_consume(
        queue='process_completed',
        auto_ack=True,  # An image getting lost shouldn't be dangerous...
        on_message_callback=callback
    )
    chan.start_consuming()


if __name__ == '__main__':
    while True:
        try:
            with pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='127.0.0.1',
                    port=5672
                )
            ) as conn, \
                    conn.channel() as chan:
                run(chan)
        except pika.exceptions.AMQPConnectionError as err:
            print(err)
            import sys
            print(repr(sys.exc_info()[0].args))
