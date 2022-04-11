# Installation instructions

## How to run this

1: Download the dataset from https://www.kaggle.com/datasets/vermaavi/food11 as "data_push/food-11.zip"
2: docker-compose build --no-cache
3: docker-compose up

## To run data_pull.py

1: Install the bson==0.5.10, pika==1.2.0 and Pillow==9.1.0 libraries
2: python3 data_pull.py
