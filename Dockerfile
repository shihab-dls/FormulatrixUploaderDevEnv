FROM python:3.10-slim
WORKDIR /usr/local/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app ./uploader
RUN mkdir logs/ && mkdir upload && mkdir archive && mkdir EF && mkdir config

WORKDIR /usr/local/app/uploader/workers

CMD ["python3" ,"./run_file_worker.py","--rabbitmq_creds_path","../../config/rabbitmq.json","--credentials_path","../../config/dbconf.json","--config_file_ef","../../config/config_ef.json","--config_file_z","../../config/config_z.json"]
