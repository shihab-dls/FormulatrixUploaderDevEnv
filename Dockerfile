FROM python:3.10-slim
WORKDIR /usr/local/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app ./uploader
RUN mkdir logs/ && mkdir upload && mkdir archive && mkdir EF && mkdir config

WORKDIR /usr/local/app/uploader/workers

CMD ["python3" ,"./run_file_worker.py"]
