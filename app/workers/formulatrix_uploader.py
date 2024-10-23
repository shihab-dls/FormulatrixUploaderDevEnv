from file_worker import EFWorker
from file_worker import ZWorker
import asyncio
import pika
import json

class FormulatrixUploader():

    async def process_job(self, file_list, config, engine, rabbitmq_creds_path, up_files_out_dir):

        worker_type = config["task"]        
        worker = await self.create_worker(worker_type, config, engine, rabbitmq_creds_path)

        results = [await worker.process_file(file, up_files_out_dir) for file in file_list]
        
        return results
    
    async def create_worker(self, worker_type, config, engine, rabbitmq_creds_path):

        if worker_type == 'Z':
            return ZWorker(config, engine)
        
        elif worker_type == 'EF':
            with open(rabbitmq_creds_path, 'r') as j:
                rabbitmq_creds = json.loads(j.read())

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                rabbitmq_creds["server"], 
                rabbitmq_creds["port"], 
                rabbitmq_creds["vhost"], 
                pika.PlainCredentials(
                    rabbitmq_creds["user"], 
                    rabbitmq_creds["password"])))
            
            channel = connection.channel()
            channel.queue_declare(queue=rabbitmq_creds["job_queue"])
            result = channel.queue_declare(queue=rabbitmq_creds["result_queue"])
            callback_queue = result.method.queue
            return EFWorker(config, engine, channel, callback_queue)
        else:
            raise ValueError(f"Unknown worker type: {worker_type}")