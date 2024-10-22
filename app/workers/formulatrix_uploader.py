from file_worker import EFWorker
from file_worker import ZWorker
import asyncio

class FormulatrixUploader():

    async def process_job(self, file_list, config, engine):
        worker_type = config["task"]        
        worker = await self.create_worker(worker_type, config, engine)

        results = [await worker.process_file(file) for file in file_list]
        
        return results
    
    async def create_worker(self, worker_type, config, engine):
        if worker_type == 'Z':
            return ZWorker(config, engine)
        elif worker_type == 'EF':
            return EFWorker(config, engine)
        else:
            raise ValueError(f"Unknown worker type: {worker_type}")