import os
import glob
from itertools import repeat
from shared_worker_functions import *
import asyncio
from asyncio import Semaphore
from concurrent.futures import ProcessPoolExecutor
import xml.etree.ElementTree as ET
import re
import tqdm
import tqdm.asyncio
from collections import namedtuple
import logging
import logging.handlers
import uuid
import subprocess

logger = logging.getLogger()
ZOCALO_SCRIPT = "/dls_sw/i02-2/software/cronjobs/run_zoc_image_pipeline.sh"

class ZWorker:
    def __init__(self, config, session):
        self.config = config
        self.session = session
        set_logging(config["logging"])

    async def process_file(self, date_dirs, up_files_out_dir):
        logger.info(f"Date directories found: {date_dirs}")
        container_dict = self.get_container_dict(date_dirs)            
        ## Handle each barcode dir, providing an executor for each
        with ProcessPoolExecutor() as exe:
           await asyncio.gather(*(self.get_target_and_move(barcode, container_dict, exe) for barcode in container_dict))    
        
        ## Remove date dirs if empty
        await asyncio.gather(*(rmdir(date_dir) for date_dir in date_dirs))

        return f"Processed dates: {date_dirs}"

    def get_container_dict(self,date_dirs):
        container_dir = dict()
        for date_dir in date_dirs:
            barcode_dirs = glob.glob(os.path.abspath(f"{date_dir}/*/"))
            for barcode_dir in barcode_dirs:
                barcode = os.path.basename(os.path.abspath(barcode_dir))
                container_dir[barcode] = os.path.dirname(barcode_dir)
        return container_dir

    async def get_target_and_move(self,barcode, container_dict, exe):
        container = await retrieve_container_for_barcode(barcode, self.session)
    
        if not container:
            logger.error(f"Could not find container in database for {barcode}")
        
        if container["visit"] is None:
            logger.error(f"Container barcode {barcode} has no session")
            return
        else:
            logger.info(f"{barcode} visit directory: {container['visit']}")
        
        visit_dir = await get_visit_dir(container, self.config)
        
        if not visit_dir:
            logger.error(f"Could not find visit path for container barcode {barcode}")
            return
    
        target_dir = os.path.join(visit_dir, "tmp", barcode)

        if not await make_dirs(target_dir, self.config):
            logger.error(f"Could not make dir: {target_dir} for {barcode}")
            return

        src_dir = (f"{container_dict[barcode]}/{barcode}")
        files = glob.glob(f"{src_dir}/*")
        logger.info(f"trying to glob.glob({src_dir})")
        
        # tasks = []
        # for i in files:
        #     loop = asyncio.get_event_loop()
        #     task = loop.run_in_executor(exe, move_dir, i, target_dir)
        #     tasks.append(task)


        # progress = [await task for task in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        
        await rmdir(src_dir)

class EFWorker:

    def __init__(self, config, session, channel, callback_queue):
        self.config = config
        self.session = session
        set_logging(config["logging"])
        self.channel = channel
        self.callback_queue = callback_queue

    async def process_file(self, ef_files, up_files_out_dir):
        unhandled_files = []

        jpg_files = [jpg for jpg in ef_files if jpg.split(".")[1] == 'jpg']
        xml_files = [xml for xml in ef_files if xml.split(".")[1] == 'xml']
        xml_valid = [xml for xml in xml_files if xml.replace(".xml",".jpg") in jpg_files]
        
        for xml in xml_files:
            if xml not in xml_valid:
                logger.error(f"Corresponding image not found for {xml}")
                unhandled_files.append(xml)

        xml_valid.sort(key=os.path.getmtime, reverse=True)
        xml_datum = namedtuple("xml_data", "xml inspectionId root nss")
        xml_data = []
        unique_inspection_id = set()
        for xml in xml_valid:
            tree = ET.parse(xml)
            root = tree.getroot()
            ns = root.tag.split("}")[0].strip("{")
            nss = {"oppf": ns}
            inspectionId = re.sub("\-.*", "", root.find("oppf:ImagingId", nss).text)
            logger.info(f"Inspection: {inspectionId} found for {xml}")
            xml_data.append(xml_datum(xml,inspectionId,root, nss))
            unique_inspection_id.add(inspectionId)

        containers = []
        containers_dict = dict()
        for inspectionId in unique_inspection_id:
            containers.append(await retrieve_container_for_inspectionId(inspectionId, self.session))

        for i in containers:
            for key, value in i.items():
                containers_dict[key] = value

        xml_datum_with_container = namedtuple("xml_datum_with_container", "xml_datum container")
        xml_data_with_container = []

        for xml_datum in xml_data:
            xml_data_with_container.append(
                xml_datum_with_container(
                    xml_datum,
                    containers_dict[xml_datum.inspectionId])
                )
        
        n_files = len(xml_files)
        max_files = self.config["max_files"]

        if n_files > max_files:
            print("Too many files")

        n_batch = self.config["max_files_in_batch"]

        ## Handle checks and queries for valid xml files
        xml_paths_with_id_location = namedtuple("xml_paths_with_id_location", "old_path new_path inspectionId location")
        results = []
        for xml_datum_with_container in xml_data_with_container:
            results.append(await self.handle_file(xml_datum_with_container, xml_paths_with_id_location, self.session))

        ## Unhandled files do not have a new path assigned
        unhandled_files.append([result for result in results if not result.new_path])
        processing_files = [result for result in results if result.new_path]

        tasks = []
        with ProcessPoolExecutor() as exe:
            for file in processing_files:
                loop = asyncio.get_event_loop()
                task = loop.run_in_executor(exe, move_file, file.old_path, file.new_path, file.inspectionId, file.location, self.config)
                tasks.append(task)        
        progress = [await task for task in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        
        results = [xml_paths_with_id_location(task.result()[0],task.result()[1], task.result()[2], task.result()[3]) for task in tasks]

        unhandled_files.append([result for result in results if not result.new_path])
        handled_files = [result for result in results if result.new_path]

        # Generate a list of handled files
        rand_id = str(uuid.uuid4())
        up_files_out_path = f"{up_files_out_dir}/{rand_id}.txt"
        with open(up_files_out_path, "w") as fh:
            fh.write("\n".join(str(file.new_path) for file in handled_files))
            fh.write("\n")
        
        logger.info(f"Saved list of {len(handled_files)} files to {up_files_out_path}")

        # sp_output = subprocess.run([ZOCALO_SCRIPT, rand_id], capture_output=True)
        # logger.info(sp_output.stdout.decode("utf-8"))

        return f"Processed Inspection IDs: [{unique_inspection_id}]"

    async def handle_file(self, xml_datum_with_container, xml_paths_with_id_location, session):
        xml = xml_datum_with_container.xml_datum.xml
        inspectionid = xml_datum_with_container.xml_datum.inspectionId
        root = xml_datum_with_container.xml_datum.root
        nss = xml_datum_with_container.xml_datum.nss
        container = xml_datum_with_container.container
        image = xml.replace(".xml",".jpg")


        if not file_ready(xml):
            return xml_paths_with_id_location(xml,None)

        if not container:
            target_dir = f"{self.config['holding_dir']}/nosession"
            files_target = [image,xml,target_dir]
            logger.error(f"Could not find container in database for {inspectionid}")
            move_unhandled(files_target)
            return xml_paths_with_id_location(xml,None, inspectionid, None)

        visit_dir = await get_visit_dir(container, self.config)

        if not visit_dir:
            logger.error(f"No visit directory for {inspectionid}")
            return xml_paths_with_id_location(xml,None, inspectionid, None)

        new_path = f"{visit_dir}/imaging/{container['containerId']}/{inspectionid}"

        if not await make_dirs(new_path,self.config):
            logger.error(f"Could not make dir: {target_dir} for {inspectionid}")
            return xml_paths_with_id_location(xml,None)

        position = self.get_position(
            root.find("oppf:Drop", nss).text,
            container["containerType"],)
        
        if not position:
            logger.error(f"Could not math drop for {xml}")
            return xml_paths_with_id_location(xml,None, inspectionid, None)
                    
        sampleid = await retrieve_sample_for_container_id_and_location(position, container["containerId"], self.session)
    
        if not sampleid:
            target_dir = f"{self.config['holding_dir']}/nosample"
            files_target = [image,xml,target_dir]
            logger.error(f"Couldnt find a blsample for containerid: {container['containerId']}, position: {position}")
            move_unhandled(files_target)
            return xml_paths_with_id_location(xml,None, inspectionid, position)
        else:
            sampleid = sampleid[0]

        mppx, mppy = self.get_mpp_coords(root, nss)

        iid = await upsert_sample_image(
            p_id= None,
            p_sampleId= str(sampleid),
            p_micronsPerPixelX = str(mppx),
            p_micronsPerPixelY= str(mppy),
            p_containerInspectionId = str(inspectionid),
            session=None # dev purposes
        )

        new_file = f"{new_path}/{iid}.jpg"

        await upsert_sample_image(
            p_id= iid,
            p_imageFullPath= str(new_file),
            session= None
        )

        return xml_paths_with_id_location(xml,new_file, inspectionid, position)
                    
    def get_mpp_coords(self, root, nss):
        mppx = float(
            root.find("oppf:SizeInMicrons", nss).find("oppf:Width", nss).text
        ) / float(
            root.find("oppf:SizeInPixels", nss).find("oppf:Width", nss).text
        )
        mppy = float(
            root.find("oppf:SizeInMicrons", nss).find("oppf:Height", nss).text
        ) / float(
            root.find("oppf:SizeInPixels", nss).find("oppf:Height", nss).text
        )
        return mppx, mppy

    def get_position(self, text_position, platetype):
        well, drop = text_position.split(".")

        drop = int(drop)
        row = ord(well[0]) - 65
        col = int(well[1:]) - 1

        # Need to know what type of plate this is to know how many columns its got
        # This should be in the database, currently in json format embedded in this collection:
        # http://ispyb.diamond.ac.uk/beta/client/js/modules/shipment/collections/platetypes.js
        if platetype not in self.config["types"]:
            #logger.error("Unknown plate type: %s" % platetype)
            print("Unknown plate type")
            return

        ty = self.config["types"][platetype]

        # Position is a linear sequence left to right across the plate
        return (
            (ty["well_per_row"] * row * ty["drops_per_well"])
            + (col * ty["drops_per_well"])
            + (drop - 1)
            + 1
        )