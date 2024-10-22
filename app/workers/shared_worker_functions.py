import os
import subprocess
import time
import shutil
from PIL import Image
import asyncio
from sqlalchemy import text
import logging
import sys
import logging.handlers
from collections import namedtuple
import pika
import json
import uuid


logger = logging.getLogger()

async def get_visit_dir(container, config):
    visit = container["visit"]
    proposal = visit[: visit.index("-")]
    new_root = f"{config['upload_dir']}/{proposal}/{visit}"
    old_root = f"{config['upload_dir']}/{container['year']}/{visit}"
    return old_root if os.path.exists(old_root) else new_root if os.path.exists(new_root) else None

def move_unhandled(files_target):
    image, xml, target = files_target[0], files_target[1], files_target[2]
    
    for file in [image,xml]:
        os.rename(file, f"{target}/{os.path.basename(file)}")

def transpose_and_save(f, new_f):
    im = Image.open(f)
    im_flipped = im.transpose(Image.FLIP_TOP_BOTTOM)
    im_flipped.save(new_f)
    return im_flipped

def file_ready(f): return True if time.time() - os.stat(f).st_mtime > 10 and os.stat(f).st_size > 0 else False

async def rmdir(src_dir):
    try:
        os.rmdir(src_dir)
        logger.info(f"Trying to rm {src_dir}")
    except OSError:
        logger.error(f"Could not rm {src_dir}, as it is not empty")
        pass

async def make_dirs(path, config):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            if config.get("web_user"):
                subprocess.call(
                    [
                        "/usr/bin/setfacl",
                        "-R",
                        "-m",
                        "u:" + config["web_user"] + ":rwx",
                        path,
                    ]
                )
        except NotADirectoryError:
            return False

        except OSError:
            # if exc.errno == errno.EEXIST and os.path.isdir(new_path):
            #     pass
            # elif exc.errno == errno.EACCES:
            #     logger.error("%s - %s" % (exc.strerror, new_path))
            #     return False
            # else:
            #     raise
            raise  # This code was broken and always raised

    return True

def move_dir(f, target_dir):
    if file_ready(f):
        new_f = os.path.join(target_dir, os.path.basename(f))
        try:
            old_file, ext = os.path.splitext(f)
            if ext == ".tif":
                transpose_and_save(f,new_f)
            else:
                shutil.copyfile(f, new_f)
            try:
                os.unlink(f)
            except IOError:
                logger.error(f"Error deleting image file {f})")
                pass
        except IOError:
            logger.error(
                f"Error flipping/copying image file {f} to {new_f}"
            )
            pass
    else:
        logger.info(f"Not moving file {f} yet")
        pass
    return

def move_file(xml, new_f, inspectionId, location, config):
    image = xml.replace(".xml",".jpg")
    try:
        flip = transpose_and_save(image,new_f)
        file, ext = os.path.splitext(new_f)
        
        try:
            flip.thumbnail(
                (config["thumb_width"], config["thumb_height"])
            )
            try:
                #flip.save(f"{file}th{ext}")
                pass
            except IOError:
                print("Error saving file")
                return xml, None, inspectionId, location
            try:
                #os.unlink(image)
                #os.unlink(xml) ## Currently, nothing goes 
                return xml, new_f, inspectionId, location
            except:
                return xml, None, inspectionId, location
        except IOError:
            return xml, None, inspectionId, location
    except IOError:
        return xml, None, inspectionId, location
    
async def retrieve_container_for_barcode(barcode, session):
    async with session() as connection:
        async with connection.begin():
            result = await connection.execute(text(f'SELECT concat(p.proposalCode, p.proposalNumber, "-", bs.visit_number) "visit", date_format(c.blTimeStamp, "%Y") "year" FROM Container c LEFT OUTER JOIN BLSession bs ON bs.sessionId = c.sessionId LEFT OUTER JOIN Proposal p ON p.proposalId = bs.proposalId WHERE c.barcode="{barcode}" LIMIT 1;'))
        return result.mappings().all()[0]

async def retrieve_container_for_inspectionId(inspectionId, session, sem):
    async with sem:
        async with session() as connection:
            async with connection.begin():
                result = await connection.execute(text(f'SELECT c.containerType, c.containerId, c.sessionId, concat(p.proposalCode, p.proposalNumber, "-", bs.visit_number) "visit", date_format(c.blTimeStamp, "%Y") as year FROM Container c INNER JOIN ContainerInspection ci ON ci.containerId = c.containerId INNER JOIN Dewar d ON d.dewarId = c.dewarId INNER JOIN Shipping s ON s.shippingId = d.shippingId INNER JOIN Proposal p ON p.proposalId = s.proposalId LEFT OUTER JOIN BLSession bs ON bs.sessionId = c.sessionId WHERE ci.containerinspectionId = {inspectionId} LIMIT 1;'))
            return {inspectionId: result.mappings().all()[0]}

async def retrieve_sample_for_container_id_and_location(p_location, p_containerId, session):
    async with session() as connection:
        async with connection.begin():
            result = await connection.execute(text(f'SELECT blSampleId FROM BLSample WHERE containerId="{p_containerId}" AND location="{p_location}" LIMIT 1;'))
    return result.fetchone()

def set_logging(logs):
    levels_dict = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    logger.setLevel(logging.INFO)
    for log_name in logs:
        handler = None
        if log_name == "syslog":
            handler = logging.handlers.SysLogHandler(
                address=(logs[log_name]["host"], logs[log_name]["port"])
            )
        elif log_name == "rotating_file":
            handler = logging.handlers.RotatingFileHandler(
                filename=logs[log_name]["filename"],
                maxBytes=logs[log_name]["max_bytes"],
                backupCount=logs[log_name]["no_files"],
            )
        else:
            sys.exit(
                "Invalid logging mechanism defined in config: %s. (Valid options are syslog and rotating_file.)"
                % log_name
            )

        handler.setFormatter(logging.Formatter(logs[log_name]["format"]))
        level = logs[log_name]["level"]
        if levels_dict[level]:
            handler.setLevel(levels_dict[level])
        else:
            handler.setLevel(logging.WARNING)
        logger.addHandler(handler)

def publish(job, channel, callback):
    
    payload = {
        "plate": f"{job.inspectionId}",
        "well": int(job.location),
        "image_path": f"{job.new_path}"
    }

    delivery = json.dumps(payload)

    channel.basic_publish(
        exchange="",
        routing_key="jobs",
        properties=pika.BasicProperties(
            reply_to=callback
        ),
        body=delivery
    )
    print(f"Sent job: {delivery}")