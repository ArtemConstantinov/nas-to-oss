import os
import glob
import oss2
from .queue_object import QueueObject


def ScanNotInBucket(auth, settings, queue):
    """
    Compare OSS bucket with local storage and put in to queue for uploading files what is not esist in bucket
    :param auth: < Auth > object  
    :param settings: < AppSettings > object
    :param queue: link to queue for uploading
    :return: 
    """
    uploads_targets = settings.uploads_targets
    storage = settings.watching_dir
    for bucket_info in uploads_targets:
        bucket_name = bucket_info.get("bucket_name")
        bucket_endpoint = bucket_info.get("endpoint")
        bucket = oss2.Bucket(auth, bucket_endpoint, bucket_name)
        objects_in_oss = bucket.list_objects()
        files_keys = list()
        for obj in objects_in_oss.object_list:
            files_keys.append(obj.key)

        folders = bucket_info.get("nas_folders")
        for folder in folders:
            fileslist = glob.glob(storage + folder + "/*.*")
            for f in fileslist:
                if os.path.basename(f).encode("utf-8") not in files_keys:
                    object_to_upload = QueueObject(f, bucket_name, bucket_endpoint)
                    queue.put(object_to_upload)
                    print object_to_upload

