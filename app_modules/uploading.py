import os
import sys
import oss2
from oss2.exceptions import NoSuchUpload
import threading
import cPickle as pickle
import logging

# logging.basicConfig(level=logging.DEBUG,
#                     format='(%(threadName)-10s) %(message)s',
#                     )


class UploadingObject:
    """
    Structure object for save information about upload to pkl file
    """
    key = ""
    path = ""
    upload_id = ""
    initiate_date = ""
    part_number = 1
    parts = []
    offset = 0

    def __unicode__(self):
        return self.key

    def __str__(self):
        return self.key


class UploadIdNotInBucket(Exception):
    def __init__(self, *args, **kwargs):
        super(UploadIdNotInBucket, self).__init__(*args, **kwargs)


class UploadThread(threading.Thread):
    """
    Thread class for upload one file in multiple partition
    """
    def __init__(self, queue_object, auth):
        """
        
        :param queue_object: < QueueObject > object with information about uploading file and target
        :param auth: < Auth > object
        """
        threading.Thread.__init__(self)
        self.queue_object = queue_object
        self._auth = auth
        self.pkl_path = './temp_pkl/{0}.pkl'.format(self.queue_object.path.replace("./nas/", "").replace("/", "_"))
        self.stoprequest = threading.Event()

    def run(self):
        self.uploadFile()

    def join(self, timeout=None):
        self.stoprequest.set()
        super(UploadThread, self).join(timeout)

    def uploadFile(self):
        """
        Function check if cache file with uploading information is exist and try to continue uplod otherwise start new 
        file uploading to OSS
        :return: 
        """
        total_size = os.stat(self.queue_object.path).st_size
        part_size = oss2.determine_part_size(total_size, preferred_size=2 * 1024 * 1024)

        try:
            with open(self.pkl_path, 'rb') as input_file:  # We try to open file with title of uploading key
                storied_object = pickle.load(input_file)  # if file is open means upload was not finished
                parts = storied_object.parts
                upload_id = storied_object.upload_id

                bucket_name = storied_object.bucket_name
                endpoint = storied_object.endpoint
                bucket = oss2.Bucket(self._auth, endpoint, bucket_name)
                # offset = storied_object.offset
        except IOError:
            bucket = oss2.Bucket(self._auth, self.queue_object.endpoint, self.queue_object.bucket_name)
            upload_id = bucket.init_multipart_upload(self.queue_object.key).upload_id
            storied_object = UploadingObject()
            uploade_parts = bucket.list_multipart_uploads()

            for part in uploade_parts.upload_list:
                if part.upload_id == upload_id:
                    with open(self.pkl_path, 'wb') as output_file:
                        storied_object.key = self.queue_object.key
                        storied_object.bucket_name = self.queue_object.bucket_name
                        storied_object.endpoint = self.queue_object.endpoint
                        storied_object.path = self.queue_object.path
                        storied_object.upload_id = upload_id
                        storied_object.initiate_date = part.initiation_date
                        pickle.dump(storied_object, output_file, pickle.HIGHEST_PROTOCOL)

            parts = []

        with open(self.queue_object.path, 'rb') as fileobj:
            while storied_object.offset < total_size and not self.stoprequest.isSet():
                # print storied_object.part_number
                # print storied_object.parts
                num_to_upload = min(part_size, total_size - storied_object.offset)
                upload_content = oss2.SizedFileAdapter(fileobj, num_to_upload)
                try:
                    result = bucket.upload_part(self.queue_object.key,
                                                upload_id,
                                                storied_object.part_number,
                                                upload_content)
                except NoSuchUpload:
                    print "\n ==== Not finished upload not exist on  OSS bucket ===="
                    print " Clean local cache, and update uploading queue"
                    os.remove(self.pkl_path)
                    raise UploadIdNotInBucket("Upload id is not in bucket")

                # Append directly to class is didn't work with "pickle"
                parts.append(oss2.models.PartInfo(storied_object.part_number, result.etag))
                storied_object.parts = parts

                if num_to_upload == part_size:
                    percentage = str(self._percentage(num_to_upload * storied_object.part_number, total_size))
                else:
                    percentage = 'Complete'

                # print percentage

                # logging.debug("File: %s => Bucket: %s - %s", key, bucket_name, percentage)
                # print "File: {0} => Bucket: {1} - {2}".format(self.queue_object.key.encode("utf-8"),
                #                                               self.queue_object.bucket_name,
                #                                               percentage)

                sys.stdout.write("\rFile: {0} => Bucket: {1} - {2}".format(self.queue_object.key.encode("utf-8"),
                                                              self.queue_object.bucket_name,
                                                              percentage)).splitlines()
                sys.stdout.flush()

                storied_object.offset += num_to_upload

                storied_object.part_number += 1
                with open(self.pkl_path, 'wb') as output_file:
                    pickle.dump(storied_object, output_file, pickle.HIGHEST_PROTOCOL)

            if not self.stoprequest.isSet():
                bucket.complete_multipart_upload(self.queue_object.key, upload_id, parts)
                os.remove(self.pkl_path)

    @staticmethod
    def _percentage(consume_bytes, total_bytes):
        """
        Calculate percentage of how much file was uploaded to oss
        :param consume_bytes: 
        :param total_bytes: 
        :return: < String > 
        """
        if total_bytes:
            rate = int(100 * (float(consume_bytes) / float(total_bytes)))
            return '{0}% '.format(rate)
