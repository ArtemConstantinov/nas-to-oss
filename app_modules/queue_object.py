import os
from hurry.filesize import size


class QueueObject:
    def __init__(self, filepath, bucket_name, endpoint):
        self.key = os.path.basename(filepath)
        self.path = filepath.encode("utf-8")
        self.bucket_name = bucket_name
        self.endpoint = endpoint

    def __str__(self):
        return "{0} - {1}".format(self.key.encode("utf-8"), size(os.stat(self.path).st_size).encode("utf-8"))

    def __unicode__(self):
        return "{0} - {1}".format(self.key.encode("utf-8"), size(os.stat(self.path).st_size).encode("utf-8"))
