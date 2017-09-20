
import time
import os

import Queue
import threading

import oss2

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from app_modules.settings import AppSettings
from app_modules.uploading import UploadThread, UploadIdNotInBucket
from app_modules.utils import ScanNotInBucket
from app_modules.queue_object import QueueObject

auth = ''


class Watcher:
    """
    Class watching change events in specified folder
    """
    def __init__(self, settings, queue):
        self.settings = settings
        self.queue = queue

        self.DIRECTORY_TO_WATCH = self.settings.watching_dir
        self.observer = Observer()

    def run(self):
        event_handler = Handler(self.queue, self.settings)
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        pool = []
        try:
            while True:

                while not self.queue.empty():
                    if threading.active_count() < (3 + self.settings.num_threads):
                        obj = self.queue.get()
                        worker = UploadThread(obj, auth)
                        pool.append(worker)
                        try:
                            worker.start()
                        except UploadIdNotInBucket:
                            self.queue.put(obj)

                time.sleep(self.settings.main_loop_sleep)

        except (KeyboardInterrupt, SystemExit):
            self.observer.stop()
            for thread in pool:
                thread.join()
            print "\nTerminate"
            exit(0)
        finally:
            self.observer.join()


class Handler(PatternMatchingEventHandler):
    patterns = ["*.*"]

    def __init__(self, queue, settings):
        self.queue = queue
        self.settings = settings
        super(Handler, self).__init__(ignore_patterns=["*/.DS_store"])

    def process(self, event):
        if event.is_directory is False:
            file_size = os.stat(event.src_path).st_size
            folder = os.path.dirname(event.src_path).replace(self.settings.watching_dir, "")
            # print "{0} - {1}".format(event.src_path, size(file_size))
            if file_size > 0:
                for target in self.settings.uploads_targets:
                    if folder in target.get('nas_folders'):
                        object_to_upload = QueueObject(event.src_path,
                                                       target.get('bucket_name'),
                                                       target.get('endpoint'))
                        self.queue.put(object_to_upload)

    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)


def main():
    global auth

    uploading_objects_queue = Queue.Queue()

    settings = AppSettings()  # init app settings
    auth = oss2.Auth(settings.access_key_id, settings.access_key_secret)  # authentication with oss account

    print "\n ==== Files to upload ====\n"
    ScanNotInBucket(auth=auth, settings=settings, queue=uploading_objects_queue)  # Can be add an if statement for avoid checking files before run program

    print "\n ==== Start Uploading ====\n"

    w = Watcher(settings, uploading_objects_queue)
    w.run()  # main tread


if __name__ == '__main__':
    main()
