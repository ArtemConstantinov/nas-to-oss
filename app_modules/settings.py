import json
import io

try:
    to_unicode = unicode
except NameError:
    to_unicode = str


class AppSettings:
    config_filename = 'configs.json'
    watching_dir = './'
    uploads_targets = []  # contains object { bucket_name: "string", endpoint: "url", nas_folders: [ list of related folders from NAS ] }
    num_threads = 2
    app_version = '0.0.2a'
    access_key_id = ''
    access_key_secret = ''

    def __init__(self):
        try:
            with open(self.config_filename) as configs_file:
                data = json.load(configs_file)
                print data.get("version")
                self.watching_dir = data.get("watching_dir")
                self.uploads_targets = data.get("uploads_targets")
                self.num_threads = int(data.get("num_threads"))
                self.main_loop_sleep = int(data.get("main_loop_sleep"))

                self.access_key_id = data.get("access_key_id")
                self.access_key_secret = data.get("access_key_secret")

        except IOError:

            file_example = {
                "version": "0.0.2a",
                "access_key_id": "<Your access key ID>",
                "access_key_secret": "<Your access key secret>",
                "watching_dir": "./nas/",
                "num_threads": 2,
                "main_loop_sleep": 10,
                "uploads_targets":
                    [
                        {
                            "bucket_name": "<Your bucket name 1>",
                            "endpoint": "<Your buckets end point 1>",
                            "nas_folders":
                                [
                                    "demo1",
                                    "demo2"
                                ]
                        },
                        {
                            "bucket_name": "<Your bucket name 2>",
                            "endpoint": "<Your buckets end point 2>",
                            "nas_folders":
                                [
                                    "demo3",
                                    "demo1"
                                ]
                        }
                    ]
            }

            with io.open(self.config_filename, 'w', encoding='utf8') as outfile:
                str_ = json.dumps(file_example, indent=4, sort_keys=True, separators=(',', ': '), ensure_ascii=False)
                outfile.write(to_unicode(str_))

            raise Exception('Configs file is not exist or is corrupted, please fill in ')
