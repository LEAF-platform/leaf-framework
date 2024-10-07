import os
import sys
import unittest
import time
from threading import Thread
import shutil
import csv
from datetime import datetime

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))
sys.path.insert(0, os.path.join("..","..",".."))

from core.modules.input_modules.file_watcher import FileWatcher
test_file_dir = "test_files"

class TestFileWatcher(unittest.TestCase):
    def setUp(self):
        if not os.path.isdir(test_file_dir):
            os.mkdir(test_file_dir)

    def tearDown(self):
        if os.path.isdir(test_file_dir):
            shutil.rmtree(test_file_dir)
        time.sleep(2)

    def test_file_watcher_change(self):
        def mod_file(filename, interval, count):
            for _ in range(count):
                now = datetime.now()
                timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                with open(filename, "a") as file:
                    file.write(f"{timestamp}\n")
                
                time.sleep(interval)

        message_count = 0
        def mock_callback(data):
            nonlocal message_count
            message_count += 1


        text_watch_file = os.path.join(test_file_dir, "tmp.txt")
        if not os.path.isfile(text_watch_file):
            with open(text_watch_file, "w"):
                pass

        num_mod = 3
        interval = 2
        watcher = FileWatcher(text_watch_file,
                              measurement_callbacks=mock_callback)
        watcher.start()
        mthread = Thread(target=mod_file, args=(text_watch_file, 
                                                interval, num_mod))
        mthread.start()
        mthread.join()
        time.sleep(2)
        watcher.stop()
        self.assertEqual(message_count, num_mod)
        
    def test_file_watcher_creation(self):
        def create_file(filepath, interval, count):
            for _ in range(count):                
                if os.path.isfile(filepath):
                    os.remove(filepath)
                headers = ["Timestamp"] + [f"TestHeading{str(h)}" 
                                        for h in range(0, 10)]
                with open(filepath, "w", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow(headers)
                time.sleep(interval)
                
        message_count = 0
        def mock_callback(data):
            nonlocal message_count
            message_count += 1

        creation_file = os.path.join(test_file_dir,"tst.csv")
        watcher = FileWatcher(creation_file,start_callbacks=mock_callback)
        num_create = 3
        interval = 2
        watcher.start()
        mthread = Thread(target=create_file,args=(creation_file,interval,
                                                  num_create))
        mthread.start()
        mthread.join()
        time.sleep(2)
        watcher.stop()
        self.assertEqual(message_count,num_create)

    def test_file_watcher_deletion(self):
        def delete_file(filepath, interval, count):
            for _ in range(count):
                if not os.path.isfile(filepath):
                    headers = ["Timestamp"] + [f"TestHeading{str(h)}" 
                                            for h in range(0, 10)]
                    with open(filepath, "w", newline="") as file:
                        writer = csv.writer(file)
                        writer.writerow(headers)
                time.sleep(interval)
                os.remove(filepath)
                time.sleep(interval)
                
        message_count = 0
        def mock_callback(data):
            nonlocal message_count
            message_count += 1


        deletion_file = os.path.join(test_file_dir,"tst.csv")
        watcher = FileWatcher(deletion_file,stop_callbacks=mock_callback)
        num_create = 3
        interval = 2
        watcher.start()
        mthread = Thread(target=delete_file,args=(deletion_file,interval,
                                                  num_create))
        mthread.start()
        mthread.join()
        time.sleep(2)
        watcher.stop()
        self.assertEqual(message_count,num_create)


if __name__ == "__main__":
    unittest.main()