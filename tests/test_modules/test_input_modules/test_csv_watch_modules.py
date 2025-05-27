import os
import sys
import unittest
import time
from threading import Thread
import csv
from datetime import datetime
import tempfile

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from leaf.modules.input_modules.csv_watcher import CSVWatcher
from leaf_register.metadata import MetadataManager


class TestCSVWatcher(unittest.TestCase):
    def test_csv_watcher_change(self):
        with tempfile.TemporaryDirectory() as test_dir:
            def mod_file(filename, interval, count):
                for _ in range(count):
                    now = datetime.now()
                    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                    with open(filename, "a") as file:
                        file.write(f"{timestamp}\n")
                    time.sleep(interval)

            topics = {}
            def mock_callback(topic,data):
                nonlocal topics
                topic = topic()
                if topic not in topics:
                    topics[topic] = []
                topics[topic].append(data)

            text_watch_file = os.path.join(test_dir, "test_csv_watcher_change.txt")
            if not os.path.isfile(text_watch_file):
                with open(text_watch_file, "w"):
                    pass

            num_mod = 3
            interval = 2
            metadata = MetadataManager()
            watcher = CSVWatcher(text_watch_file,metadata,
                                  callbacks=[mock_callback])
            watcher.start()
            mthread = Thread(target=mod_file, args=(text_watch_file, 
                                                    interval, num_mod))
            mthread.start()
            mthread.join()
            time.sleep(1)
            watcher.stop()
            self.assertEqual(len(topics[metadata.experiment.measurement()]), num_mod)

    def test_csv_watcher_creation(self):
        with tempfile.TemporaryDirectory() as test_dir:

            def create_file(filepath, interval, count):
                for _ in range(count):
                    if os.path.isfile(filepath):
                        os.remove(filepath)
                    headers = ["Timestamp"] + [
                        f"TestHeading{str(h)}" for h in range(0, 10)
                    ]
                    with open(filepath, "w", newline="") as file:
                        writer = csv.writer(file)
                        writer.writerow(headers)
                    time.sleep(interval)

            topics = {}
            def mock_callback(topic,data):
                nonlocal topics
                topic = topic()
                if topic not in topics:
                    topics[topic] = []
                topics[topic].append(data)

            creation_file = os.path.join(test_dir, 
                                         "test_csv_watcher_creation.csv")
            metadata = MetadataManager()
            watcher = CSVWatcher(creation_file,metadata,
                                  callbacks=[mock_callback])
            num_create = 3
            interval = 2
            watcher.start()
            mthread = Thread(target=create_file, args=(creation_file, 
                                                       interval, 
                                                       num_create))
            mthread.start()
            mthread.join()
            time.sleep(1)
            watcher.stop()
            self.assertEqual(len(topics[metadata.experiment.start()]), 
                             num_create)

    def test_csv_watcher_deletion(self) -> None:
        with tempfile.TemporaryDirectory() as test_dir:

            def delete_file(filepath, interval, count):
                for _ in range(count):
                    if not os.path.isfile(filepath):
                        headers = ["Timestamp"] + [
                            f"TestHeading{str(h)}" for h in range(0, 10)
                        ]
                        with open(filepath, "w", newline="") as file:
                            writer = csv.writer(file)
                            writer.writerow(headers)
                    time.sleep(interval)
                    os.remove(filepath)
                    time.sleep(interval)

            topics = {}
            def mock_callback(topic,data):
                nonlocal topics
                topic = topic()
                if topic not in topics:
                    topics[topic] = []
                topics[topic].append(data)

            deletion_file = os.path.join(test_dir, 
                                         "test_csv_watcher_deletion.csv")
            metadata = MetadataManager()
            watcher = CSVWatcher(deletion_file,metadata,
                                  callbacks=[mock_callback])
            num_create = 3
            interval = 1
            watcher.start()
            mthread = Thread(target=delete_file, 
                             args=(deletion_file, interval, num_create))
            mthread.start()
            mthread.join()
            time.sleep(1)
            watcher.stop()
            self.assertEqual(len(topics[metadata.experiment.stop()]), 
                             num_create)

    def test_watch_directory(self):
        with tempfile.TemporaryDirectory() as test_dir:
            def create_file(filepath, interval, count):
                for _ in range(count):
                    if os.path.isfile(filepath):
                        os.remove(filepath)
                    headers = ["Timestamp"] + [
                        f"TestHeading{str(h)}" for h in range(0, 10)
                    ]
                    with open(filepath, "w", newline="") as file:
                        writer = csv.writer(file)
                        writer.writerow(headers)
                    time.sleep(interval)

            def mod_file(filename, interval, count):
                for _ in range(count):
                    now = datetime.now()
                    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                    with open(filename, "a") as file:
                        file.write(f"{timestamp}\n")
                    time.sleep(interval)

            topics = {}
            def mock_callback(topic,data):
                nonlocal topics
                topic = topic()
                if topic not in topics:
                    topics[topic] = []
                topics[topic].append(data)

            metadata = MetadataManager()
            watcher = CSVWatcher(test_dir,metadata,
                                  callbacks=[mock_callback])
            num_create = 3
            interval = 2
            watcher.start()
            creation_file = os.path.join(test_dir,"tmp.csv")
            mthread = Thread(target=create_file, args=(creation_file, 
                                                       interval, 
                                                       num_create))
            mthread.start()
            mthread.join()
            time.sleep(1)
            watcher.stop()
            self.assertEqual(len(topics[metadata.experiment.start()]), 
                             num_create)

            num_mod = 3
            interval = 2
            metadata = MetadataManager()
            watcher = CSVWatcher(creation_file,metadata,
                                  callbacks=[mock_callback])
            watcher.start()
            mthread = Thread(target=mod_file, args=(creation_file, 
                                                    interval, num_mod))
            mthread.start()
            mthread.join()
            time.sleep(1)
            watcher.stop()
            self.assertEqual(len(topics[metadata.experiment.measurement()]), num_mod)

    def test_watch_directory_return_filepath(self):
        with tempfile.TemporaryDirectory() as test_dir:
            def create_file(filepath, interval):
                if os.path.isfile(filepath):
                    os.remove(filepath)
                headers = ["Timestamp"] + [
                    f"TestHeading{str(h)}" for h in range(0, 10)
                ]
                with open(filepath, "w", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow(headers)
                time.sleep(interval)

            def mod_file(filename, interval):
                now = datetime.now()
                timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                with open(filename, "a") as file:
                    file.write(f"{timestamp}\n")
                time.sleep(interval)


            topics = {}
            creation_file = os.path.join(test_dir,"tmp.csv")
            def mock_callback(topic,data):
                nonlocal topics
                topic = topic()
                self.assertEqual(data,creation_file)
                if topic not in topics:
                    topics[topic] = []
                topics[topic].append(data)

            metadata = MetadataManager()
            watcher = CSVWatcher(test_dir,metadata,
                                  callbacks=[mock_callback],
                                  return_data=False)
            interval = 2
            watcher.start()
            
            mthread = Thread(target=create_file, args=(creation_file, 
                                                       interval))
            mthread.start()
            mthread.join()
            time.sleep(1)
            self.assertEqual(len(topics[metadata.experiment.start()]),1)
            mthread = Thread(target=mod_file, args=(creation_file, 
                                                    interval))
            mthread.start()
            mthread.join()
            time.sleep(1)
            watcher.stop()
            self.assertEqual(len(topics[metadata.experiment.measurement()]),1)

if __name__ == "__main__":
    unittest.main()
