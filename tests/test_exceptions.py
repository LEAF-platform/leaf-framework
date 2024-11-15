import sys
import unittest
import json
from unittest.mock import patch, MagicMock, mock_open
import yaml
import errno
import os
from csv import Error as csv_error
import paho.mqtt.client as mqtt
from threading import Thread
import time
import csv

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.adapters.equipment_adapter import AbstractInterpreter
from leaf.modules.output_modules.mqtt import MQTT
from leaf.modules import KEYDB
from leaf.modules import FileWatcher
from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules import ControlPhase
from leaf.modules.process_modules.discrete_module import DiscreteProcess
from leaf.modules.process_modules.continous_module import ContinousProcess
from start import _process_instance
from start import _get_output_module
from start import run_adapters
from start import stop_all_adapters
import start
from leaf.metadata_manager.metadata import MetadataManager
from leaf.modules.input_modules.csv_watcher import CSVWatcher
from leaf.modules import FILE
from leaf.error_handler.exceptions import ClientUnreachableError
from leaf.error_handler.exceptions import SeverityLevel
from leaf.error_handler.exceptions import AdapterBuildError
from leaf.error_handler.exceptions import InputError
from leaf.error_handler.exceptions import InterpreterError
from leaf.error_handler.error_holder import ErrorHolder
from leaf.adapters import equipment_adapter
from leaf.adapters.functional_adapters.biolector1.biolector1_interpreter import (
    Biolector1Interpreter,
)

curr_dir = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(curr_dir, "test_config.yaml"), "r") as file:
    config = yaml.safe_load(file)

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])
try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except:
    un = None
    pw = None


watch_file = os.path.join("tmp.txt")
curr_dir = os.path.dirname(os.path.realpath(__file__))
test_file_dir = os.path.join(curr_dir, "static_files")
initial_file = os.path.join(test_file_dir, "biolector1_metadata.csv")
measurement_file = os.path.join(test_file_dir, "biolector1_measurement.csv")
all_data_file = os.path.join(test_file_dir, "biolector1_full.csv")
text_watch_file = os.path.join("tmp.txt")


class MockBioreactorInterpreter(AbstractInterpreter):
    def __init__(self) -> None:
        super().__init__()
        self.id = "test_bioreactor"

    def metadata(self, data):
        return data

    def measurement(self, data):
        return data

    def simulate(self):
        return


class MockEquipment(EquipmentAdapter):
    def __init__(self, instance_data, fp, error_holder=None):
        metadata_manager = MetadataManager()
        watcher = FileWatcher(fp, metadata_manager)
        output = MQTT(broker, port, username=un, password=pw, clientid=None)
        start_p = ControlPhase(
            output, metadata_manager.experiment.start, metadata_manager
        )
        stop_p = ControlPhase(
            output, metadata_manager.experiment.stop, metadata_manager
        )
        measure_p = MeasurePhase(output, metadata_manager)
        details_p = ControlPhase(output, metadata_manager.details, metadata_manager)

        watcher.add_start_callback(start_p.update)
        watcher.add_measurement_callback(measure_p.update)
        watcher.add_stop_callback(stop_p.update)
        watcher.add_initialise_callback(details_p.update)
        phase = [start_p, measure_p, stop_p]
        mock_process = [DiscreteProcess(phase)]

        super().__init__(
            instance_data,
            watcher,
            mock_process,
            MockBioreactorInterpreter(),
            metadata_manager,
            error_holder=error_holder,
        )


class TestExceptionsInit(unittest.TestCase):
    def setUp(self):
        pass

    def test_start_get_output_module_not_found(self):
        config = {
            "OUTPUTS": [
                {
                    "plugin": "MQTTT",
                    "broker": "test.mosquitto.org",
                    "port": 1883,
                    "fallback": "KEYDB",
                },
                {
                    "plugin": "KEYDB",
                    "host": "localhost",
                    "port": 6379,
                    "db": 0,
                    "fallback": "FILE",
                },
                {"plugin": "FILE", "filename": "local.json", "fallback": None},
            ],
        }
        with self.assertRaises(AdapterBuildError):
            _get_output_module(config, None)

    def test_start_get_output_fallback_not_found(self):
        config = {
            "OUTPUTS": [
                {
                    "plugin": "MQTT",
                    "broker": "test.mosquitto.org",
                    "port": 1883,
                    "fallback": "KEYDBB",
                },
                {
                    "plugin": "KEYDB",
                    "host": "localhost",
                    "port": 6379,
                    "db": 0,
                    "fallback": "FILE",
                },
                {"plugin": "FILE", "filename": "local.json", "fallback": None},
            ],
        }
        with self.assertRaises(AdapterBuildError):
            _get_output_module(config, None)

    def test_start_get_output_invalid_params(self):
        config = {
            "OUTPUTS": [
                {
                    "plugin": "MQTT",
                    "port": 1883,
                    "fallback": "KEYDB",
                },
                {
                    "plugin": "KEYDB",
                    "host": "localhost",
                    "port": 6379,
                    "db": 0,
                    "fallback": "FILE",
                },
                {"plugin": "FILE", "filename": "local.json", "fallback": None},
            ],
        }
        with self.assertRaises(AdapterBuildError):
            _get_output_module(config, None)

    def test_start_process_instance_not_found(self):
        config = {
            "adapter": "BioLector123",
            "data": {"instance_id": "biolector_devonshire10", "institute": "NCL"},
            "requirements": {"write_file": "tmp1.csv"},
        }
        output = MQTT(broker, port, username=un, password=pw, clientid=None)
        with self.assertRaises(AdapterBuildError):
            _process_instance(config, output)

    def test_start_process_instance_no_requirements(self):
        config = {
            "adapter": "BioLector1",
            "data": {"instance_id": "biolector_devonshire10", "institute": "NCL"},
            "requirements": {},
        }
        output = MQTT(broker, port, username=un, password=pw, clientid=None)
        with self.assertRaises(AdapterBuildError):
            _process_instance(config, output)

    def test_start_process_instance_missing_id(self):
        config = {
            "adapter": "BioLector1",
            "data": {"institute": "NCL"},
            "requirements": {"write_file": "tmp1.csv"},
        }
        output = MQTT(broker, port, username=un, password=pw, clientid=None)
        with self.assertRaises(AdapterBuildError):
            _process_instance(config, output)

    def test_file_watcher_invalid_filepath(self):
        filepath = None
        metadata_manager = MetadataManager()
        with self.assertRaises(AdapterBuildError):
            FileWatcher(filepath, metadata_manager)

    def test_output_module_connect(self):
        f_broker = "Unknown_broker_addr"
        with self.assertRaises(ClientUnreachableError):
            MQTT(f_broker, clientid=None)

    def test_raise_continous_discrete_process(self):
        metadata_manager = MetadataManager()
        output = MQTT(broker, port, username=un, password=pw, clientid=None)
        start_p = ControlPhase(
            output, metadata_manager.experiment.start, metadata_manager
        )
        stop_p = ControlPhase(
            output, metadata_manager.experiment.stop, metadata_manager
        )
        measure_p = MeasurePhase(output, metadata_manager)

        phase = [start_p, measure_p, stop_p]

        def _init_cont_proc():
            ContinousProcess(phase)

        def _init_disc_proc():
            DiscreteProcess([phase[0]])

        self.assertRaises(AdapterBuildError, _init_cont_proc)
        self.assertRaises(AdapterBuildError, _init_disc_proc)


class TestExceptionsGeneral(unittest.TestCase):
    def setUp(self):
        self.error_holder = MagicMock()
        self.broker = "test_broker"
        self.port = 1883
        self.host = "localhost"
        self.mqtt_client = MQTT(
            broker=self.broker, port=self.port, error_holder=self.error_holder
        )
        self.keydb_client = KEYDB(
            host=self.host, port=self.port, error_holder=self.error_holder
        )
        self.file_client = FILE(filename="test.json", error_holder=self.error_holder)

    @patch("core.modules.output_modules.mqtt.mqtt.Client.connect")
    def test_mqtt_module_cant_connect_init(self, mock_connect):
        mock_connect.side_effect = ClientUnreachableError("Broker unreachable")
        with self.assertRaises(ClientUnreachableError):
            self.mqtt_client.__init__(
                broker=self.broker, port=self.port, error_holder=self.error_holder
            )
        self.error_holder.add_error.assert_called_once()

    @patch("core.modules.output_modules.mqtt.MQTT._handle_exception")
    @patch("core.modules.output_modules.mqtt.mqtt.Client.publish")
    def test_mqtt_module_cant_transmit(self, mock_publish, mock_handle_exception):
        mock_publish.return_value.rc = mqtt.MQTT_ERR_NO_CONN
        self.mqtt_client.transmit("test/topic", "message")

        self.assertEqual(mock_handle_exception.call_count, 1)
        first_call = mock_handle_exception.call_args_list[0]
        self.assertIn("no output mechanisms available", str(first_call))

    @patch("core.modules.output_modules.mqtt.mqtt.Client.publish")
    def test_mqtt_module_cant_flush(self, mock_publish):
        mock_publish.side_effect = ClientUnreachableError("No connection")

        with self.assertRaises(ClientUnreachableError):
            self.mqtt_client.flush("test/topic")
        self.error_holder.add_error.assert_called_once()

    @patch("core.modules.output_modules.mqtt.MQTT._handle_exception")
    def test_mqtt_module_on_connect_invalid_input(self, mock_handle_exception):
        self.mqtt_client.on_connect("test_client", None, None, rc=1)
        mock_handle_exception.assert_called_once()
        self.assertIn(
            "Connection refused: Unacceptable protocol version",
            str(mock_handle_exception.call_args[0][0]),
        )

    @patch("core.modules.output_modules.mqtt.mqtt.Client.connect")
    def test_mqtt_module_on_connect_cant_reach_broker(self, mock_connect):
        mock_connect.side_effect = ClientUnreachableError("Server unavailable")
        with self.assertRaises(ClientUnreachableError):
            self.mqtt_client.__init__(
                broker=self.broker, port=self.port, error_holder=self.error_holder
            )

    @patch("core.modules.output_modules.keydb_client.redis.StrictRedis.set")
    def test_keydb_transmit_cant_access_client(self, mock_set):
        mock_set.side_effect = ClientUnreachableError("Unable to connect to KeyDB")
        self.keydb_client.transmit("test_key", "test_data")
        self.assertEqual(self.error_holder.add_error.call_count, 2)

    @patch("core.modules.output_modules.keydb_client.KEYDB._handle_redis_error")
    @patch("core.modules.output_modules.keydb_client.redis.StrictRedis")
    def test_keydb_connect_cant_access_client(self, mock_redis, mock_handle_error):
        mock_redis.side_effect = ClientUnreachableError("Connection to KeyDB failed")
        with self.assertRaises(ClientUnreachableError):
            self.keydb_client.connect()


    @patch("core.modules.output_modules.mqtt.mqtt.Client.on_disconnect")
    def test_mqtt_module_on_disconnect_reconnect_failure(self, mock_on_disconnect):
        mock_on_disconnect.side_effect = ClientUnreachableError("Reconnect failed")
        self.mqtt_client.on_disconnect("test_client", None, rc=2)
        self.assertEqual(self.error_holder.add_error.call_count, 2)

    @patch("core.modules.output_modules.file.open", new_callable=mock_open)
    @patch("core.modules.output_modules.file.os.path.exists", return_value=True)
    def test_file_transmit_cant_access_file(self, mock_exists, mock_open_file):
        self.error_holder.reset_mock()  # Reset previous error calls
        mock_open_file.side_effect = OSError("Unable to open file")

        # Attempt to transmit data and expect fallback mechanism
        self.file_client.transmit("test_topic", "test_data")

        # Verify the error was handled
        self.error_holder.add_error.assert_called_once()

    @patch("core.modules.output_modules.file.open", new_callable=mock_open)
    def test_file_transmit_invalid_json(self, mock_open_file):
        self.error_holder.reset_mock()  # Reset previous error calls
        # Simulate a JSON decoding error
        mock_open_file.side_effect = json.JSONDecodeError("Invalid JSON", doc="", pos=0)

        # Attempt to transmit data and expect fallback mechanism
        self.file_client.transmit("test_topic", "test_data")

        # Verify the error was handled
        self.error_holder.add_error.assert_called_once()

    @patch("core.modules.output_modules.file.open", new_callable=mock_open)
    @patch("core.modules.output_modules.file.os.path.exists", return_value=True)
    def test_file_retrieve_cant_access_file(self, mock_exists, mock_open_file):
        self.error_holder.reset_mock()  # Reset previous error calls
        # Simulate an OSError during file retrieval
        mock_open_file.side_effect = OSError("Unable to access file")

        # Attempt to retrieve data
        result = self.file_client.retrieve("test_topic")

        # Assert that None is returned due to error and error is recorded
        self.assertIsNone(result)
        self.error_holder.add_error.assert_called_once()

    @patch("json.load", side_effect=json.JSONDecodeError("Invalid JSON", doc="", pos=0))
    @patch("core.modules.output_modules.file.open", new_callable=mock_open)
    def test_file_retrieve_invalid_json(self, mock_open_file, mock_json_load):
        # Attempt to retrieve data
        result = self.file_client.retrieve("test_topic")

        # Assert that None is returned due to error and error is recorded
        self.assertIsNone(result)
        self.error_holder.add_error.assert_called_once()

    def test_start_handler_no_fallback(self):
        error_holder = ErrorHolder(threshold=5)
        output = MQTT(
            broker,
            port,
            username=un,
            password=pw,
            clientid=None,
            error_holder=error_holder,
        )

        write_dir = "test"
        if not os.path.isdir(write_dir):
            os.mkdir(write_dir)
        write_file = os.path.join(write_dir, "tmp1.csv")

        ins = [
            {
                "equipment": {
                    "adapter": "BioLector1",
                    "data": {
                        "instance_id": "biolector_devonshire10",
                        "institute": "NCL",
                    },
                    "requirements": {"write_file": write_file},
                }
            }
        ]

        def _start():
            mthread = Thread(target=run_adapters, args=[ins, output, error_holder])
            mthread.start()
            return mthread

        def _stop(thread):
            stop_all_adapters()
            thread.join()

        with self.assertLogs(start.__name__, level="WARNING") as logs:
            adapter_thread = _start()
            time.sleep(2)
            output.disconnect()
            while not output.client.is_connected():
                time.sleep(0.1)
            self.assertTrue(output.client.is_connected())
            _stop(adapter_thread)

        expected_exceptions = [
            ClientUnreachableError(
                "Cant store data, no output mechanisms available",
                SeverityLevel.WARNING,
            ),
        ]
        self.assertTrue(len(logs.records) > 0)
        for log in logs.records:
            exc_type, exc_value, exc_traceback = log.exc_info
            for exp_exc in list(expected_exceptions):
                if (
                    type(exp_exc) == exc_type
                    and exp_exc.severity == exc_value.severity
                    and exp_exc.args == exc_value.args
                ):
                    expected_exceptions.remove(exp_exc)
        self.assertEqual(len(expected_exceptions), 0)

    def test_start_handler_no_connection(self):
        error_holder = ErrorHolder(threshold=5)
        write_dir = "test"
        if not os.path.isdir(write_dir):
            os.mkdir(write_dir)
        write_file = os.path.join(write_dir, "tmp1.csv")
        file_fn = os.path.join(write_dir, "file_fn.txt")
        file = FILE(file_fn)
        fake_broker = "fake_mqtt_broker_"
        output = MQTT(
            fake_broker,
            port,
            username=un,
            password=pw,
            clientid=None,
            error_holder=error_holder,
            fallback=file,
        )

        ins = [
            {
                "equipment": {
                    "adapter": "BioLector1",
                    "data": {
                        "instance_id": "biolector_devonshire10",
                        "institute": "NCL",
                    },
                    "requirements": {"write_file": write_file},
                }
            }
        ]

        def _start():
            mthread = Thread(target=run_adapters, args=[ins, output, error_holder])
            mthread.start()
            return mthread

        def _stop(thread):
            stop_all_adapters()
            thread.join()

        with self.assertLogs(start.__name__, level="WARNING") as logs:
            adapter_thread = _start()
            while output._enabled:
                time.sleep(0.1)
            _stop(adapter_thread)

        expected_exceptions = [
            ClientUnreachableError(
                "Error connecting to broker: [Errno -3] Temporary failure in name resolution",
                SeverityLevel.WARNING,
            ),
        ]
        expected_logs = ["Disabling client MQTT."]
        self.assertFalse(output._enabled)
        self.assertTrue(len(logs.records) > 0)
        for log in logs.records:
            exc_type, exc_value, exc_traceback = log.exc_info
            for exp_log in list(expected_logs):
                if exp_log == log.getMessage():
                    expected_logs.remove(exp_log)
            for exp_exc in list(expected_exceptions):
                if (
                    type(exp_exc) == exc_type
                    and exp_exc.severity == exc_value.severity
                    and exp_exc.args == exc_value.args
                ):
                    expected_exceptions.remove(exp_exc)

        self.assertEqual(len(expected_exceptions), 0)
        self.assertEqual(len(expected_logs), 0)

    def test_start_handler_multiple_adapter_critical(self):
        error_holder = ErrorHolder(threshold=5)
        write_dir = "test"
        if not os.path.isdir(write_dir):
            os.mkdir(write_dir)
        write_file1 = os.path.join(write_dir, "tmp1.csv")
        write_file2 = os.path.join(write_dir, "tmp2.csv")
        file_fn = os.path.join(write_dir, "file_fn.txt")
        file = FILE(file_fn)
        output = MQTT(
            broker,
            port,
            username=un,
            password=pw,
            clientid=None,
            error_holder=error_holder,
            fallback=file,
        )

        ins = [
            {
                "equipment": {
                    "adapter": "BioLector1",
                    "data": {
                        "instance_id": "test_start_handler_multiple_adapter_reset1",
                        "institute": "test_start_handler_multiple_adapter_reset_ins1",
                    },
                    "requirements": {"write_file": write_file1},
                }
            },
            {
                "equipment": {
                    "adapter": "BioLector1",
                    "data": {
                        "instance_id": "test_start_handler_multiple_adapter_reset2",
                        "institute": "test_start_handler_multiple_adapter_reset_ins2",
                    },
                    "requirements": {"write_file": write_file2},
                }
            },
        ]

        def _start():
            mthread = Thread(target=run_adapters, args=[ins, output, error_holder])
            mthread.start()
            return mthread

        def _stop(thread):
            stop_all_adapters()
            thread.join()

        with self.assertLogs(start.__name__, level="ERROR") as logs:
            adapter_thread = _start()
            time.sleep(5)
            exception = ClientUnreachableError(
                "test_multiple_adapter_reset_test_exception",
                severity=SeverityLevel.CRITICAL,
            )
            error_holder.add_error(exception)
            while not _is_error_seen(exception, error_holder):
                time.sleep(0.1)
            _stop(adapter_thread)

        expected_exceptions = [exception]
        self.assertTrue(len(logs.records) > 0)
        for log in logs.records:
            exc_type, exc_value, exc_traceback = log.exc_info
            for exp_exc in list(expected_exceptions):
                if (
                    type(exp_exc) == exc_type
                    and exp_exc.severity == exc_value.severity
                    and exp_exc.args == exc_value.args
                ):
                    expected_exceptions.remove(exp_exc)

        self.assertEqual(len(expected_exceptions), 0)

    def test_start_handler_multiple_adapter_reset(self):
        error_holder = ErrorHolder(threshold=5)
        write_dir = "test"
        if not os.path.isdir(write_dir):
            os.mkdir(write_dir)
        write_file1 = os.path.join(write_dir, "tmp1.csv")
        write_file2 = os.path.join(write_dir, "tmp2.csv")
        file_fn = os.path.join(write_dir, "file_fn.txt")
        file = FILE(file_fn)
        output = MQTT(
            broker,
            port,
            username=un,
            password=pw,
            clientid=None,
            error_holder=error_holder,
            fallback=file,
        )

        ins = [
            {
                "equipment": {
                    "adapter": "BioLector1",
                    "data": {
                        "instance_id": "test_start_handler_multiple_adapter_reset1",
                        "institute": "test_start_handler_multiple_adapter_reset_ins1",
                    },
                    "requirements": {"write_file": write_file1},
                }
            },
            {
                "equipment": {
                    "adapter": "BioLector1",
                    "data": {
                        "instance_id": "test_start_handler_multiple_adapter_reset2",
                        "institute": "test_start_handler_multiple_adapter_reset_ins2",
                    },
                    "requirements": {"write_file": write_file2},
                }
            },
        ]

        def _start():
            mthread = Thread(target=run_adapters, args=[ins, output, error_holder])
            mthread.start()
            return mthread

        def _stop(thread):
            stop_all_adapters()
            thread.join()

        with self.assertLogs(start.__name__, level="ERROR") as logs:
            adapter_thread = _start()
            time.sleep(5)
            exception = ClientUnreachableError(
                "test_multiple_adapter_reset_test_exception",
                severity=SeverityLevel.ERROR,
            )
            error_holder.add_error(exception)
            while not output.client.is_connected() or not _is_error_seen(
                exception, error_holder
            ):
                time.sleep(0.1)
            self.assertTrue(output.client.is_connected())
            _stop(adapter_thread)

        expected_exceptions = [exception]
        self.assertTrue(len(logs.records) > 0)
        for log in logs.records:
            exc_type, exc_value, exc_traceback = log.exc_info
            for exp_exc in list(expected_exceptions):
                if (
                    type(exp_exc) == exc_type
                    and exp_exc.severity == exc_value.severity
                    and exp_exc.args == exc_value.args
                ):
                    expected_exceptions.remove(exp_exc)

        self.assertEqual(len(expected_exceptions), 0)

class TestExceptionsAdapterSpecific(unittest.TestCase):
    def setUp(self):
        self.file_path = "/some/fake/path/file.txt"
        self.metadata_manager = MagicMock()
        self.file_watcher = FileWatcher(
            file_path=self.file_path,
            metadata_manager=self.metadata_manager,
            start_callbacks=[MagicMock()],
            measurement_callbacks=[MagicMock()],
            stop_callbacks=[MagicMock()],
        )

    @patch("core.modules.input_modules.file_watcher.Observer.start")
    def test_file_watcher_start_os_error(self, mock_observer_start):
        # Simulate an OSError with specific errno (e.g., ENOSPC)
        mock_observer_start.side_effect = OSError(
            errno.ENOSPC, "No space left on device"
        )

        with self.assertRaises(InputError) as context:
            self.file_watcher.start()

        self.assertIn("Inotify watch limit reached", str(context.exception))

    @patch("core.modules.input_modules.file_watcher.Observer.start")
    def test_file_watcher_start_unexpected_os_error(self, mock_observer_start):
        # Simulate an OSError with an unexpected errno code
        mock_observer_start.side_effect = OSError("Unexpected OS error")
        with self.assertRaises(InputError) as context:
            self.file_watcher.start()
        self.assertIn("Unexpected OS error", str(context.exception))

    @patch("builtins.open", new_callable=mock_open)
    def test_file_watcher_on_created_os_error(self, mock_open_file):
        # Simulate an OSError during file opening on creation
        mock_open_file.side_effect = OSError("Failed to open file on creation")
        mock_event = MagicMock()
        mock_event.src_path = self.file_path
        with self.assertRaises(InputError) as context:
            self.file_watcher.on_created(mock_event)
        self.assertIn("I/O error during creation event", str(context.exception))

    @patch("builtins.open", new_callable=mock_open)
    def test_csv_watcher_on_created_parse_error(self, mock_open_file):
        # Setup CSVWatcher and simulate csv.Error during file reading
        csv_watcher = CSVWatcher(
            file_path=self.file_path, metadata_manager=self.metadata_manager
        )
        mock_open_file.side_effect = csv_error("CSV parsing failed")
        mock_event = MagicMock()
        mock_event.src_path = self.file_path
        with self.assertRaises(InputError) as context:
            csv_watcher.on_created(mock_event)
        self.assertIn("CSV parsing error", str(context.exception))

    @patch("builtins.open", new_callable=mock_open)
    def test_file_watcher_on_modified_not_found(self, mock_open_file):
        # Simulate a FileNotFoundError during file opening on modification
        mock_open_file.side_effect = FileNotFoundError("File not found on modification")
        mock_event = MagicMock()
        mock_event.src_path = self.file_path
        with self.assertRaises(InputError) as context:
            self.file_watcher.on_modified(mock_event)
        self.assertIn(
            "File not found during modification event", str(context.exception)
        )

    @patch("builtins.open", new_callable=mock_open)
    def test_file_watcher_on_modified_os_error(self, mock_open_file):
        # Simulate an OSError during file modification
        mock_open_file.side_effect = OSError("OS error during modification")
        mock_event = MagicMock()
        mock_event.src_path = self.file_path
        with self.assertRaises(InputError) as context:
            self.file_watcher.on_modified(mock_event)
        self.assertIn("I/O error during modification event", str(context.exception))

    @patch.object(FileWatcher, "_handle_exception")
    def test_file_not_found_error(self, mock_handle_exception):
        error = FileNotFoundError("File not found")
        self.file_watcher._file_event_exception(error, "creation")

        mock_handle_exception.assert_called_once()
        args = mock_handle_exception.call_args[0][0]
        self.assertIsInstance(args, InputError)
        self.assertIn("File not found during creation event", str(args))

    @patch.object(FileWatcher, "_handle_exception")
    def test_permission_error(self, mock_handle_exception):
        error = PermissionError("Permission denied")
        self.file_watcher._file_event_exception(error, "modification")

        mock_handle_exception.assert_called_once()
        args = mock_handle_exception.call_args[0][0]
        self.assertIsInstance(args, InputError)
        self.assertIn(
            "Permission denied when accessing file during modification event", str(args)
        )

    @patch.object(FileWatcher, "_handle_exception")
    def test_io_error(self, mock_handle_exception):
        error = IOError("I/O error occurred")
        self.file_watcher._file_event_exception(error, "deletion")

        mock_handle_exception.assert_called_once()
        args = mock_handle_exception.call_args[0][0]
        self.assertIsInstance(args, InputError)
        self.assertIn("I/O error during deletion event", str(args))

    @patch.object(FileWatcher, "_handle_exception")
    def test_unicode_decode_error(self, mock_handle_exception):
        error = UnicodeDecodeError("utf-8", b"", 0, 1, "invalid start byte")
        self.file_watcher._file_event_exception(error, "creation")

        mock_handle_exception.assert_called_once()
        args = mock_handle_exception.call_args[0][0]
        self.assertIsInstance(args, InputError)
        self.assertIn("Encoding error while reading file", str(args))

    @patch.object(FileWatcher, "_handle_exception")
    def test_generic_error(self, mock_handle_exception):
        error = Exception("Generic error")
        self.file_watcher._file_event_exception(error, "modification")

        mock_handle_exception.assert_called_once()
        args = mock_handle_exception.call_args[0][0]
        self.assertIsInstance(args, InputError)
        self.assertIn("Error during modification event", str(args))

    def test_event_watcher_hardware_stalled(self):
        """
        When the FileWatcher cant monitor a file.
        """
        pass

    def test_measurement_adapter_outlier(self):
        """
        When the FileWatcher cant monitor a file.
        """
        pass

    def test_biolector_no_start(self):
        fp = os.path.join(test_file_dir, "biolector1_measurement.csv")
        with open(fp, "r", encoding="latin-1") as file:
            reader = list(csv.reader(file, delimiter=";"))
        error_holder = ErrorHolder(threshold=5)
        interpreter = Biolector1Interpreter(error_holder=error_holder)
        interpreter.measurement(reader)
        exp_excep = [
            InterpreterError(
                "No filters defined, likely because the adapter hasn't identified experiment start",
                severity=SeverityLevel.WARNING
            )
        ]
        actual_errors = list(error_holder.get_unseen_errors())
        self.assertTrue(len(actual_errors) > 0)
        for error, tb in actual_errors:
            for exp_exc in list(exp_excep):
                if (
                    type(exp_exc) == type(error)
                    and exp_exc.severity == error.severity
                    and exp_exc.args == error.args
                ):
                    exp_excep.remove(exp_exc)
        self.assertEqual(len(exp_excep), 0)

    def test_biolector_interpreter_measurements(self):
        measure_fp = os.path.join(test_file_dir, "biolector1_measurement.csv")
        metadata_fp = os.path.join(test_file_dir, "biolector1_metadata.csv")
        with open(metadata_fp, "r", encoding="latin-1") as file:
            md = list(csv.reader(file, delimiter=";"))
        with open(measure_fp, "r", encoding="latin-1") as file:
            measurements = list(csv.reader(file, delimiter=";"))
        error_holder = ErrorHolder(threshold=5)
        interpreter = Biolector1Interpreter(error_holder=error_holder)

        interpreter.metadata(md)
        to_delete_key = list(interpreter._filtermap.keys())[0]
        del interpreter._filtermap[to_delete_key]
        interpreter.measurement(measurements)
        expected_num_errors = 2
        actual_errors = list(error_holder.get_unseen_errors())
        self.assertEqual(expected_num_errors, len(actual_errors))
        exp_excep = InterpreterError("1 not a valid filter code")
        for error, tb in actual_errors:
            self.assertEqual(type(exp_excep), type(error))
            # Upgrades error severity.
            self.assertGreaterEqual(
                int(error.severity.value), int(exp_excep.severity.value)
            )
            self.assertEqual(exp_excep.args, error.args)

    def test_equipment_adapter_start_input_file_not_found(self):
        """Tests the starting file watcher when
        dir file is in doesnt exist"""

        def _start_adapter(adapter):
            mthread = Thread(target=adapter.start)
            mthread.daemon = True
            mthread.start()
            return mthread

        def _stop_adapter(adapter, thread):
            adapter.stop()
            thread.join()

        instance_data = {
            "instance_id": "test_equipment_adapter_start_instance_id",
            "institute": "test_equipment_adapter_start_institute_id",
            "equipment_id": "TestEquipmentAdapter",
        }
        t_dir = "test_equipment_adapter_start"
        if os.path.isdir(t_dir):
            os.removedirs(t_dir)
        filepath = os.path.join(t_dir, "test_equipment_adapter_start.txt")
        error_holder = ErrorHolder(timeframe=6)
        adapter = MockEquipment(instance_data, filepath, error_holder=error_holder)

        with self.assertLogs(equipment_adapter.__name__, level="ERROR") as logs:
            adapter_thread = _start_adapter(adapter)
            time.sleep(5)
            _stop_adapter(adapter, adapter_thread)

        expected_exceptions = [
            InputError(
                "Watch file does not exist: test_equipment_adapter_start",
                SeverityLevel.ERROR,
            ),
            InputError(
                "Watch file does not exist: test_equipment_adapter_start",
                SeverityLevel.ERROR,
            ),
            InputError(
                "Watch file does not exist: test_equipment_adapter_start",
                SeverityLevel.CRITICAL,
            ),
        ]
        self.assertTrue(len(logs.records) > 0)
        for log in logs.records:
            exc_type, exc_value, exc_traceback = log.exc_info
            for exp_exc in list(expected_exceptions):
                if (
                    type(exp_exc) == exc_type
                    and exp_exc.severity == exc_value.severity
                    and exp_exc.args == exc_value.args
                ):
                    expected_exceptions.remove(exp_exc)

        self.assertEqual(len(expected_exceptions), 0)

    def test_equipment_adapter_created_file_not_found(self):
        """Tests the handling of all the custom exceptions using
        the equipment adapter start and error holder system."""

        def _start_adapter(adapter):
            mthread = Thread(target=adapter.start)
            mthread.daemon = True
            mthread.start()
            return mthread

        def _stop_adapter(adapter, thread):
            adapter.stop()
            thread.join()

        instance_data = {
            "instance_id": "test_equipment_adapter_start_instance_id",
            "institute": "test_equipment_adapter_start_institute_id",
            "equipment_id": "TestEquipmentAdapter",
        }
        t_dir = "test_equipment_adapter_start"
        filepath = os.path.join(t_dir, "test_equipment_adapter_start.txt")
        if not os.path.isdir(t_dir):
            os.mkdir(t_dir)
        if os.path.isfile(filepath):
            os.remove(filepath)
        error_holder = ErrorHolder(timeframe=6)
        adapter = MockEquipment(instance_data, filepath, error_holder=error_holder)

        with self.assertLogs(equipment_adapter.__name__, level="ERROR") as logs:
            # Unreliable test, be good to think of an alternative way to spoof a file creation.
            adapter_thread = _start_adapter(adapter)
            for i in range(0, 3):
                with open(filepath, "w") as file:
                    pass
                os.remove(filepath)
                time.sleep(1)
            time.sleep(10)
            _stop_adapter(adapter, adapter_thread)

        expected_exceptions = [
            InputError(
                "File not found during creation event: test_equipment_adapter_start.txt",
                SeverityLevel.ERROR,
            ),
            InputError(
                "File not found during creation event: test_equipment_adapter_start.txt",
                SeverityLevel.ERROR,
            ),
            InputError(
                "File not found during creation event: test_equipment_adapter_start.txt",
                SeverityLevel.CRITICAL,
            ),
        ]
        self.assertTrue(len(logs.records) > 0)
        for log in logs.records:
            exc_type, exc_value, exc_traceback = log.exc_info
            print(exc_type,exc_value)
            for exp_exc in list(expected_exceptions):
                if (
                    type(exp_exc) == exc_type
                    and exp_exc.severity == exc_value.severity
                    and exp_exc.args == exc_value.args
                ):
                    expected_exceptions.remove(exp_exc)

        self.assertEqual(len(expected_exceptions), 0)


def _is_error_seen(exception, error_holder):
    for error in error_holder._errors:
        if exception == error["error"]:
            return error["is_seen"]
    return False
