from datetime import datetime
from typing import Any

from influxobject import InfluxPoint

from leaf.adapters.equipment_adapter import AbstractInterpreter

class MBPOPCInterpreter(AbstractInterpreter):
    def __init__(self, metadata_manager: Any, error_holder=None):
        super().__init__(error_holder=error_holder)

    def metadata(self, data):
        return None
    
    def measurement(self, data: list[str]):
        print("MEASUREMENT DATA!!!!!!!!!!!!!! {}".format(data))
        influx_object = InfluxPoint()
        influx_object.set_measurement("testing")
        influx_object.set_timestamp(datetime.now())
        influx_object.set_tags({"tag1": "tag1"})
        influx_object.set_fields({"field1": 1})

        return influx_object

    # def simulate(self):
    #     return super().simulate()
