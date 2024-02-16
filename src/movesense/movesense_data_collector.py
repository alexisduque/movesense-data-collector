import os
import csv
import tempfile
import shutil
import struct
import time
import pandas as pd
from datetime import datetime

from functools import reduce

import logging
logger = logging.getLogger("__main__")

# https://stackoverflow.com/a/56243296
class DataView:
    def __init__(self, array, bytes_per_element=1):
        """
        bytes_per_element is the size of each element in bytes.
        By default, we are assume the array is one byte per element.
        """
        self.array = array
        self.bytes_per_element = 1

    def __get_binary(self, start_index, byte_count, signed=False):
        integers = [self.array[start_index + x] for x in range(byte_count)]
        bytes = [integer.to_bytes(
            self.bytes_per_element, byteorder='little', signed=signed) for integer in integers]
        return reduce(lambda a, b: a + b, bytes)

    def get_uint_16(self, start_index):
        bytes_to_read = 2
        return int.from_bytes(self.__get_binary(start_index, bytes_to_read), byteorder='little')

    def get_uint_8(self, start_index):
        bytes_to_read = 1
        return int.from_bytes(self.__get_binary(start_index, bytes_to_read), byteorder='little')

    def get_uint_32(self, start_index):
        bytes_to_read = 4
        binary = self.__get_binary(start_index, bytes_to_read)
        return struct.unpack('<I', binary)[0]  # <f for little endian

    def get_float_32(self, start_index):
        bytes_to_read = 4
        binary = self.__get_binary(start_index, bytes_to_read)
        return struct.unpack('<f', binary)[0]  # <f for little endian

class NotificationHandler:
    def __init__(self, device_id, sensor_type):
        self.device_id = device_id
        self.sensor_type = sensor_type
        self.notifications = []

    async def handle_notification(self, timestamp, sensor_data):
        local_timestamp = datetime.now().timestamp()
        notification = {
            "device_id": self.device_id,
            "sensor_type": self.sensor_type,
            "timestamp": timestamp,
            "local_timestamp": local_timestamp,
            "sensor_data": sensor_data,
        }
        #logger.debug(f"Received notification: {notification}")
        self.notifications.append(notification)


class MovesenseDataCollector:
    def __init__(self):
        self.notification_handlers = {}

    async def handle_notification(self, sender, data):
        device_id = sender
        d = DataView(data)

        # Some of the sensor types are handled differently. The second byte determines the sensor type, as defined
        # in device manager.
        type_bytes = data[:2]
        if type_bytes[-1] == 99:
            sensor_type, timestamp, sensor_data = (
                "Acc", d.get_uint_32(2), (d.get_float_32(6), d.get_float_32(10), d.get_float_32(14)))
        elif type_bytes[-1] == 98:
            sensor_type, timestamp, sensor_data = (
                "Gyro", d.get_uint_32(2), (d.get_float_32(6), d.get_float_32(10), d.get_float_32(14)))
        elif type_bytes[-1] == 97:
            sensor_type, timestamp, sensor_data = (
                "Magn", d.get_uint_32(2), (d.get_float_32(6), d.get_float_32(10), d.get_float_32(14)))
        elif type_bytes[-1] == 96:
            # This is untested, and assumed to only yield a single reading
            sensor_type, timestamp, sensor_data = "Temperature", d.get_uint_32(2), (d.get_float_32(6))
        elif type_bytes[-1] == 95:
            # This is untested, and assumed to only yield a single reading
            sensor_type, timestamp, sensor_data = "ECG", d.get_uint_32(2), (d.get_float_32(6))
        else:
            logger.error("Unknown sensor type")
            return

        #logger.debug(f"Got package from {sender}: {sensor_data}")
        handler = self.get_notification_handler(device_id, sensor_type)
        await handler.handle_notification(timestamp, sensor_data)

    def get_notification_handler(self, device_id, sensor_type):
        if (device_id, sensor_type) not in self.notification_handlers:
            self.notification_handlers[(device_id, sensor_type)] = NotificationHandler(device_id, sensor_type)
        return self.notification_handlers[(device_id, sensor_type)]

    def unify_notifications(self):
        all_notifications = []
        for handler in self.notification_handlers.values():
            all_notifications.extend(handler.notifications)

        # Create a Pandas DataFrame from the notifications
        df = pd.DataFrame(all_notifications)

        # We get the highest number of observations per sensor type available. This is used effectively as
        # "sampling rate" in the following transforms
        df.insert(0, "id", range(0, len(df)))
        highest_observation_count = df.groupby(["device_id", "sensor_type"])["id"].count().max()
        # We compute relative integer ids such that their density spans the highest count, but the observations
        # are set to be more sparse automatically. While this does not track the timestamps perfectly, it allows
        # combining the representation to be more dense. The sampling rates are nearly doubles of each other, which helps.
        df["relative_id"] = df.groupby(["device_id", "sensor_type"])["id"].transform(
            lambda x: ((x - x.min()) / ((x - x.min()).max()) * highest_observation_count).astype(int))

        # Pivot the DataFrame to get the desired structure
        df_pivot = df.pivot_table(
            index=["relative_id"],
            columns=["device_id", "sensor_type"],
            values=["local_timestamp", "timestamp", "sensor_data"],
            aggfunc=lambda x: x,
        ).reset_index()

        # Split the sensor_data columns into separate XYZ columns
        for col in df_pivot.columns:
            if "sensor_data" in col and ("Acc" in col or "Magn" in col or "Gyro" in col):
                df_pivot[["_".join(col) + "_X", "_".join(col) + "_Y", "_".join(col) + "_Z"]] = df_pivot[col].apply(lambda x: pd.Series(x))
                df_pivot.drop(col, axis=1, inplace=True)

        df_pivot.columns = [' '.join(col).strip() for col in df_pivot.columns.values]

        # Merge the timestamp columns together to give a common timestamp
        df_pivot.insert(0, "timestamp", df_pivot.filter(like="^timestamp").min(axis=1))
        df_pivot = df_pivot[df_pivot.columns.drop(list(df_pivot.filter(regex="^timestamp.+")))]

        # Merge the local_timestamp columns similarly
        df_pivot.insert(0, "local_timestamp", df_pivot.filter(like="local_timestamp").min(axis=1))
        df_pivot = df_pivot[df_pivot.columns.drop(list(df_pivot.filter(regex="local_timestamp.+")))]

        return df_pivot

