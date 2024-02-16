from bleak import BleakScanner, BleakClient
import asyncio
import logging
logger = logging.getLogger("__main__")

from bleak.backends.device import BLEDevice

from src.movesense.movesense_data_collector import MovesenseDataCollector


WRITE_CHARACTERISTIC = "34800001-7185-4d5d-b431-630e7050e8f0"
NOTIFY_CHARACTERISTIC = "34800002-7185-4d5d-b431-630e7050e8f0"
NAME_CHARACTERISTIC = ""


class ConnectedDevice:
    def __init__(self, device: BLEDevice, client: BleakClient):
        self.device = device
        self.client = client


class MovesenseDeviceManager:
    def __init__(self, config=None):

        self.data_collector = MovesenseDataCollector()

        self.loop = asyncio.get_event_loop()  # Create an event loop
        self.connected_devices = []

        # If we have a predefined session config, we utilize it
        if config:
            self.config = config
            # Collect the connectable devices
            available_devices = self.get_available_devices()

            for d in config["devices"]:
                if d["address"] not in [device.address for device in available_devices]:
                    logger.warning(f"Could not connect to device: {d['address']}")
                else:
                    # Connect to device
                    target_device = next((device for device in available_devices if device.address == d["address"]), None)
                    connected_device = self.connect(target_device)
                    # Subscribe to the data channels
                    for path in d["paths"]:
                        # Add ids to the paths so they can be recognized at read time
                        id_bytes = bytearray([1, 99])
                        if "Acc" in path:
                            id_bytes[1] = id_bytes[1] - 0
                        elif "Gyro" in path:
                            id_bytes[1] = id_bytes[1] - 1
                        elif "Magn" in path:
                            id_bytes[1] = id_bytes[1] - 2
                        elif "IMU9" in path:
                            id_bytes[1] = id_bytes[1] - 3
                        elif "Temp" in path:
                            id_bytes[1] = id_bytes[1] - 4
                        elif "ECG" in path:
                            id_bytes[1] = id_bytes[1] - 5

                        byte_path = id_bytes + bytearray(path, "utf-8")

                        self.subscribe_to_sensor(connected_device, byte_path)
            logger.info("Session config loaded.")
        else:
            logger.info("No session config found.")


    def run_coroutine_sync(self, coroutine):
        return self.loop.run_until_complete(coroutine)

    def get_available_devices(self, show_all=False):
        logger.info("Searching for available devices...")
        devices = self.run_coroutine_sync(BleakScanner.discover(timeout=5.0))
        found_devices = []
        for device in devices:
            if show_all or "movesense" in device.name.lower():
                found_devices.append(device)
                logger.info(f"Found device: {len(found_devices)}. {device.name} - {device.address}")

        return found_devices

    def connect(self, device):
        async def connection_coroutine(device):
            client = BleakClient(device.address)
            await client.connect()
            logger.info(f"Connected to {device.name} ({device.address})")
            connected_device = ConnectedDevice(device, client)
            self.connected_devices.append(connected_device)
            return connected_device

        return self.run_coroutine_sync(connection_coroutine(device))

    def show_connected_devices(self):
        logger.info("Connected MoveSense devices:")
        for i, device in enumerate(self.connected_devices):
            logger.info(f"{i + 1}. {device.device.name}: {device.device.address}")

    def rename_device(self, device, new_name):
        async def rename_coroutine(device, new_name):
            await device.client.write_gatt_char(NAME_CHARACTERISTIC, new_name, response=True)

        self.run_coroutine_sync(rename_coroutine(device, new_name))

    def subscribe_to_sensor(self, device, path):
        async def subscribe_coroutine(device, path):
            await device.client.write_gatt_char(WRITE_CHARACTERISTIC, path, response=True)
        self.run_coroutine_sync(subscribe_coroutine(device, path))

    def start_data_collection_sync(self):
        logger.debug("Enabling notifications.")
        # Subscribe to notify for all connected devices
        for device in self.connected_devices:
            self.loop.run_until_complete(self.start_notify_coroutine(device))
        logger.debug("Data collection started.")

    async def start_notify_coroutine(self, device):
        await device.client.start_notify(NOTIFY_CHARACTERISTIC, lambda sender, data: asyncio.ensure_future(
            self.data_collector.handle_notification(device.device.address, data)))

    def end_data_collection(self):
        logger.debug("Disabling notifications.")

        for device in self.connected_devices:
            self.run_coroutine_sync(device.client.stop_notify(NOTIFY_CHARACTERISTIC))

        # Save the collected data
        self.data_collector.unify_notifications().to_csv("./data_output.csv", sep=",", index=False)

        logger.info("Data collection complete.")

    def disconnect_device(self, device_id):
        async def disconnect_coroutine(device_id):
            device = self.connected_devices[device_id]
            logger.info(f"Disconnecting device {device.device.name} ({device.device.address})")
            await device.client.disconnect()
            self.connected_devices.remove(device)

        # Run disconnection coroutine
        self.run_coroutine_sync(disconnect_coroutine(device_id))

    def disconnect_devices(self):
        logger.info("Disconnecting from all MoveSense devices...")

        # Run disconnection coroutines. Repeatedly disconnecting 1st entry since the devices are always popped out of
        # connected devices.
        disconnect_coroutines = [self.disconnect_device(0) for _ in range(len(self.connected_devices))]
        self.run_coroutine_sync(asyncio.gather(*disconnect_coroutines))
