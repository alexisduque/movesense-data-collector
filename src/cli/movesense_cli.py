import sys
import re
import asyncio

from src.movesense.movesense_sensor import MovesenseSensor
from src.movesense.movesense_device_manager import MovesenseDeviceManager

import logging

logger = logging.getLogger("__main__")


class MovesenseCLI:
    def __init__(self, config=None):
        self.config = config or {}

        self.device_manager = MovesenseDeviceManager(config)
        self.output_filename = None

    def display_menu(self):
        logger.info("==== Movesense CLI Menu ====")
        logger.info("1. Start Data Collection")
        logger.info("2. Connect to an Additional Device")
        logger.info("3. Configure Connected Devices")
        logger.info("4. Save Current Configuration")
        logger.info("5. Exit")

    def start_device_connection_activity(self):
        found_devices = self.device_manager.get_available_devices()

        if not found_devices:
            logger.warning("No MoveSense devices found. Make sure that the MoveSense devices are turned on, and not "
                           "connected to another device.")
            return
        else:
            logger.info("Select the MoveSense Device to connect to (list-id or mac-address)")
            choice = input()

            # Regex match for MAC address input (MAC address regex)
            if re.match("[0-9a-f]{2}([-:]?)[0-9a-f]{2}(\\1[0-9a-f]{2}){4}$", choice.lower()):
                # Find the device with the specified MAC address
                selected_device = next(
                    (device for device in found_devices if device.device.address.lower() == choice.lower()),
                    None)

                if selected_device:
                    try:
                        self.device_manager.connect(selected_device)
                        if "devices" in self.config:
                            self.config["devices"].append({"address": selected_device, "paths": []})
                        else:
                            self.config["devices"] = [{"address": selected_device, "paths": []}]
                        logger.info(f"Connected to device with MAC address: {selected_device.device.device.address}")
                    except Exception as e:
                        logger.error(
                            f"Failed to connect to device with MAC address '{selected_device.device.device.address}': {e}")
                else:
                    logger.warning(f"No device found with MAC address '{choice}'")

            else:
                try:
                    index = int(choice) - 1
                    self.device_manager.connect(found_devices[index])
                    if "devices" in self.config:
                        self.config["devices"].append({"address": found_devices[index].address, "paths": []})
                    else:
                        self.config["devices"] = [{"address": found_devices[index].address, "paths": []}]

                    logger.info(
                        f"Connected to device with list-id: {index + 1}, and MAC address: {found_devices[index].address}")
                except ValueError:
                    logger.error(f"Invalid input. Please enter a valid list-id as an integer or a MAC address.")
                except IndexError:
                    logger.error(f"Invalid list-id '{index}'. List-id out of range.")
                except Exception as e:
                    logger.error(f"Unknown device-id '{choice}'. Failed to connect: {e}")

    def start_device_configuration_activity(self):

        if not self.device_manager.connected_devices:
            logger.warning("No connected devices found")
            return

        try:
            while True:
                logger.info("Select the device to configure. 'return' to exit back to main menu.")
                self.device_manager.show_connected_devices()

                choice = input()

                if choice == "return":
                    raise KeyboardInterrupt
                selected_device = None
                # If choice based on MAC (MAC address regex)
                if re.match("[0-9a-f]{2}([-:]?)[0-9a-f]{2}(\\1[0-9a-f]{2}){4}$", choice.lower()):
                    selected_device = next((device for device in self.device_manager.connected_devices
                                            if device.device.address.lower() == choice.lower()),
                                           None)
                # If choice based on list id
                else:
                    try:
                        index = int(choice) - 1
                        selected_device = self.device_manager.connected_devices[index]
                    except ValueError:
                        logger.error(f"Invalid input. Please enter a valid list-id as an integer or a MAC address.")
                    except IndexError:
                        logger.error(f"Invalid list-id '{index + 1}'. List-id out of range.")

                # Show configuration menu
                self.start_single_device_configuration(selected_device)

        except KeyboardInterrupt:
            logger.info("Exiting the device menu")

    def start_single_device_configuration(self, device):
        try:
            while True:
                logger.info("What would you like to do?")
                logger.info("1. Rename the device")
                logger.info("2. Subscribe to Acceleration")
                logger.info("3. Subscribe to Gyroscope")
                logger.info("4. Subscribe to Magnetometer")
                logger.info("5. Subscribe to Temperature")
                logger.info("7. Subscribe to ECG")
                logger.info("8. Subscribe to IMU6")
                logger.info("9. Subscribe to IMU9")
                logger.info("10. Return")

                choice = input()

                if choice == "1":
                    logger.info("Rename the device")
                    choice = input("New device name:")
                    self.device_manager.rename_device(device, choice)
                elif choice in list(map(lambda x: str(x), range(2, 9 + 1))):
                    sensor_full = ["Acceleration", "Gyroscope", "Magnetometer", "Temperature", "ECG", "IMU6", "IMU9"][
                        int(choice) - 2]
                    msg = "Subscribe to " + sensor_full
                    logger.info(msg)

                    try:
                        logger.info("Choose the sampling rate (13, 26, 52, 104, 208, 416, 833, 1666")
                        fs = int(input())
                        # Subscription path determines response id (fixed for ease of use), "Meas", the sensor type,
                        # and sampling rate
                        sensor = MovesenseSensor(sensor_full, fs)

                        self.config["devices"][device.address]["paths"].append(
                            f"Meas/{sensor.sensor_type.value}/{sensor.sampling_rate.value}")
                        self.device_manager.subscribe_to_sensor(device, sensor)
                    except ValueError:
                        logger.error(f"Invalid input. Please enter a valid integer choice.")
                elif choice == "10":
                    raise KeyboardInterrupt

        except KeyboardInterrupt:
            logger.info("Exiting the device configuration")

    def start_collection_activity(self):
        logger.info("Starting data collection. Press ctrl+c to terminate")
        self.device_manager.start_data_collection_sync()

        try:
            asyncio.get_event_loop().run_until_complete(
                asyncio.Event().wait())  # Wait indefinitely until the event is set
        except KeyboardInterrupt:
            logger.info("Ending data collection, saving...")
        finally:
            self.device_manager.end_data_collection()

    def run(self):

        # Loop for running the interface
        while True:
            self.display_menu()
            choice = input()

            if choice == "1":
                self.start_collection_activity()
            elif choice == "2":
                self.start_device_connection_activity()
            elif choice == "3":
                self.start_device_configuration_activity()
            elif choice == "4":
                pass
            elif choice == "5" or choice.lower() == "exit":
                logger.info("Exiting MoveSense CLI. Goodbye.")
                self.device_manager.disconnect_devices()
                break
            else:
                logger.warning(f"Unrecognized selection {choice}")


if __name__ == "__main__":
    movesense_cli = MovesenseCLI()
    movesense_cli.run()
