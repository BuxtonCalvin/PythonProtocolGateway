import glob
import json
import os
import re
import time
from typing import TYPE_CHECKING

from pymodbus.exceptions import ModbusIOException

from defs.common import strtobool

from ..protocol_settings import (
    Data_Type,
    Registry_Type,
    WriteMode,
    protocol_settings,
    registry_map_entry,
)
from .transport_base import TransportWriteMode, transport_base

if TYPE_CHECKING:
    from configparser import SectionProxy
    try:
        from pymodbus.client.sync import BaseModbusClient
    except ImportError:
        from pymodbus.client import BaseModbusClient


class modbus_base(transport_base):


    #this is specifically static
    clients : dict[str, "BaseModbusClient"] = {}
    ''' str is identifier, dict of clients when multiple transports use the same ports '''

    #non-static here for reference, type hinting, python bs ect...
    modbus_delay_increament : float = 0.05
    ''' delay adjustment every error. todo: add a setting for this '''

    modbus_delay_setting : float = 0.85
    '''time inbetween requests, unmodified'''

    modbus_delay : float = 0.85
    '''time inbetween requests'''

    analyze_protocol_enabled : bool = False
    analyze_protocol_save_load : bool = False
    first_connect : bool = True

    send_holding_register : bool = True
    send_input_register : bool = True

    def __init__(self, settings : "SectionProxy", protocolSettings : "protocol_settings" = None):
        super().__init__(settings)

        self.analyze_protocol_enabled = settings.getboolean("analyze_protocol", fallback=self.analyze_protocol_enabled)
        self.analyze_protocol_save_load = settings.getboolean("analyze_protocol_save_load", fallback=self.analyze_protocol_save_load)


        #get defaults from protocol settings
        if "send_input_register" in self.protocolSettings.settings:
            self.send_input_register = strtobool(self.protocolSettings.settings["send_input_register"])

        if "send_holding_register" in self.protocolSettings.settings:
            self.send_holding_register = strtobool(self.protocolSettings.settings["send_holding_register"])

        if "batch_delay" in self.protocolSettings.settings:
            self.modbus_delay = float(self.protocolSettings.settings["batch_delay"])

        #allow enable/disable of which registers to send
        self.send_holding_register = settings.getboolean("send_holding_register", fallback=self.send_holding_register)
        self.send_input_register = settings.getboolean("send_input_register", fallback=self.send_input_register)
        self.modbus_delay = settings.getfloat(["batch_delay", "modbus_delay"], fallback=self.modbus_delay)
        self.modbus_delay_setting = self.modbus_delay


        if self.analyze_protocol_enabled:
            # Ensure connection is established first
            if not self.connected:
                # Call the child class connect method to establish the actual connection
                super().connect()
            
            # Now call init_after_connect to set up the transport properly
            if self.first_connect:
                self.first_connect = False
                self.init_after_connect()
            
            self.analyze_protocol()
            quit()

    def init_after_connect(self):
        #from transport_base settings
        if self.write_enabled:
            self.enable_write()

        #if sn is empty, attempt to autoread it
        if not self.device_serial_number:
            self.device_serial_number = self.read_serial_number()
            self.update_identifier()

    def connect(self):
        if self.connected and self.first_connect:
            self.first_connect = False
            # Always call init_after_connect when connection is established
            # This ensures proper setup even when analyze_protocol is enabled
            self.init_after_connect()

    def read_serial_number(self) -> str:
        # First try to read "Serial Number" from input registers (for protocols like EG4 v58)
        self._log.info("Looking for serial_number variable in input registers...")
        serial_number = str(self.read_variable("Serial Number", Registry_Type.INPUT))
        self._log.info("read SN from input registers: " + serial_number)
        if serial_number and serial_number != "None":
            return serial_number

        # Then try holding registers (for other protocols)
        self._log.info("Looking for serial_number variable in holding registers...")
        serial_number = str(self.read_variable("Serial Number", Registry_Type.HOLDING))
        self._log.info("read SN from holding registers: " + serial_number)
        if serial_number and serial_number != "None":
            return serial_number

        sn2 = ""
        sn3 = ""
        fields = ["Serial No 1", "Serial No 2", "Serial No 3", "Serial No 4", "Serial No 5"]
        for field in fields:
            self._log.info("Reading " + field)
            registry_entry = self.protocolSettings.get_holding_registry_entry(field)
            if registry_entry is not None:
                self._log.info("Reading " + field + "("+str(registry_entry.register)+")")
                data = self.read_modbus_registers(registry_entry.register, registry_type=Registry_Type.HOLDING)
                if not hasattr(data, "registers") or data.registers is None:
                    self._log.critical("Failed to get serial number register ("+field+") ; exiting")
                    exit()

                serial_number = serial_number  + str(data.registers[0])

                data_bytes = data.registers[0].to_bytes((data.registers[0].bit_length() + 7) // 8, byteorder="big")
                sn2 = sn2 + str(data_bytes.decode("utf-8"))
                sn3 = str(data_bytes.decode("utf-8")) + sn3

            time.sleep(self.modbus_delay*2) #sleep inbetween requests so modbus can rest

        print(sn2)
        print(sn3)

        if not re.search("[^a-zA-Z0-9_]", sn2) :
            serial_number = sn2

        return serial_number

    def enable_write(self):
        if self.write_enabled and self.write_mode == TransportWriteMode.UNSAFE:
            self._log.warning("enable write - WARNING - UNSAFE MODE - validation SKIPPED")
            return

        self._log.info("Validating Protocol for Writing")
        self.write_enabled = False
        
        # Add a small delay to ensure device is ready, especially during initialization
        time.sleep(self.modbus_delay * 2)
        
        try:
            score_percent = self.validate_protocol(Registry_Type.HOLDING)
            if(score_percent > 90):
                self.write_enabled = True
                self._log.warning("enable write - validation passed")
            elif self.write_mode == TransportWriteMode.RELAXED:
                self.write_enabled = True
                self._log.warning("enable write - WARNING - RELAXED MODE")
            else:
                self._log.error("enable write FAILED - WRITE DISABLED")
        except Exception as e:
            self._log.error(f"enable write FAILED due to error: {str(e)}")
            if self.write_mode == TransportWriteMode.RELAXED:
                self.write_enabled = True
                self._log.warning("enable write - WARNING - RELAXED MODE (due to validation error)")
            else:
                self._log.error("enable write FAILED - WRITE DISABLED")



    def write_data(self, data : dict[str, str], from_transport : transport_base) -> None:
        if not self.write_enabled:
            return

        registry_map = self.protocolSettings.get_registry_map(Registry_Type.HOLDING)

        for key, value in data.items():
            for entry in registry_map:
                if entry.variable_name == key:
                    self.write_variable(entry, value, Registry_Type.HOLDING)
                    break

        time.sleep(self.modbus_delay) #sleep inbetween requests so modbus can rest

    def read_data(self) -> dict[str, str]:
        info = {}
        #modbus - only read input/holding registries
        for registry_type in (Registry_Type.INPUT, Registry_Type.HOLDING):

            #enable / disable input/holding register
            if registry_type == Registry_Type.INPUT and not self.send_input_register:
                continue

            if registry_type == Registry_Type.HOLDING and not self.send_holding_register:
                continue

            #calculate ranges dynamically -- for variable read timing
            ranges = self.protocolSettings.calculate_registry_ranges(self.protocolSettings.registry_map[registry_type],
                                                                     self.protocolSettings.registry_map_size[registry_type],
                                                                     timestamp=self.last_read_time)

            registry = self.read_modbus_registers(ranges=ranges, registry_type=registry_type)
            new_info = self.protocolSettings.process_registery(registry, self.protocolSettings.get_registry_map(registry_type))

            if False:
                new_info = {self.__input_register_prefix + key: value for key, value in new_info.items()}

            info.update(new_info)

        if not info:
            self._log.info("Register is Empty; transport busy?")

        return info

    def validate_protocol(self, protocolSettings : "protocol_settings") -> float:
        score_percent = self.validate_registry(Registry_Type.HOLDING)
        return score_percent


    def validate_registry(self, registry_type : Registry_Type = Registry_Type.INPUT) -> float:
        score : float = 0
        info = {}
        registry_map : list[registry_map_entry] = self.protocolSettings.get_registry_map(registry_type)
        info = self.read_registry(registry_type)

        for value in registry_map:
            if value.variable_name in info:
                evaluate = True

                if value.concatenate and value.register != value.concatenate_registers[0]: #only eval concated values once
                    evaluate = False

                if evaluate:
                    score = score + self.protocolSettings.validate_registry_entry(value, info[value.variable_name])

        maxScore = len(registry_map)
        for entry in registry_map: #adjust max score to exclude disabled registers
            if entry.write_mode == WriteMode.WRITEONLY or entry.write_mode == WriteMode.READDISABLED:
                maxScore -= 1

        percent = score*100/maxScore
        self._log.info("validation score: " + str(score) + " of " + str(maxScore) + " : " + str(round(percent)) + "%")
        return percent

    def analyze_protocol(self, settings_dir : str = "protocols"):
        print("=== PROTOCOL ANALYZER ===")
        protocol_names : list[str] = []
        protocols : dict[str,protocol_settings] = {}

        for file in glob.glob(settings_dir + "/*.json"):
            file = file.lower().replace(settings_dir, "").replace("/", "").replace("\\", "").replace("\\", "").replace(".json", "")
            print(file)
            protocol_names.append(file)

        # Use the configured protocol's register ranges instead of maximum from all protocols
        # This prevents trying to read non-existent registers from other protocols
        if hasattr(self, 'protocolSettings') and self.protocolSettings:
            max_input_register = self.protocolSettings.registry_map_size[Registry_Type.INPUT]
            max_holding_register = self.protocolSettings.registry_map_size[Registry_Type.HOLDING]
            print(f"Using configured protocol register ranges: input={max_input_register}, holding={max_holding_register}")
            
            # Use the configured protocol for analysis
            protocols[self.protocolSettings.name] = self.protocolSettings
        else:
            # Fallback to calculating max from all protocols (original behavior)
            max_input_register : int = 0
            max_holding_register : int = 0

            for name in protocol_names:
                protocols[name] = protocol_settings(name)

                if protocols[name].registry_map_size[Registry_Type.INPUT] > max_input_register:
                    max_input_register = protocols[name].registry_map_size[Registry_Type.INPUT]

                if protocols[name].registry_map_size[Registry_Type.HOLDING] > max_holding_register:
                    max_holding_register = protocols[name].registry_map_size[Registry_Type.HOLDING]

            print("max input register: ", max_input_register)
            print("max holding register: ", max_holding_register)

        self.modbus_delay = self.modbus_delay #decrease delay because can probably get away with it due to lots of small reads
        print("read INPUT Registers: ")

        input_save_path = "input_registry.json"
        holding_save_path = "holding_registry.json"

        #load previous scan if enabled and exists
        if self.analyze_protocol_save_load and os.path.exists(input_save_path) and os.path.exists(holding_save_path):
            with open(input_save_path, "r") as file:
                input_registry = json.load(file)

            with open(holding_save_path, "r") as file:
                holding_registry = json.load(file)

            # Convert keys to integers
            input_registry = {int(key): value for key, value in input_registry.items()}
            holding_registry = {int(key): value for key, value in holding_registry.items()}
        else:
            #perform registry scan
            ##batch_size = 1, read registers one by one; if out of bound. it just returns error
            input_registry = self.read_modbus_registers(start=0, end=max_input_register, registry_type=Registry_Type.INPUT)
            holding_registry = self.read_modbus_registers(start=0, end=max_holding_register, registry_type=Registry_Type.HOLDING)

            if self.analyze_protocol_save_load: #save results if enabled
                with open(input_save_path, "w") as file:
                    json.dump(input_registry, file)

                with open(holding_save_path, "w") as file:
                    json.dump(holding_registry, file)

        #print results for debug
        print("=== START INPUT REGISTER ===")
        if input_registry:
            print([(key, value) for key, value in input_registry.items()])
        print("=== END INPUT REGISTER ===")
        print("=== START HOLDING REGISTER ===")
        if holding_registry:
            print([(key, value) for key, value in holding_registry.items()])
        print("=== END HOLDING REGISTER ===")

        #very well possible the registers will be incomplete due to different hardware sizes
        #so dont assume they are set / complete
        #we'll see about the behaviour. if it glitches, this could be a way to determine protocol.


        input_register_score : dict[str, int] = {}
        holding_register_score : dict[str, int] = {}

        input_valid_count : dict[str, int] = {}
        holding_valid_count  : dict[str, int] = {}

        def evaluate_score(entry : registry_map_entry, val):
            score = 0
            if entry.data_type == Data_Type.ASCII:
                if val and not re.match("[^a-zA-Z0-9_-]", val): #validate ascii
                    mod = 1
                    if entry.concatenate:
                        mod = len(entry.concatenate_registers)

                    if entry.value_regex: #regex validation
                        if re.match(entry.value_regex, val):
                            mod = mod * 2
                        else:
                            mod = mod * -2 #regex validation failed, double damage!

                    score = score + (2 * mod) #double points for ascii
                pass
            else: #default type
                if isinstance(val, str):
                    #likely to be a code
                    score = score + 2
                elif val != 0:
                    if val >= entry.value_min and val <= entry.value_max:
                        score = score + 1

                        if entry.value_max != 65535: #double points for non-default range
                            score = score + 1

            return score



        for name, protocol in protocols.items():
            input_register_score[name] = 0
            holding_register_score[name] = 0
            #very rough percentage. tood calc max possible score.
            input_valid_count[name] = 0
            holding_valid_count[name] = 0

            #process registry based on protocol
            input_info = protocol.process_registery(input_registry, protocol.registry_map[Registry_Type.INPUT])
            holding_info = protocol.process_registery(input_registry, protocol.registry_map[Registry_Type.HOLDING])


            for entry in protocol.registry_map[Registry_Type.INPUT]:
                if entry.variable_name in input_info:
                    val = input_info[entry.variable_name]
                    score = evaluate_score(entry, val)
                    if score > 0:
                        input_valid_count[name] = input_valid_count[name] + 1

                    input_register_score[name] = input_register_score[name] + score


            for entry in protocol.registry_map[Registry_Type.HOLDING]:
                if entry.variable_name in holding_info:
                    val = holding_info[entry.variable_name]
                    score = evaluate_score(entry, val)

                    if score > 0:
                        holding_valid_count[name] = holding_valid_count[name] + 1

                    holding_register_score[name] = holding_register_score[name] + score


        protocol_scores: dict[str, int] = {}
        #combine scores
        for name, protocol in protocols.items():
            protocol_scores[name] = input_register_score[name] + holding_register_score[name]

        #print scores
        for name in sorted(protocol_scores, key=protocol_scores.get, reverse=True):
            print("=== "+str(name)+" - "+str(protocol_scores[name])+" ===")
            print("input register score: " + str(input_register_score[name]) + "; valid registers: "+str(input_valid_count[name])+" of " + str(len(protocols[name].get_registry_map(Registry_Type.INPUT))))
            print("holding register score : " + str(holding_register_score[name]) + "; valid registers: "+str(holding_valid_count[name])+" of " + str(len(protocols[name].get_registry_map(Registry_Type.HOLDING))))


    def write_variable(self, entry : registry_map_entry, value : str, registry_type : Registry_Type = Registry_Type.HOLDING):
        """ writes a value to a ModBus register; todo: registry_type to handle other write functions"""

        value = value.strip()

        temp_map = [entry]
        ranges = self.protocolSettings.calculate_registry_ranges(temp_map, self.protocolSettings.registry_map_size[registry_type], init=True) #init=True to bypass timechecks
        registry = self.read_modbus_registers( ranges=ranges, registry_type=registry_type)
        info = self.protocolSettings.process_registery(registry, temp_map)
        #read current value
        #current_registers = self.read_modbus_registers(start=entry.register, end=entry.register, registry_type=registry_type)
        #current_value = current_registers[entry.register]
        current_value = info[entry.variable_name]

        if not self.write_mode == TransportWriteMode.UNSAFE:
            if not self.protocolSettings.validate_registry_entry(entry, current_value):
                return self._log.error(f"WRITE_ERROR: Invalid value in register '{current_value}'. Unsafe to write")
                #raise ValueError(err)

            if not (entry.data_type == Data_Type._16BIT_FLAGS or entry.data_type == Data_Type._8BIT_FLAGS or entry.data_type == Data_Type._32BIT_FLAGS): #skip validation for write; validate further down
                if not self.protocolSettings.validate_registry_entry(entry, value):
                    return self._log.error(f"WRITE_ERROR: Invalid new value, '{value}'. Unsafe to write")

        #handle codes
        if entry.variable_name+"_codes" in self.protocolSettings.codes:
            codes = self.protocolSettings.codes[entry.variable_name+"_codes"]
            for key, val in codes.items():
                if val == value: #convert "string" to key value
                    value = key
                    break

        #apply unit_mod before writing.
        if entry.unit_mod != 1:
            value = int(float(value) / entry.unit_mod) # say unitmod is 0.1. 105*0.1 = 10.5. 10.5 / 0.1 = 105.

        #results[entry.variable_name]
        ushortValue : int = None #ushort
        if entry.data_type == Data_Type.USHORT:
            ushortValue = int(value)
            if ushortValue < 0 or ushortValue > 65535:
                 raise ValueError("Invalid value")
        elif entry.data_type.value > 200 or entry.data_type == Data_Type.BYTE: #bit types
            bit_size = Data_Type.getSize(entry.data_type)

            new_val = int(value)
            if 0 > new_val or new_val > (2**bit_size -1): # Calculate max value for n bits: 2^n - 1
                return self._log.error(f"WRITE_ERROR: Invalid new value, '{value}'. Exceeds value range. Unsafe to write")

            bit_index = entry.register_bit
            bit_mask = ((1 << bit_size) - 1) << bit_index  # Create a mask for extracting X bits starting from bit_index
            clear_mask = ~(bit_mask)  # Mask for clearing the bits to be updated

            # Clear the bits to be updated in the current_value
            ushortValue = registry[entry.register] & clear_mask

            # Set the bits according to the new_value at the specified bit position
            ushortValue |= (new_val << bit_index) & bit_mask

            #bit_size = Data_Type.getSize(entry.data_type)
            bit_mask = (1 << bit_size) - 1  # Create a mask for extracting X bits
            bit_index = entry.register_bit
            check_value = (ushortValue >> bit_index) & bit_mask


            if check_value != new_val:
                raise ValueError("something went wrong bitwise")
        elif entry.data_type == Data_Type._16BIT_FLAGS or entry.data_type == Data_Type._8BIT_FLAGS or entry.data_type == Data_Type._32BIT_FLAGS:
            #16 bit flags
            flag_size : int = Data_Type.getSize(entry.data_type)

            if re.match(rf"^[0-1]{{{flag_size}}}$", value): #bitflag string
                #is string of 01... s
                # Convert binary string to an integer
                value = int(value[::-1], 2) #reverse string

                # Ensure it fits within ushort range (0-65535)
                if value > 65535:
                    return self._log.error(f"WRITE_ERROR: '{value}' Exceeds 65535. Unsafe to write")
            else:
                return self._log.error(f"WRITE_ERROR: Invalid new value for bitflags, '{value}'. Unsafe to write")

            #apply bitmasks
            bit_index = entry.register_bit
            bit_mask = ((1 << flag_size) - 1) << bit_index  # Create a mask for extracting X bits starting from bit_index
            clear_mask = ~(bit_mask)  # Mask for clearing the bits to be updated

            # Clear the bits to be updated in the current_value
            ushortValue = registry[entry.register] & clear_mask

            # Set the bits according to the new_value at the specified bit position
            ushortValue |= (value << bit_index) & bit_mask

        else:
            raise TypeError("Unsupported data type")

        if ushortValue is None:
            raise ValueError("Invalid value - None")

        self._log.info(f"WRITE: {current_value} => {value} ( {registry[entry.register]} => {ushortValue} ) to Register {entry.register}")
        self.write_register(entry.register, ushortValue)
        #entry.next_read_timestamp = 0 #ensure is read next interval


    def read_variable(self, variable_name : str, registry_type : Registry_Type, entry : registry_map_entry = None):
        ##clean for convinecne
        if variable_name:
            variable_name = variable_name.strip().lower().replace(" ", "_")

        registry_map = self.protocolSettings.get_registry_map(registry_type)

        if entry is None:
            for e in registry_map:
                if e.variable_name == variable_name:
                    entry = e
                    break

        if entry:
            start : int = 0
            end : int = 0
            if not entry.concatenate:
                start = entry.register
                end = entry.register
            else:
                start = min(entry.concatenate_registers)
                end = max(entry.concatenate_registers)

            registers = self.read_modbus_registers(start=start, end=end, registry_type=registry_type)
            
            # Special handling for concatenated ASCII variables (like serial numbers)
            if entry.concatenate and entry.data_type == Data_Type.ASCII:
                concatenated_value = ""
                
                # For serial numbers, we need to extract 8-bit ASCII characters from 16-bit registers
                # Each register contains two ASCII characters (low byte and high byte)
                for reg in entry.concatenate_registers:
                    if reg in registers:
                        reg_value = registers[reg]
                        # Extract low byte (bits 0-7) and high byte (bits 8-15)
                        low_byte = reg_value & 0xFF
                        high_byte = (reg_value >> 8) & 0xFF
                        
                        # Convert each byte to ASCII character
                        low_char = chr(low_byte)
                        high_char = chr(high_byte)
                        
                        concatenated_value += low_char + high_char
                    else:
                        self._log.warning(f"Register {reg} not found in registry")
                
                result = concatenated_value.replace("\x00", " ").strip()
                return result
            
            # Only process the specific entry, not the entire registry map
            results = self.protocolSettings.process_registery(registers, [entry])
            result = results.get(entry.variable_name)
            return result
        else:
            self._log.warning(f"Entry not found for variable: {variable_name}")
            return None

    def read_modbus_registers(self, ranges : list[tuple] = None, start : int = 0, end : int = None, batch_size : int = None, registry_type : Registry_Type = Registry_Type.INPUT ) -> dict:
        ''' maybe move this to transport_base ?'''

        # Get batch_size from protocol settings if not provided
        if batch_size is None:
            if hasattr(self, 'protocolSettings') and self.protocolSettings:
                batch_size = self.protocolSettings.settings.get("batch_size", 45)
                try:
                    batch_size = int(batch_size)
                except (ValueError, TypeError):
                    batch_size = 45
            else:
                batch_size = 45

        if not ranges: #ranges is empty, use min max
            if start == 0 and end is None:
                return {} #empty

            end = end + 1
            ranges = []
            start = start - batch_size
            while( start := start + batch_size ) < end:
                count = batch_size
                if start + batch_size > end:
                    count = end - start + 1
                ranges.append((start, count)) ##APPEND TUPLE

        registry : dict[int,] = {}
        retries = 7
        retry = 0
        total_retries = 0

        index = -1
        while (index := index + 1) < len(ranges) :
            range = ranges[index]

            self._log.info("get registers ("+str(index)+"): " +str(registry_type)+ " - " + str(range[0]) + " to " + str(range[0]+range[1]-1) + " ("+str(range[1])+")")
            time.sleep(self.modbus_delay) #sleep for 1ms to give bus a rest #manual recommends 1s between commands

            isError = False
            register = None  # Initialize register variable
            try:
                register = self.read_registers(range[0], range[1], registry_type=registry_type)

            except ModbusIOException as e:
                self._log.error("ModbusIOException: " + str(e))
                # In pymodbus 3.7+, ModbusIOException doesn't have error_code attribute
                # Treat all ModbusIOException as retryable errors
                isError = True


            if register is None or isinstance(register, bytes) or (hasattr(register, 'isError') and register.isError()) or isError: #sometimes weird errors are handled incorrectly and response is a ascii error string
                if register is None:
                    self._log.error("No response received from modbus device")
                elif isinstance(register, bytes):
                    self._log.error(register.decode("utf-8"))
                else:
                    self._log.error(str(register))
                self.modbus_delay += self.modbus_delay_increament #increase delay, error is likely due to modbus being busy

                if self.modbus_delay > 60: #max delay. 60 seconds between requests should be way over kill if it happens
                    self.modbus_delay = 60

                if retry > retries: #instead of none, attempt to continue to read. but with no retires.
                    continue
                else:
                    #undo step in loop and retry read
                    retry = retry + 1
                    total_retries = total_retries + 1
                    self._log.warning("Retry("+str(retry)+" - ("+str(total_retries)+")) range("+str(index)+")")
                    index = index - 1
                    continue
            elif self.modbus_delay > self.modbus_delay_setting: #no error, decrease delay
                self.modbus_delay -= self.modbus_delay_increament
                if self.modbus_delay < self.modbus_delay_setting:
                    self.modbus_delay = self.modbus_delay_setting


            retry -= 1
            if retry < 0:
                retry = 0

            # Only process registers if we have a valid response
            if register is not None and hasattr(register, 'registers') and register.registers is not None:
                #combine registers into "registry"
                i = -1
                while(i := i + 1 ) < range[1]:
                    #print(str(i) + " => " + str(i+range[0]))
                    registry[i+range[0]] = register.registers[i]

        return registry

    def read_registry(self, registry_type : Registry_Type = Registry_Type.INPUT) -> dict[str,str]:
        map = self.protocolSettings.get_registry_map(registry_type)
        if not map:
            return {}

        registry = self.read_modbus_registers(self.protocolSettings.get_registry_ranges(registry_type), registry_type=registry_type)
        info = self.protocolSettings.process_registery(registry, map)
        return info
