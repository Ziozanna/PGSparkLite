#####################################################
# Spark Amp Server Class
#
# Handles two-way communication with a Spark Amp via BLE
#####################################################

import asyncio
import threading

from bleak import BleakClient, BleakScanner
from EventNotifier import Notifier

from lib.common import (dict_amp, dict_bias_noisegate, dict_bias_reverb, dict_BPM, dict_bpm,
                        dict_bpm_change, dict_callback, dict_chain_preset,
                        dict_change_effect, dict_Change_Effect_State,
                        dict_change_parameter, dict_change_preset, dict_comp,
                        dict_connection_lost, dict_connection_message,
                        dict_connection_success, dict_delay, dict_drive,
                        dict_effect, dict_Effect, dict_effect_type, dict_gate,
                        dict_log_change_only, dict_message, dict_mod,
                        dict_Name, dict_name, dict_New_Effect, dict_new_effect,
                        dict_New_Preset, dict_Off, dict_Old_Effect,
                        dict_old_effect, dict_On, dict_OnOff, dict_parameter,
                        dict_Parameter, dict_pedal_chain_preset,
                        dict_pedal_status, dict_preset, dict_preset_corrupt,
                        dict_Preset_Number, dict_preset_stored,
                        dict_refresh_onoff, dict_reverb, dict_state,
                        dict_turn_on_off, dict_update_debug_log,
                        dict_update_effect, dict_update_onoff,
                        dict_update_parameter, dict_update_preset, dict_value,
                        dict_Value, get_amp_effect_name, get_js_effect_name)
from lib.external.SparkClass import SparkMessage
from lib.external.SparkCommsClass import SparkComms, SPARK_CHAR_NOTIFY
from lib.external.SparkReaderClass import SparkReadMessage
from lib.messages import (msg_amp_connected, msg_amp_preset_stored,
                          msg_connection_failed, msg_preset_error,
                          msg_retrieving_config)
from lib.plugins.custom import CustomExpression
from lib.plugins.onoff import OnOff
from lib.plugins.volume import VolumePedal
from lib.sparkdevices import SparkDevices
from lib.sparklistener import SparkListener
from lib.sparkpreset import SparkPreset
import config

SPARK_SERVICE_UUID = "0000ffc0-0000-1000-8000-00805f9b34fb"


class SparkAmpServer:
    def __init__(self, socketio):
        self.socketio = socketio
        self.connected = False
        self.msg = SparkMessage()
        self.listener = None
        self.comms = None
        self.config = None

        self._ble_client = None
        self._ble_loop   = None
        self._ble_thread = None
        self._keepalive_timer = None

        self.notifier = Notifier(
            [dict_callback, dict_connection_lost, dict_preset_corrupt])
        self.notifier.subscribe(dict_callback, self.callback_event)
        self.notifier.subscribe(dict_connection_lost, self.connection_lost_event)
        self.notifier.subscribe(dict_preset_corrupt, self.preset_corrupt_event)

        self.amp_update_count   = 0
        self.chain_update_count = 0
        self.plugin             = None
        self.debug_logging      = False
        self.connect_in_progress = False
        self.disable_expression_pedal = False

    # ------------------------------------------------------------------ #
    #  BLE helpers
    # ------------------------------------------------------------------ #

    def _run_ble_loop(self, loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    async def _ble_connect(self, address=None):
        device = None
        if address:
            device = await BleakScanner.find_device_by_address(address, timeout=10)
            if device is None:
                print(f"Address {address} not found, falling back to UUID scan...")

        if device is None:
            print("Scanning for Spark Go by service UUID...")
            device = await BleakScanner.find_device_by_filter(
                lambda d, adv: SPARK_SERVICE_UUID in (adv.service_uuids or []),
                timeout=15,
            )

        if device is None:
            raise Exception("Spark Go not found — make sure it is powered on and in pairing mode")

        print(f"Found: {device.name} ({device.address})")
        client = BleakClient(device, disconnected_callback=self._on_ble_disconnect)
        await client.connect()
        return client

    def _on_ble_disconnect(self, client):
        print("BLE disconnected (Bleak callback)")
        self._stop_keepalive()
        if self.connected:
            self.connection_lost_event()

    def _start_keepalive(self):
        self._stop_keepalive()
        self._keepalive_timer = threading.Timer(20.0, self._keepalive_tick)
        self._keepalive_timer.daemon = True
        self._keepalive_timer.start()

    def _stop_keepalive(self):
        if self._keepalive_timer:
            self._keepalive_timer.cancel()
            self._keepalive_timer = None

    def _keepalive_tick(self):
        if self.connected and self.comms:
            try:
                self.comms.send_keepalive()
            except Exception:
                pass
            self._start_keepalive()

    # ------------------------------------------------------------------ #
    #  Public interface
    # ------------------------------------------------------------------ #

    def change_to_preset(self, hw_preset):
        cmd = self.msg.change_hardware_preset(hw_preset)
        success = self.comms.send_it(cmd[0])

        if not success:
            self.connection_lost_event()
        else:
            self.request_preset(hw_preset)

        self.log_debug_message("change_to_preset - value " + str(hw_preset))

    def change_effect(self, old_effect, new_effect):
        cmd = self.msg.change_effect(old_effect, new_effect)
        success = self.comms.send_it(cmd[0])

        if not success:
            self.connection_lost_event()

        self.log_debug_message(
            "change_effect - old: " + old_effect + " new: " + new_effect)

        if self.plugin.name == old_effect:
            self.log_debug_message("change_effect - update_plugin")
            self.update_plugin(enabled=False)

    def change_effect_parameter(self, effect, parameter, value):
        cmd = self.msg.change_effect_parameter(effect, parameter, value)
        success = self.comms.send_it(cmd[0])

        if not success:
            self.connection_lost_event()

        self.log_debug_message(
            "change parameter: effect: " + effect
            + " parameter: " + str(parameter)
            + " value: " + str(value))

    def connect(self):
        if self.connect_in_progress:
            return
        self.connect_in_progress = True

        if self.listener:
            self.listener.stop()

        if self._ble_client and self._ble_loop:
            try:
                asyncio.run_coroutine_threadsafe(
                    self._ble_client.disconnect(), self._ble_loop
                ).result(timeout=5)
            except Exception:
                pass

        if self._ble_loop:
            self._ble_loop.call_soon_threadsafe(self._ble_loop.stop)
            self._ble_thread.join(timeout=3)

        try:
            loop = asyncio.new_event_loop()
            self._ble_loop   = loop
            self._ble_thread = threading.Thread(
                target=self._run_ble_loop, args=(loop,), daemon=True)
            self._ble_thread.start()

            future = asyncio.run_coroutine_threadsafe(
                self._ble_connect(config.amp_bt_address), loop)
            client = future.result(timeout=30)
            self._ble_client = client

            self.reader = SparkReadMessage()
            self.comms  = SparkComms(client, loop)

            asyncio.run_coroutine_threadsafe(
                client.start_notify(SPARK_CHAR_NOTIFY, self.comms._on_notify),
                loop,
            ).result(timeout=5)

            self.listener = SparkListener(self.reader, self.comms, self.notifier)
            t = threading.Thread(target=self.listener.start, args=(), daemon=True)
            t.start()

            self.connected = True
            self._start_keepalive()

            self.socketio.emit(dict_connection_message,
                               {dict_message: msg_retrieving_config})
            self.initialise()
            self.socketio.emit(dict_connection_message,
                               {dict_message: msg_amp_connected})

        except Exception as e:
            self.connect_in_progress = False
            print("connect error", e)
            self.socketio.emit(dict_connection_message,
                               {dict_message: msg_connection_failed})
        else:
            self.connect_in_progress = False

    def log_debug_message(self, message):
        if self.debug_logging:
            self.socketio.emit(dict_update_debug_log, {dict_message: message})

    def initialise(self):
        return self.comms.send_state_request()

    def eject(self):
        self._stop_keepalive()
        self.config = None
        self.listener.stop()
        self.request_preset(0)

        if self._ble_client and self._ble_loop:
            try:
                asyncio.run_coroutine_threadsafe(
                    self._ble_client.disconnect(), self._ble_loop
                ).result(timeout=5)
            except Exception:
                pass

        if self._ble_loop:
            self._ble_loop.call_soon_threadsafe(self._ble_loop.stop)

        self.connected = False

    def expression_pedal(self, value):
        if self.disable_expression_pedal:
            self.log_debug_message(
                "expression_pedal - value received but is disabled")
            return

        self.log_debug_message(
            "expression_pedal - value received: " + str(value))

        if self.plugin.type == "param":
            self.log_debug_message("expression_pedal - plugin.type = param")
            params = self.plugin.calculate_params(value)
            if params is None:
                self.log_debug_message("expression_pedal - No params returned")
                return

            for param in params:
                self.change_effect_parameter(
                    get_amp_effect_name(self.plugin.name), param[0], param[1])
                self.socketio.emit(dict_update_parameter, {
                    dict_effect:    self.plugin.name,
                    dict_parameter: param[0],
                    dict_value:     param[1],
                })
            return

        if self.plugin.type == "onoff":
            self.log_debug_message("expression_pedal - plugin.type = onoff")
            isOn = self.plugin.calculate_state(value)
            state = dict_On if isOn else dict_Off
            self.log_debug_message(
                "expression_pedal - isOn = " + str(isOn))
            self.socketio.emit(dict_update_onoff, {
                dict_state:       state,
                dict_effect:      self.plugin.name,
                dict_effect_type: self.plugin.effect_type,
            })
            return

        self.log_debug_message("expression_pedal - plugin.type not found")

    # Slot used as dedicated "chain preset" slot on Spark Go (hardware preset 4).
    # Spark Go has no 0x7f "user slot" like Spark 40, so we use a fixed valid slot.
    CHAIN_PRESET_SLOT = 3

    def send_preset(self, chain_preset):
        self.log_debug_message("send_preset - " + chain_preset.name)
        import time

        target_slot = self.CHAIN_PRESET_SLOT
        chain_preset.preset = target_slot
        spark_preset = SparkPreset(chain_preset, type=dict_chain_preset)
        preset_blocks = self.msg.create_preset(spark_preset.json())

        print(f"send_preset: uploading {len(preset_blocks)} block(s) to slot {target_slot}", flush=True)
        for block in preset_blocks:
            self.comms.send_it(block)

        # Wait for amp ACK (cmd=05/sub=01) before switching
        time.sleep(0.5)

        # Switch to the target slot — amp loads the newly uploaded data
        print(f"send_preset: switching to slot {target_slot}", flush=True)
        self.comms.send_it(self.msg.change_hardware_preset(target_slot)[0])

        self.config.parse_chain_preset(chain_preset)

    def set_bpm(self, bpm):
        self.config.bpm = bpm
        spark_preset = SparkPreset(self.config, bpm=True)
        preset = self.msg.create_preset(spark_preset.json())

        for i in preset:
            self.comms.send_it(i)

        change_user_preset = self.msg.change_hardware_preset(0x7f)
        self.comms.send_it(change_user_preset[0])

        self.log_debug_message("set_bpm - value " + str(bpm))

    def store_amp_preset(self):
        spark_preset = SparkPreset(self.config)
        preset = self.msg.create_preset(spark_preset.json())

        for i in preset:
            self.comms.send_it(i)

        change_user_preset = self.msg.change_hardware_preset(self.config.preset)
        self.comms.send_it(change_user_preset[0])

    def toggle_effect_onoff(self, effect_type):
        effect      = None
        effect_name = None

        if effect_type == dict_drive:
            effect = self.config.drive
        elif effect_type == dict_mod:
            effect = self.config.modulation
        elif effect_type == dict_delay:
            effect = self.config.delay
        elif effect_type == dict_reverb:
            effect = self.config.reverb
            effect_name = self.config.reverb[dict_Name]
        elif effect_type == dict_gate:
            effect = self.config.gate
        elif effect_type == dict_comp:
            effect = self.config.comp
        elif effect_type == dict_amp:
            effect = self.config.amp

        if effect is None:
            return {}

        if effect_name is None:
            effect_name = get_js_effect_name(effect[dict_Name])

        state = dict_Off
        if effect[dict_OnOff] == dict_Off:
            state = dict_On

        self.turn_effect_onoff(get_amp_effect_name(effect[dict_Name]), state)
        self.config.update_config(effect[dict_Name], dict_turn_on_off, state)
        self.config.last_call = dict_turn_on_off

        self.log_debug_message(
            "toggle_effect_onoff - effect_type: " + effect_type
            + " state: " + state)

        return {dict_effect: effect_name, dict_state: state,
                dict_effect_type: effect_type}

    def turn_effect_onoff(self, effect, state):
        cmd = self.msg.turn_effect_onoff(effect, state)
        self.comms.send_it(cmd[0])
        self.log_debug_message(
            "turn_effect_onoff - effect: " + effect + " state: " + state)

    def request_preset(self, hw_preset):
        self.comms.send_preset_request(hw_preset)

    # ------------------------------------------------------------------ #
    #  Utility
    # ------------------------------------------------------------------ #

    def get_pedal_status(self):
        if self.config is None:
            return {}

        return {
            dict_drive:         self.config.drive[dict_OnOff],
            dict_delay:         self.config.delay[dict_OnOff],
            dict_mod:           self.config.modulation[dict_OnOff],
            dict_reverb:        self.config.reverb[dict_OnOff],
            dict_preset:        self.config.preset,
            dict_BPM:           str(int(self.config.bpm)),
            dict_Name:          self.config.presetName,
            dict_chain_preset:  self.config.chain_preset_id,
        }

    def load_inbound_data(self, data):
        if self.config is None:
            self.config = SparkDevices(data)
        else:
            self.config.parse_preset(data)
            # Restore chain_preset_id if the amp echoed back a known chain preset UUID
            try:
                from database.service import get_chain_preset_by_uuid
                match = get_chain_preset_by_uuid(self.config.uuid)
                if match:
                    self.config.chain_preset_id = match.id
            except Exception:
                pass

        self.update_plugin()

        self.socketio.emit(dict_pedal_status, self.get_pedal_status())
        self.socketio.emit(dict_connection_success, {'url': '/'})

    def update_plugin(self, effect_name=None, param=None,
                      enabled=None, effect_type=None):
        if effect_name is None or enabled is False:
            amp = self.config.get_current_effect_by_type(dict_amp)
            self.plugin = VolumePedal(amp[dict_Name])
        elif param is None:
            self.plugin = OnOff(effect_name, effect_type)
        else:
            self.plugin = CustomExpression(str(effect_name), param)

    # ------------------------------------------------------------------ #
    #  Event handling
    # ------------------------------------------------------------------ #

    def callback_event(self, data):
        if dict_New_Preset in data:
            preset = data[dict_New_Preset]
            self.socketio.emit(dict_update_preset, {dict_value: preset})
            self.request_preset(preset)
            return

        if dict_Preset_Number in data:
            if self.config is not None and self.config.last_call != '':
                cancel = False
                if self.config.last_call == dict_turn_on_off:
                    cancel = True
                elif self.config.last_call == dict_change_effect:
                    cancel = True
                elif self.config.last_call == dict_chain_preset:
                    cancel = True
                elif self.config.last_call == dict_preset_stored:
                    self.socketio.emit(dict_preset_stored,
                                       {dict_message: msg_amp_preset_stored})
                    cancel = True
                elif (self.config.last_call == dict_pedal_chain_preset
                      or self.config.last_call == dict_change_preset):
                    self.load_inbound_data(data)
                    cancel = True

                if cancel:
                    self.config.last_call = ''
                    return

            if (self.config is None
                    or self.config.preset != data[dict_Preset_Number]):
                self.load_inbound_data(data)
            return

        if dict_Old_Effect in data:
            if self.config.last_call == dict_change_effect:
                self.config.last_call = ''
                return

            old_effect = get_js_effect_name(data[dict_Old_Effect])
            new_effect = get_js_effect_name(data[dict_New_Effect])

            self.socketio.emit(dict_update_effect, {
                dict_old_effect:     old_effect,
                dict_effect_type:    dict_amp,
                dict_new_effect:     new_effect,
                dict_log_change_only: False,
            })
            self.config.update_config(old_effect, dict_change_effect, new_effect)
            return

        if dict_Change_Effect_State in data:
            effect      = data[dict_Change_Effect_State]
            effect_type = self.config.get_type_by_effect_name(effect)
            state       = data[dict_OnOff]

            self.socketio.emit(dict_refresh_onoff, {
                dict_effect:      effect,
                dict_state:       state,
                dict_effect_type: effect_type,
            })
            self.config.update_config(effect, dict_turn_on_off, state)

        if dict_Effect in data:
            if self.config.last_call == dict_turn_on_off:
                self.config.last_call = ''
                return

            if data[dict_Effect] == dict_bias_reverb:
                effect = self.config.reverb[dict_Name]
            else:
                effect = get_js_effect_name(data[dict_Effect])

            parameter = data[dict_Parameter]
            value     = data[dict_Value]

            self.config.update_config(effect, dict_change_parameter,
                                      value, parameter)
            self.socketio.emit(dict_update_parameter, {
                dict_effect:    effect,
                dict_parameter: parameter,
                dict_value:     value,
            })

            state = self.config.switch_onoff_parameter(effect, parameter, value)
            if state is None:
                return

            self.socketio.emit(dict_update_onoff, {
                dict_effect:      effect,
                dict_state:       state[1],
                dict_effect_type: state[0],
            })
            self.config.update_config(effect, dict_turn_on_off, state)

        if dict_BPM in data:
            self.config.bpm = data[dict_BPM]
            self.socketio.emit(dict_bpm_change, {dict_bpm: int(data[dict_BPM])})

    def connection_lost_event(self):
        self.connected = False
        self.socketio.emit('connection-lost', {'url': '/'})

    def preset_corrupt_event(self):
        self.connected = False
        self.socketio.emit(dict_connection_message,
                           {dict_message: msg_preset_error})
