import asyncio
import queue


SPARK_CHAR_WRITE   = "0000ffc1-0000-1000-8000-00805f9b34fb"
SPARK_CHAR_NOTIFY  = "0000ffc2-0000-1000-8000-00805f9b34fb"


class SparkComms:
    def __init__(self, client, loop):
        self._client = client
        self._loop   = loop
        self._rx_buf = queue.Queue()

    # Called from the asyncio loop when the amp sends a notification
    def _on_notify(self, sender, data: bytearray):
        # decode SysEx header for readability: cmd sub
        info = ""
        d = bytes(data)
        if len(d) >= 6 and d[0] == 0xf0:
            info = f" cmd={d[4]:02x} sub={d[5]:02x}"
        print(f"RX ({len(data)}){info}: {data.hex()}", flush=True)
        for b in data:
            self._rx_buf.put(bytes([b]))

    # ------------------------------------------------------------------ #
    #  Low-level send / receive
    # ------------------------------------------------------------------ #

    def send_it(self, dat):
        try:
            # decode for readability
            info = ""
            if len(dat) >= 6 and dat[0:2] == b'\x01\xfe':
                sysex = dat[16:] if len(dat) > 16 else b''
                if len(sysex) >= 6 and sysex[0] == 0xf0:
                    info = f" cmd={sysex[4]:02x} sub={sysex[5]:02x}"
            elif len(dat) >= 6 and dat[0] == 0xf0:
                info = f" cmd={dat[4]:02x} sub={dat[5]:02x}"
            print(f"TX ({len(dat)}){info}: {dat.hex()}", flush=True)
            future = asyncio.run_coroutine_threadsafe(
                self._client.write_gatt_char(SPARK_CHAR_WRITE, dat, response=False),
                self._loop,
            )
            future.result(timeout=5)
            return True
        except Exception as e:
            print(f"BLE send error: {e}")
            return False

    def read_it(self, dat_len):
        try:
            result = b''
            for _ in range(dat_len):
                result += self._rx_buf.get(timeout=10)
            return result
        except queue.Empty:
            return None

    # ------------------------------------------------------------------ #
    #  Protocol helpers (unchanged from original)
    # ------------------------------------------------------------------ #

    def send_ack(self, seq, cmd):
        ack = bytes.fromhex(
            "01fe000041ff17000000000000000000f001"
            + "%0.2X" % seq + "0004" + "%0.2X" % cmd + "f7"
        )
        self.send_it(ack)

    def send_preset_request(self, preset):
        preset_hex = "%0.2x" % preset
        # SysEx only (no \x01\xfe wrapper) — Spark Go BLE expects bare SysEx
        arg = (
            "f0010400" "0201"
            "00" "00" + preset_hex
            + "00000000000000000000000000000000"
              "00000000000000000000000000000000" "0000f7"
        )
        print(f"BLE send preset_request: {arg}", flush=True)
        self.send_it(bytes.fromhex(arg))

    def send_keepalive(self):
        # Lightweight ping: asks for current preset number (cmd=02, sub=10).
        # The amp responds with cmd=03/sub=10 which the listener filters out.
        arg = "f0010400" "0210" + "00" * 34 + "f7"
        self.send_it(bytes.fromhex(arg))

    def send_state_request(self):
        # SysEx only (no \x01\xfe wrapper) — Spark Go BLE expects bare SysEx
        arg = (
            "f0010400" "0210"
            "00" "0000"
            "0000000000000000000000000000000000"
            "000000000000000000000000000000"
            "0000f7"
        )
        print(f"BLE send state_request(1): {arg}", flush=True)
        self.send_it(bytes.fromhex(arg))

        arg = (
            "f0010400" "0201"
            "00" "0100"
            "0000000000000000000000000000000000"
            "000000000000000000000000000000"
            "0000f7"
        )
        print(f"BLE send state_request(2): {arg}", flush=True)
        self.send_it(bytes.fromhex(arg))

    def get_block(self):
        # Spark Go BLE sends raw SysEx chunks (f0...f7) without the \x01\xfe
        # block wrapper used by classic BT. Read one SysEx chunk and wrap it
        # in a synthetic \x01\xfe header so SparkReaderClass works unchanged.

        # Phase 1: wait indefinitely for SysEx start 0xf0.
        # The amp is silent between user interactions — no timeout here.
        rd_data = b''
        while True:
            try:
                b = self._rx_buf.get(timeout=1.0)
            except queue.Empty:
                continue
            if b == b'\xf0':
                rd_data = b'\xf0'
                break

        # Phase 2: once SysEx started, read until 0xf7 with normal timeout.
        # If the link drops mid-SysEx, discard and wait for the next 0xf0.
        while True:
            b = self.read_it(1)
            if b is None:
                print("BLE: mid-SysEx timeout, discarding partial chunk", flush=True)
                rd_data = b''
                # restart from phase 1
                while True:
                    try:
                        b = self._rx_buf.get(timeout=1.0)
                    except queue.Empty:
                        continue
                    if b == b'\xf0':
                        rd_data = b'\xf0'
                        break
                continue
            rd_data += b
            if b == b'\xf7':
                break

        # Synthetic block: \x01\xfe\x00\x00\x41\xff + [len] + 9×\x00 + SysEx
        # SparkReaderClass.structure_data() reads block[6]=length, block[16:]=SysEx
        block_len = min(16 + len(rd_data), 255)
        header = b'\x01\xfe\x00\x00\x41\xff' + bytes([block_len]) + b'\x00' * 9
        return header + rd_data

    def get_data(self):
        resp = []
        while True:
            blk = self.get_block()
            resp.append(blk)
            # SysEx content: f0 01 [seq] [chk] [cmd] [sub] [7bit-data...] f7
            sysex = blk[16:]
            if len(sysex) < 7:
                break
            cmd     = sysex[4]
            sub_cmd = sysex[5]
            # Multi-chunk preset messages (cmd=1 or 3, sub=1) arrive as
            # consecutive SysEx notifications. sysex[6]=bitmask, sysex[7]=
            # num_chunks, sysex[8]=this_chunk (both < 128, bitmask irrelevant).
            if cmd in (0x01, 0x03) and sub_cmd == 0x01 and len(sysex) > 8:
                num_chunks = sysex[7]
                this_chunk = sysex[8]
                if this_chunk + 1 < num_chunks:
                    continue
            break
        return resp
