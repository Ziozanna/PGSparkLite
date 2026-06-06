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
        for b in data:
            self._rx_buf.put(bytes([b]))

    # ------------------------------------------------------------------ #
    #  Low-level send / receive
    # ------------------------------------------------------------------ #

    def send_it(self, dat):
        try:
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
        arg = (
            "01fe000053fe3c000000000000000000"
            "f0010400" "0201"
            "00" "00" + preset_hex
            + "00000000000000000000000000000000"
              "00000000000000000000000000000000" "0000f7"
        )
        self.send_it(bytes.fromhex(arg))

    def send_state_request(self):
        arg = (
            "01fe000053fe3c000000000000000000"
            "f0010400" "0210"
            "00" "0000"
            "0000000000000000000000000000000000"
            "000000000000000000000000000000"
            "0000f7"
        )
        self.send_it(bytes.fromhex(arg))

        arg = (
            "01fe000053fe3c000000000000000000"
            "f0010400" "0201"
            "00" "0100"
            "0000000000000000000000000000000000"
            "000000000000000000000000000000"
            "0000f7"
        )
        self.send_it(bytes.fromhex(arg))

    def get_block(self):
        rd_data  = b''
        read_len = 1
        a = -1
        while a == -1:
            chunk = self.read_it(read_len)
            if chunk is None:
                raise ConnectionError("BLE read timeout")
            rd_data += chunk
            a = rd_data.find(b'\x01\xfe')
            if a >= 0:
                rd_data = rd_data[a:]
                while len(rd_data) < 7:
                    rd_data += self.read_it(read_len)
                blk_len = rd_data[6]
                while len(rd_data) < blk_len:
                    rd_data += self.read_it(read_len)
                res     = rd_data[:blk_len]
                rd_data = rd_data[blk_len:]
        return res

    def get_data(self):
        resp       = []
        last_block = False

        while not last_block:
            blk = self.get_block()
            resp.append(blk)

            blk_len   = blk[6]
            direction = blk[4:6]
            seq       = blk[18]
            cmd       = blk[20]
            sub_cmd   = blk[21]

            if direction == b'\x53\xfe' and cmd == 0x01 and sub_cmd != 0x04:
                self.send_ack(seq, cmd)

            if direction == b'\x53\xfe':
                if blk_len < 0xad:
                    last_block = True
                else:
                    num_chunks = blk[23]
                    this_chunk = blk[24]
                    if this_chunk + 1 == num_chunks:
                        last_block = True

            if direction == b'\x41\xff':
                if blk_len < 0x6a:
                    last_block = True
                else:
                    pos = blk.rfind(b'\xf0\x01')
                    try:
                        num_chunks = blk[pos + 7]
                        this_chunk = blk[pos + 8]
                        if this_chunk == len(resp) and this_chunk + 1 == num_chunks:
                            last_block = True
                    except Exception:
                        pass
        return resp
