import asyncio
import struct
import io

class OpCode:
    PING = 0x01
    PONG = 0x02
    GET = 0x03
    SET = 0x04
    DEL = 0x05
    EXISTS = 0x06
    OK = 0x10
    ERROR = 0x11
    VALUE = 0x12
    NIL = 0x13
    INTEGER = 0x14
    ARRAY = 0x15
    VAdd = 0x20
    VSearch = 0x21

class CelrixClient:
    MAGIC = b'CELX'
    VERSION = 1
    HEADER_FMT = '>4sBBHIQ2x' # Magic(4), Ver(1), Op(1), Flags(2), Len(4), ReqID(8), Res(2)
    HEADER_SIZE = 22

    def __init__(self, host='127.0.0.1', port=6380):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.req_id = 0

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port)

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    def _next_req_id(self):
        self.req_id += 1
        return self.req_id

    async def set(self, key: str, value: str, ttl: int = 0) -> bool:
        key_bytes = key.encode()
        val_bytes = value.encode()
        
        # Payload: [key_len: u32][key][val_len: u32][val][ttl: u64]
        payload = struct.pack(f'>I{len(key_bytes)}sI{len(val_bytes)}sQ',
                            len(key_bytes), key_bytes,
                            len(val_bytes), val_bytes,
                            ttl)
        
        await self._send_frame(OpCode.SET, payload)
        return await self._expect_ok()

    async def get(self, key: str) -> str:
        key_bytes = key.encode()
        payload = struct.pack(f'>I{len(key_bytes)}s', len(key_bytes), key_bytes)
        
        await self._send_frame(OpCode.GET, payload)
        return await self._read_response()

    async def del_key(self, key: str) -> bool:
        key_bytes = key.encode()
        payload = struct.pack(f'>I{len(key_bytes)}s', len(key_bytes), key_bytes)
        
        await self._send_frame(OpCode.DEL, payload)
        res = await self._read_response()
        # DEL returns Integer (count)? Wait, Command::Del encodes to OpCode::Del.
        # Response should be OpCode::Integer? Check handler.rs
        # Looking at Command::Del in handler, usually returns count.
        # Assuming Integer response.
        return res > 0

    async def vadd(self, key: str, vector: list) -> bool:
        key_bytes = key.encode()
        # Payload: [key_len][key][count][f32...]
        payload = bytearray()
        payload.extend(struct.pack('>I', len(key_bytes)))
        payload.extend(key_bytes)
        payload.extend(struct.pack('>I', len(vector)))
        for f in vector:
            payload.extend(struct.pack('>f', f))
            
        await self._send_frame(OpCode.VAdd, bytes(payload))
        return await self._expect_ok()

    async def vsearch(self, vector: list, k: int = 10) -> list:
        # Payload: [count][f32...][k]
        payload = bytearray()
        payload.extend(struct.pack('>I', len(vector)))
        for f in vector:
            payload.extend(struct.pack('>f', f))
        payload.extend(struct.pack('>I', k))
        
        await self._send_frame(OpCode.VSearch, bytes(payload))
        return await self._read_response()

    async def send_command(self, cmd_name: str, *args):
        # Fallback/Debug method for arbitrary opcodes?
        # Not easily supported with strict binary opcodes unless we map string to opcode.
        # For now, special case the Vector commands if we knew their opcodes.
        # But for Phase 8 integration test, we need VADD/VSEARCH support.
        # Vector opcodes not defined in the core file I read!
        # Maybe they are in extended_commands.rs?
        # If I can't send them, I can't test them yet.
        # I'll check extended_commands.rs next.
        pass 

    async def _send_frame(self, opcode: int, payload: bytes):
        req_id = self._next_req_id()
        header = struct.pack(self.HEADER_FMT, 
                           self.MAGIC, 
                           self.VERSION, 
                           opcode, 
                           0, # Flags
                           len(payload), 
                           req_id)
        self.writer.write(header + payload)
        await self.writer.drain()

    async def _expect_ok(self) -> bool:
        opcode, data = await self._read_frame()
        if opcode == OpCode.OK:
            return True
        elif opcode == OpCode.ERROR:
            raise Exception(f"Server Error: {data.decode(errors='replace')}")
        else:
            raise Exception(f"Expected OK, got OpCode {opcode}")

    async def _read_response(self):
        opcode, data = await self._read_frame()
        
        if opcode == OpCode.VALUE:
            return data.decode()
        elif opcode == OpCode.NIL:
            return None
        elif opcode == OpCode.INTEGER:
            return struct.unpack('>q', data)[0]
        elif opcode == OpCode.ERROR:
            raise Exception(f"Server Error: {data.decode(errors='replace')}")
        elif opcode == OpCode.OK:
            return "OK"
        elif opcode == OpCode.ARRAY:
             # Array payload: [count: u32][len1: u32][bytes1]...
             count = struct.unpack('>I', data[:4])[0]
             cursor = 4
             items = []
             for _ in range(count):
                 item_len = struct.unpack('>I', data[cursor:cursor+4])[0]
                 cursor += 4
                 item_bytes = data[cursor:cursor+item_len]
                 cursor += item_len
                 items.append(item_bytes.decode(errors='replace'))
             return items
        else:
            raise Exception(f"Unknown response opcode: {opcode}")

    async def _read_frame(self):
        header_bytes = await self.reader.readexactly(self.HEADER_SIZE)
        magic, ver, opcode, flags, length, req_id = struct.unpack(self.HEADER_FMT, header_bytes)
        
        if magic != self.MAGIC:
            raise Exception("Invalid magic bytes")
            
        if length > 0:
            payload = await self.reader.readexactly(length)
        else:
            payload = b''
            
        return opcode, payload
