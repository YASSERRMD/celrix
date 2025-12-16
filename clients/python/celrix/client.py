import asyncio
import io

class Client:
    def __init__(self, host='127.0.0.1', port=6380):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port)

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def set(self, key: str, value: str) -> bool:
        await self.send_command("SET", key, value)
        return await self.expect_ok()

    async def get(self, key: str) -> str:
        await self.send_command("GET", key)
        return await self.read_response()

    async def del_key(self, key: str) -> bool:
        await self.send_command("DEL", key)
        res = await self.read_response()
        return res > 0

    async def send_command(self, *args):
        # *<num_args>\r\n$<len>\r\n<arg>\r\n...
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            arg_str = str(arg)
            cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
        self.writer.write(cmd.encode())
        await self.writer.drain()

    async def expect_ok(self) -> bool:
        res = await self.read_response()
        if res == "OK":
            return True
        raise Exception(f"Expected OK, got {res}")

    async def read_response(self):
        line = await self.reader.readline()
        if not line:
            raise Exception("Connection closed")
        
        line = line.decode().strip()
        if not line:
            return None

        prefix = line[0]
        content = line[1:]

        if prefix == '+':
            return content
        elif prefix == '-':
            raise Exception(content)
        elif prefix == ':':
            return int(content)
        elif prefix == '$':
            length = int(content)
            if length == -1:
                return None
            data = await self.reader.read(length + 2)
            return data[:length].decode()
        elif prefix == '*':
            count = int(content)
            if count == -1:
                return None
            res = []
            for _ in range(count):
                res.append(await self.read_response())
            return res
        else:
            raise Exception(f"Unknown response prefix: {prefix}")
