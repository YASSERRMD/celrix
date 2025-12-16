import * as net from 'net';

const MAGIC = Buffer.from('CELX');
const VERSION = 1;
const HEADER_SIZE = 22;

export enum OpCode {
    Ping = 0x01,
    Pong = 0x02,
    Get = 0x03,
    Set = 0x04,
    Del = 0x05,
    Exists = 0x06,

    Ok = 0x10,
    Error = 0x11,
    Value = 0x12,
    Nil = 0x13,
    Integer = 0x14,
    Array = 0x15,

    VAdd = 0x20,
    VSearch = 0x21,
}

export class Client {
    private socket: net.Socket;
    private reader: ResponseReader;
    private pending: Array<{ resolve: (val: any) => void, reject: (err: Error) => void }>;
    private nextReqId: bigint = BigInt(1);

    constructor(private host: string = '127.0.0.1', private port: number = 6380) {
        this.socket = new net.Socket();
        this.reader = new ResponseReader();
        this.pending = [];
    }

    async connect(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.socket.connect(this.port, this.host, () => {
                resolve();
            });
            this.socket.on('error', (err) => {
                reject(err);
                this.pending.forEach(p => p.reject(err));
                this.pending = [];
            });
            this.socket.on('data', (data: Buffer) => {
                this.reader.append(data);
                this.processResponses();
            });
        });
    }

    close(): void {
        this.socket.end();
    }

    async ping(): Promise<void> {
        await this.sendFrame(OpCode.Ping, Buffer.alloc(0));
        const res = await this.nextResponse();
        if (res !== 'PONG') throw new Error(`Expected PONG, got ${res}`);
    }

    async set(key: string, value: string): Promise<void> {
        const keyBuf = Buffer.from(key);
        const valBuf = Buffer.from(value);

        // [key_len][key][val_len][val][ttl]
        const payload = Buffer.alloc(4 + keyBuf.length + 4 + valBuf.length + 8);
        let offset = 0;

        payload.writeUInt32BE(keyBuf.length, offset); offset += 4;
        keyBuf.copy(payload, offset); offset += keyBuf.length;

        payload.writeUInt32BE(valBuf.length, offset); offset += 4;
        valBuf.copy(payload, offset); offset += valBuf.length;

        payload.writeBigUInt64BE(BigInt(0), offset); // TTL 0

        await this.sendFrame(OpCode.Set, payload);
        await this.expectOk();
    }

    async get(key: string): Promise<string | null> {
        const keyBuf = Buffer.from(key);
        const payload = Buffer.alloc(4 + keyBuf.length);
        payload.writeUInt32BE(keyBuf.length, 0);
        keyBuf.copy(payload, 4);

        await this.sendFrame(OpCode.Get, payload);
        return await this.nextResponse();
    }

    async del(key: string): Promise<boolean> {
        const keyBuf = Buffer.from(key);
        const payload = Buffer.alloc(4 + keyBuf.length);
        payload.writeUInt32BE(keyBuf.length, 0);
        keyBuf.copy(payload, 4);

        await this.sendFrame(OpCode.Del, payload);
        const res = await this.nextResponse();
        return (res as number) > 0;
    }

    async vadd(key: string, vector: number[]): Promise<void> {
        const keyBuf = Buffer.from(key);
        // [key_len][key][count][f32...]
        const payload = Buffer.alloc(4 + keyBuf.length + 4 + (vector.length * 4));
        let offset = 0;

        payload.writeUInt32BE(keyBuf.length, offset); offset += 4;
        keyBuf.copy(payload, offset); offset += keyBuf.length;

        payload.writeUInt32BE(vector.length, offset); offset += 4;
        for (const f of vector) {
            payload.writeFloatBE(f, offset); offset += 4;
        }

        await this.sendFrame(OpCode.VAdd, payload);
        await this.expectOk();
    }

    async vsearch(vector: number[], k: number): Promise<string[]> {
        // [count][f32...][k]
        const payload = Buffer.alloc(4 + (vector.length * 4) + 4);
        let offset = 0;

        payload.writeUInt32BE(vector.length, offset); offset += 4;
        for (const f of vector) {
            payload.writeFloatBE(f, offset); offset += 4;
        }
        payload.writeUInt32BE(k, offset);

        await this.sendFrame(OpCode.VSearch, payload);
        const res = await this.nextResponse();
        if (!Array.isArray(res)) throw new Error(`Expected array, got ${res}`);
        return res as string[];
    }

    private async expectOk() {
        const res = await this.nextResponse();
        if (res !== 'OK') throw new Error(`Expected OK, got ${res}`);
    }

    private async sendFrame(opcode: OpCode, payload: Buffer): Promise<void> {
        const header = Buffer.alloc(HEADER_SIZE);
        MAGIC.copy(header, 0);
        header.writeUInt8(VERSION, 4);
        header.writeUInt8(opcode, 5);
        header.writeUInt16BE(0, 6); // flags
        header.writeUInt32BE(payload.length, 8);
        header.writeBigUInt64BE(this.nextReqId++, 12);
        header.writeUInt16BE(0, 20); // reserved

        return new Promise((resolve, reject) => {
            this.socket.write(header);
            this.socket.write(payload, (err) => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    private nextResponse(): Promise<any> {
        return new Promise((resolve, reject) => {
            this.pending.push({ resolve, reject });
            this.processResponses();
        });
    }

    private processResponses(): void {
        while (this.pending.length > 0) {
            try {
                const res = this.reader.parse();
                if (res === undefined) {
                    return; // Incomplete
                }
                const p = this.pending.shift();
                if (res instanceof Error) {
                    p?.reject(res);
                } else {
                    p?.resolve(res);
                }
            } catch (err: any) {
                const p = this.pending.shift();
                p?.reject(err);
            }
        }
    }
}

class ResponseReader {
    private buffer: Buffer;

    constructor() {
        this.buffer = Buffer.alloc(0);
    }

    append(data: Buffer): void {
        this.buffer = Buffer.concat([this.buffer, data]);
    }

    parse(): any | undefined {
        if (this.buffer.length < HEADER_SIZE) return undefined;

        // Check magic
        if (this.buffer.subarray(0, 4).toString() !== 'CELX') {
            throw new Error("Invalid magic bytes");
        }

        const opcode = this.buffer.readUInt8(5);
        const payloadLen = this.buffer.readUInt32BE(8);

        if (this.buffer.length < HEADER_SIZE + payloadLen) {
            return undefined; // Wait for full payload
        }

        // Extract payload
        const payload = this.buffer.subarray(HEADER_SIZE, HEADER_SIZE + payloadLen);
        this.buffer = this.buffer.subarray(HEADER_SIZE + payloadLen);

        switch (opcode) {
            case OpCode.Ok: return 'OK';
            case OpCode.Pong: return 'PONG';
            case OpCode.Nil: return null;
            case OpCode.Error: return new Error(payload.toString());
            case OpCode.Value: return payload.toString();
            case OpCode.Integer: {
                if (payload.length < 8) throw new Error("Invalid integer payload");
                // JS number is double (53-bit int safe), might lose precision for full 64-bit
                // But for client simple use, we return Number or BigInt?
                // Let's use Number for convenience, unless > MAX_SAFE_INTEGER
                const val = payload.readBigInt64BE(0);
                return Number(val);
            }
            case OpCode.Array: {
                // [count: u32][len1: u32][bytes1]...
                if (payload.length < 4) return [];
                let offset = 0;
                const count = payload.readUInt32BE(offset); offset += 4;
                const items = [];
                for (let i = 0; i < count; i++) {
                    if (offset + 4 > payload.length) throw new Error("Incomplete array");
                    const itemLen = payload.readUInt32BE(offset); offset += 4;
                    if (offset + itemLen > payload.length) throw new Error("Incomplete array item");
                    items.push(payload.subarray(offset, offset + itemLen).toString());
                    offset += itemLen;
                }
                return items;
            }
            default: throw new Error(`Unknown opcode: ${opcode}`);
        }
    }
}
