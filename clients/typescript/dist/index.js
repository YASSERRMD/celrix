"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.Client = exports.OpCode = void 0;
const net = __importStar(require("net"));
const MAGIC = Buffer.from('CELX');
const VERSION = 1;
const HEADER_SIZE = 22;
var OpCode;
(function (OpCode) {
    OpCode[OpCode["Ping"] = 1] = "Ping";
    OpCode[OpCode["Pong"] = 2] = "Pong";
    OpCode[OpCode["Get"] = 3] = "Get";
    OpCode[OpCode["Set"] = 4] = "Set";
    OpCode[OpCode["Del"] = 5] = "Del";
    OpCode[OpCode["Exists"] = 6] = "Exists";
    OpCode[OpCode["Ok"] = 16] = "Ok";
    OpCode[OpCode["Error"] = 17] = "Error";
    OpCode[OpCode["Value"] = 18] = "Value";
    OpCode[OpCode["Nil"] = 19] = "Nil";
    OpCode[OpCode["Integer"] = 20] = "Integer";
    OpCode[OpCode["Array"] = 21] = "Array";
    OpCode[OpCode["VAdd"] = 32] = "VAdd";
    OpCode[OpCode["VSearch"] = 33] = "VSearch";
})(OpCode || (exports.OpCode = OpCode = {}));
class Client {
    constructor(host = '127.0.0.1', port = 6380) {
        this.host = host;
        this.port = port;
        this.nextReqId = BigInt(1);
        this.socket = new net.Socket();
        this.reader = new ResponseReader();
        this.pending = [];
    }
    async connect() {
        return new Promise((resolve, reject) => {
            this.socket.connect(this.port, this.host, () => {
                resolve();
            });
            this.socket.on('error', (err) => {
                reject(err);
                this.pending.forEach(p => p.reject(err));
                this.pending = [];
            });
            this.socket.on('data', (data) => {
                this.reader.append(data);
                this.processResponses();
            });
        });
    }
    close() {
        this.socket.end();
    }
    async ping() {
        await this.sendFrame(OpCode.Ping, Buffer.alloc(0));
        const res = await this.nextResponse();
        if (res !== 'PONG')
            throw new Error(`Expected PONG, got ${res}`);
    }
    async set(key, value) {
        const keyBuf = Buffer.from(key);
        const valBuf = Buffer.from(value);
        // [key_len][key][val_len][val][ttl]
        const payload = Buffer.alloc(4 + keyBuf.length + 4 + valBuf.length + 8);
        let offset = 0;
        payload.writeUInt32BE(keyBuf.length, offset);
        offset += 4;
        keyBuf.copy(payload, offset);
        offset += keyBuf.length;
        payload.writeUInt32BE(valBuf.length, offset);
        offset += 4;
        valBuf.copy(payload, offset);
        offset += valBuf.length;
        payload.writeBigUInt64BE(BigInt(0), offset); // TTL 0
        await this.sendFrame(OpCode.Set, payload);
        await this.expectOk();
    }
    async get(key) {
        const keyBuf = Buffer.from(key);
        const payload = Buffer.alloc(4 + keyBuf.length);
        payload.writeUInt32BE(keyBuf.length, 0);
        keyBuf.copy(payload, 4);
        await this.sendFrame(OpCode.Get, payload);
        return await this.nextResponse();
    }
    async del(key) {
        const keyBuf = Buffer.from(key);
        const payload = Buffer.alloc(4 + keyBuf.length);
        payload.writeUInt32BE(keyBuf.length, 0);
        keyBuf.copy(payload, 4);
        await this.sendFrame(OpCode.Del, payload);
        const res = await this.nextResponse();
        return res > 0;
    }
    async vadd(key, vector) {
        const keyBuf = Buffer.from(key);
        // [key_len][key][count][f32...]
        const payload = Buffer.alloc(4 + keyBuf.length + 4 + (vector.length * 4));
        let offset = 0;
        payload.writeUInt32BE(keyBuf.length, offset);
        offset += 4;
        keyBuf.copy(payload, offset);
        offset += keyBuf.length;
        payload.writeUInt32BE(vector.length, offset);
        offset += 4;
        for (const f of vector) {
            payload.writeFloatBE(f, offset);
            offset += 4;
        }
        await this.sendFrame(OpCode.VAdd, payload);
        await this.expectOk();
    }
    async vsearch(vector, k) {
        // [count][f32...][k]
        const payload = Buffer.alloc(4 + (vector.length * 4) + 4);
        let offset = 0;
        payload.writeUInt32BE(vector.length, offset);
        offset += 4;
        for (const f of vector) {
            payload.writeFloatBE(f, offset);
            offset += 4;
        }
        payload.writeUInt32BE(k, offset);
        await this.sendFrame(OpCode.VSearch, payload);
        const res = await this.nextResponse();
        if (!Array.isArray(res))
            throw new Error(`Expected array, got ${res}`);
        return res;
    }
    async expectOk() {
        const res = await this.nextResponse();
        if (res !== 'OK')
            throw new Error(`Expected OK, got ${res}`);
    }
    async sendFrame(opcode, payload) {
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
                if (err)
                    reject(err);
                else
                    resolve();
            });
        });
    }
    nextResponse() {
        return new Promise((resolve, reject) => {
            this.pending.push({ resolve, reject });
            this.processResponses();
        });
    }
    processResponses() {
        while (this.pending.length > 0) {
            try {
                const res = this.reader.parse();
                if (res === undefined) {
                    return; // Incomplete
                }
                const p = this.pending.shift();
                if (res instanceof Error) {
                    p === null || p === void 0 ? void 0 : p.reject(res);
                }
                else {
                    p === null || p === void 0 ? void 0 : p.resolve(res);
                }
            }
            catch (err) {
                const p = this.pending.shift();
                p === null || p === void 0 ? void 0 : p.reject(err);
            }
        }
    }
}
exports.Client = Client;
class ResponseReader {
    constructor() {
        this.buffer = Buffer.alloc(0);
    }
    append(data) {
        this.buffer = Buffer.concat([this.buffer, data]);
    }
    parse() {
        if (this.buffer.length < HEADER_SIZE)
            return undefined;
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
                if (payload.length < 8)
                    throw new Error("Invalid integer payload");
                // JS number is double (53-bit int safe), might lose precision for full 64-bit
                // But for client simple use, we return Number or BigInt?
                // Let's use Number for convenience, unless > MAX_SAFE_INTEGER
                const val = payload.readBigInt64BE(0);
                return Number(val);
            }
            case OpCode.Array: {
                // [count: u32][len1: u32][bytes1]...
                if (payload.length < 4)
                    return [];
                let offset = 0;
                const count = payload.readUInt32BE(offset);
                offset += 4;
                const items = [];
                for (let i = 0; i < count; i++) {
                    if (offset + 4 > payload.length)
                        throw new Error("Incomplete array");
                    const itemLen = payload.readUInt32BE(offset);
                    offset += 4;
                    if (offset + itemLen > payload.length)
                        throw new Error("Incomplete array item");
                    items.push(payload.subarray(offset, offset + itemLen).toString());
                    offset += itemLen;
                }
                return items;
            }
            default: throw new Error(`Unknown opcode: ${opcode}`);
        }
    }
}
