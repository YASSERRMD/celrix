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
exports.Client = void 0;
const net = __importStar(require("net"));
class Client {
    constructor(host = '127.0.0.1', port = 6380) {
        this.host = host;
        this.port = port;
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
    async set(key, value) {
        await this.sendCommand('SET', key, value);
        const res = await this.nextResponse();
        if (res !== 'OK') {
            throw new Error(`Expected OK, got ${res}`);
        }
    }
    async get(key) {
        await this.sendCommand('GET', key);
        return await this.nextResponse();
    }
    async del(key) {
        await this.sendCommand('DEL', key);
        const res = await this.nextResponse();
        return res > 0;
    }
    async sendCommand(cmd, ...args) {
        let msg = `*${args.length + 1}\r\n$${cmd.length}\r\n${cmd}\r\n`;
        for (const arg of args) {
            msg += `$${arg.length}\r\n${arg}\r\n`;
        }
        return new Promise((resolve, reject) => {
            this.socket.write(msg, (err) => {
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
        if (this.buffer.length === 0)
            return undefined;
        const type = this.buffer[0];
        const crlfIndex = this.buffer.indexOf('\r\n');
        if (crlfIndex === -1)
            return undefined;
        const line = this.buffer.subarray(1, crlfIndex).toString();
        if (type === 43) { // +
            this.consume(crlfIndex + 2);
            return line;
        }
        else if (type === 45) { // -
            this.consume(crlfIndex + 2);
            return new Error(line);
        }
        else if (type === 58) { // :
            this.consume(crlfIndex + 2);
            return parseInt(line, 10);
        }
        else if (type === 36) { // $
            const len = parseInt(line, 10);
            this.consume(crlfIndex + 2);
            if (len === -1)
                return null;
            if (this.buffer.length < len + 2) {
                // Not enough data, backtrack need careful handling or 
                // just reconstruct/don't consume headers if payload missing?
                // Simpler: Reset state? No, can't easily undo consume.
                // Correct logic must check total length before consuming header 
                // OR keep separate offset. 
                // Let's implement full check-first logic for simplicity here.
                // Rollback:
                this.buffer = Buffer.concat([Buffer.from(`$${line}\r\n`), this.buffer]);
                return undefined;
            }
            const data = this.buffer.subarray(0, len).toString();
            this.consume(len + 2);
            return data;
        }
        else if (type === 42) { // *
            // Arrays complex to parse in this simple sync blocking reader without state machine
            // For basic Client MVP, support primarily simple responses or flat arrays
            return undefined; // TODO: Array support
        }
        return undefined;
    }
    consume(n) {
        this.buffer = this.buffer.subarray(n);
    }
}
