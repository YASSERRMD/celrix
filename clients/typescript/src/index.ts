import * as net from 'net';

export class Client {
    private socket: net.Socket;
    private reader: ResponseReader;
    private pending: Array<{ resolve: (val: any) => void, reject: (err: Error) => void }>;

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

    async set(key: string, value: string): Promise<void> {
        await this.sendCommand('SET', key, value);
        const res = await this.nextResponse();
        if (res !== 'OK') {
            throw new Error(`Expected OK, got ${res}`);
        }
    }

    async get(key: string): Promise<string | null> {
        await this.sendCommand('GET', key);
        return await this.nextResponse();
    }

    async del(key: string): Promise<boolean> {
        await this.sendCommand('DEL', key);
        const res = await this.nextResponse();
        return (res as number) > 0;
    }

    private async sendCommand(cmd: string, ...args: string[]): Promise<void> {
        let msg = `*${args.length + 1}\r\n$${cmd.length}\r\n${cmd}\r\n`;
        for (const arg of args) {
            msg += `$${arg.length}\r\n${arg}\r\n`;
        }

        return new Promise((resolve, reject) => {
            this.socket.write(msg, (err) => {
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
        if (this.buffer.length === 0) return undefined;

        const type = this.buffer[0];
        const crlfIndex = this.buffer.indexOf('\r\n');
        if (crlfIndex === -1) return undefined;

        const line = this.buffer.subarray(1, crlfIndex).toString();

        if (type === 43) { // +
            this.consume(crlfIndex + 2);
            return line;
        } else if (type === 45) { // -
            this.consume(crlfIndex + 2);
            return new Error(line);
        } else if (type === 58) { // :
            this.consume(crlfIndex + 2);
            return parseInt(line, 10);
        } else if (type === 36) { // $
            const len = parseInt(line, 10);
            this.consume(crlfIndex + 2);
            if (len === -1) return null;

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
        } else if (type === 42) { // *
            // Arrays complex to parse in this simple sync blocking reader without state machine
            // For basic Client MVP, support primarily simple responses or flat arrays
            return undefined; // TODO: Array support
        }

        return undefined;
    }

    private consume(n: number): void {
        this.buffer = this.buffer.subarray(n);
    }
}
