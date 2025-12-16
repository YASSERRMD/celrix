export declare class Client {
    private host;
    private port;
    private socket;
    private reader;
    private pending;
    constructor(host?: string, port?: number);
    connect(): Promise<void>;
    close(): void;
    set(key: string, value: string): Promise<void>;
    get(key: string): Promise<string | null>;
    del(key: string): Promise<boolean>;
    private sendCommand;
    private nextResponse;
    private processResponses;
}
