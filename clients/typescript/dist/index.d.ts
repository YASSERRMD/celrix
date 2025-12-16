export declare enum OpCode {
    Ping = 1,
    Pong = 2,
    Get = 3,
    Set = 4,
    Del = 5,
    Exists = 6,
    Ok = 16,
    Error = 17,
    Value = 18,
    Nil = 19,
    Integer = 20,
    Array = 21,
    VAdd = 32,
    VSearch = 33
}
export declare class Client {
    private host;
    private port;
    private socket;
    private reader;
    private pending;
    private nextReqId;
    constructor(host?: string, port?: number);
    connect(): Promise<void>;
    close(): void;
    ping(): Promise<void>;
    set(key: string, value: string): Promise<void>;
    get(key: string): Promise<string | null>;
    del(key: string): Promise<boolean>;
    vadd(key: string, vector: number[]): Promise<void>;
    vsearch(vector: number[], k: number): Promise<string[]>;
    private expectOk;
    private sendFrame;
    private nextResponse;
    private processResponses;
}
