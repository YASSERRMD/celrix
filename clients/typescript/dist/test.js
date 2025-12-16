"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const index_1 = require("./index");
async function main() {
    const client = new index_1.Client('127.0.0.1', 6380);
    try {
        console.log('Connecting...');
        await client.connect();
        console.log('Connected!');
        console.log('Ping...');
        await client.ping();
        console.log('Pong!');
        console.log('Setting key...');
        await client.set('hello_ts', 'world_ts');
        console.log('Getting key...');
        const val = await client.get('hello_ts');
        console.log(`Got: ${val}`);
        if (val !== 'world_ts')
            throw new Error('Value mismatch');
        console.log('Testing Vector operations...');
        const vector = new Array(1536).fill(0.1);
        await client.vadd('v_ts', vector);
        console.log('VAdd success');
        const results = await client.vsearch(vector, 5);
        console.log(`VSearch results: ${results}`);
        if (!results.includes('v_ts'))
            throw new Error('v_ts not found in results');
        console.log('All TypeScript tests passed!');
    }
    catch (err) {
        console.error('Test failed:', err);
        process.exit(1);
    }
    finally {
        client.close();
    }
}
main();
