/*export { toBufferKey as keyValueToBuffer, compareKeys, compareKeys as compareKey, fromBufferKey as bufferToKeyValue } */
import { setExternals } from './external.js';
import * as orderedBinary from 'https://deno.land/x/orderedbinary@v1.2.2/index.js';
import { Encoder as MsgpackrEncoder } from 'https://deno.land/x/msgpackr@v1.5.0/index.js';
import { WeakLRUCache } from 'https://deno.land/x/weakcache@v1.1.3/index.js';
function arch() {
    return Deno.build.arch;
}
import * as path from 'node:path';
export { fileURLToPath } from 'node:url';
import { EventEmitter } from 'node:events'
setExternals({ orderedBinary, MsgpackrEncoder, WeakLRUCache, arch, path, EventEmitter, fs: Deno });