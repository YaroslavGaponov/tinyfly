/*
 tinyfly is an open-source in-memory database project implementing a networked, in-memory key-value store
 Copyright (c) 2017 Yaroslav Gaponov <yaroslav.gaponov@gmail.com>
*/

'use strict';

class BitSet {
    constructor(array) {
        this._array = array;
    }
    clear() {
        for (let i = 0; i < this._array.length; i++) {
            this._array[i] = 0;
        }
    }
    fetch() {
        for (let base = 0; base < this._array.length; base++) {
            for (let offset = 0; offset < 32; offset++) {
                if (((this._array[base] >> offset) & 1) === 0) {
                    this._array[base] |= 1 << offset;
                    return (base << 5) + offset;
                }
            }
        }
        return -1;
    }
    free(id) {
        const base = id >> 5;
        const offset = id & 0x1f;
        this._array[base] &= ~(1 << offset);
    }

}

const BLOCK =  Object.freeze({
    FREE: 0,
    BUSY: 1
});

class Storage {
    constructor(buffer) {
        this._buffer = buffer;
    }
    clear() {
        this._buffer.writeUInt8(BLOCK.FREE, 0);
        this._buffer.writeInt32BE(this._buffer.length, 1);
        return this;
    }
    save(key, value) {
        const data = new Buffer(key + '\0' + value);
        let offset = 0;
        for (;;) {
            const flag = this._buffer.readUInt8(offset);
            const size = this._buffer.readUInt32BE(offset + 1);
            if (flag === BLOCK.FREE && size >= data.length) {
                this._buffer.writeUInt8(BLOCK.BUSY, offset);
                this._buffer.writeInt32BE(data.length, offset + 1);
                data.copy(this._buffer, offset + 5);
                const other_size = size - data.length - 5;
                if (other_size > 0) {
                    this._buffer.writeUInt8(BLOCK.FREE, offset + 5 + data.length);
                    this._buffer.writeInt32BE(other_size, offset + 5 + data.length + 1);
                    return offset;
                }
            }
            offset += size + 5;
        }
        return -1;
    }
    load(offset) {
        const flag = this._buffer.readUInt8(offset);
        if (flag === BLOCK.FREE) {
            return null;
        }
        const size = this._buffer.readUInt32BE(offset + 1);
        const pair = this._buffer.slice(offset + 5, offset + 5 + size).toString().split('\0');
        return {
            key: pair[0],
            value: pair[1]
        };

    }
    delete(offset) {
        const flag = this._buffer.readUInt8(offset);
        if (flag === BLOCK.FREE) {
            return false;
        }        
        this._buffer.writeUInt8(BLOCK.FREE, offset);
        return true;
    }
}


const EOC = 0xffffffff;

class Index {
    constructor(buffer) {
        const length = buffer.length >> 3;
        const nodes_length = (length >> 1) + (length >> 2); // 75%
        const bitset_length = nodes_length >> 5;
        const htable_length = length - nodes_length - bitset_length;

        this._bitset = new BitSet(new Uint32Array(buffer.slice(0, bitset_length)));
        this._table = new Uint32Array(buffer.slice(bitset_length, bitset_length + htable_length));
        this._nodes = new Uint32Array(buffer.slice(bitset_length + htable_length));
    }
    static calc_hash(s) {
        let hash = 0;
        if (s.length === 0) {
            return hash;
        }
        for (let i = 0; i < s.length; i++) {
            let code = s.charCodeAt(i);
            hash = ((hash << 5) - hash) + code;
            hash = hash & hash;
        }
        return hash >>> 0;
    }
    clear() {
        this._bitset.clear();
        for (let i = 0; i < this._table.length; i++) {
            this._table[i] = EOC;
        }
        return this;
    }
    has(key) {
        const hash = Index.calc_hash(key);
        const index = hash % this._table.length;
        let offset = this._table[index];
        for (;;) {
            if (offset === EOC) {
                return false;
            }
            const curr_hash = this._nodes[offset];
            const curr_next = this._nodes[offset + 2];
            if (hash === curr_hash) {
                return true;
            } else if (hash > curr_hash) {
                return false;
            } else {
                offset = curr_next;
            }
        }
    }
    set(id, key) {
        const hash = Index.calc_hash(key);
        const index = hash % this._table.length;
        let pred_offset = EOC;
        let offset = this._table[index];
        for (;;) {
            if (offset === EOC) {
                let idx = this._bitset.fetch();
                idx = idx + idx << 1;
                this._nodes[idx] = hash;
                this._nodes[idx + 1] = id;
                this._nodes[idx + 2] = offset;
                if (pred_offset === EOC) {
                    this._table[index] = idx;
                } else {
                    this._nodes[pred_offset + 2] = idx;
                }
                return true;
            }
            const curr_hash = this._nodes[offset];
            const curr_next = this._nodes[offset + 2];
            if (hash === curr_hash) {
                this._nodes[offset + 1] = id;
                return true;
            } else if (hash > curr_hash) {
                let idx = this._bitset.fetch();
                idx = idx + idx << 1;
                this._nodes[idx] = hash;
                this._nodes[idx + 1] = id;
                this._nodes[idx + 2] = offset;
                if (pred_offset === EOC) {
                    this._table[index] = idx;
                } else {
                    this._nodes[pred_offset + 2] = idx;
                }
                return true;
            } else if (hash < curr_hash) {
                pred_offset = offset;
                offset = curr_next;
            }
        }

    }
    get(key) {
        const hash = Index.calc_hash(key);
        const index = hash % this._table.length;
        let offset = this._table[index];
        for (;;) {
            if (offset === EOC) {
                return -1;
            }
            const curr_hash = this._nodes[offset];
            const curr_id = this._nodes[offset + 1];
            const curr_next = this._nodes[offset + 2];
            if (hash === curr_hash) {
                return curr_id;
            } else if (hash > curr_hash) {
                return -1;
            } else {
                offset = curr_next;
            }
        }
    }
    delete(key) {
        const hash = Index.calc_hash(key);
        const index = hash % this._table.length;
        let pred_offset = EOC;
        let offset = this._table[index];
        for (;;) {
            if (offset === EOC) {
                return -1;
            }
            const curr_hash = this._nodes[offset];
            const curr_id = this._nodes[offset + 1];
            const curr_next = this._nodes[offset + 2];
            if (hash === curr_hash) {
                if (pred_offset === EOC) {
                    this._table[index] = curr_next;
                } else {
                    this._nodes[pred_offset + 2] = curr_next;
                }
                return curr_id;
            } else if (hash > curr_hash) {
                return -1;
            } else {
                pred_offset = offset;
                offset = curr_next;
            }
        }
    }
}

class NoSql {
    constructor(index, storage) {
        this._index = index;
        this._storage = storage;
    }
    has(key) {
        return this._index.has(key);
    }
    set(key, value) {
        const id = this._storage.save(key, value);
        if (id === -1) {
            return false;
        }
        return this._index.set(id, key);
    }
    get(key) {
        const id = this._index.get(key);
        if (id === -1) {
            return;
        }
        return this._storage.load(id);
    }
    delete(key) {
        const id = this._index.delete(key);
        if (id === -1) {
            return false;
        }
        return this._storage.delete(id);
    }

}

const space = Buffer.alloc(0xffffff);
const nosql = new NoSql(
    new Index(space.slice(0, 0xffff)).clear(),
    new Storage(space.slice(0xffff)).clear()
);

const net = require('net');
const HTTP_CODES = require('http').STATUS_CODES;
const PROTOCOL = 'HTTP/1.1';
const PORT = process.env.PORT || 17878;

net.createServer(
    (socket) => {
        const reply = (code, body) => {
            socket.end(PROTOCOL + ' ' + code +  ' ' + HTTP_CODES[code] + ' \r\n\r\n' + (body ? body : ''));
        };
        socket.on('data', (chunk) => {
            const [header, body] = chunk.toString().split('\r\n\r\n');
            const [method, path] = header.split('\r\n')[0].split(' ');
            const key = path.slice(1);
            switch(method) {
                case 'HEAD': 
                    return reply(nosql.has(key) ? 200 : 404);
                
                case 'GET': 
                    const pair = nosql.get(key);
                    return pair ? 
                        reply(200, pair.value) :
                        reply(404)
                    ;
                case 'PUT':
                    nosql.delete(key);
                    return reply(nosql.set(key, body) ? 200 : 500);

                case 'POST':
                    return reply(nosql.set(key, body) ? 200 : 500);

                case 'DELETE': 
                    return reply(nosql.delete(key, body) ? 200 : 404);                
            }
            return reply(501);
        });
    })
    .listen(PORT, () => {
        console.log('tinyfly is opened server on', PORT);
    })
;

