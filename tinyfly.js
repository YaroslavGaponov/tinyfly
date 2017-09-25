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
    getKey(offset) {
        const flag = this._buffer.readUInt8(offset);
        if (flag === BLOCK.FREE) {
            return null;
        }
        const size = this._buffer.readUInt32BE(offset + 1);
        const pair = this._buffer.slice(offset + 5, offset + 5 + size).toString().split('\0');
        return pair[0];
    }
    getValue(offset) {
        const flag = this._buffer.readUInt8(offset);
        if (flag === BLOCK.FREE) {
            return null;
        }
        const size = this._buffer.readUInt32BE(offset + 1);
        const pair = this._buffer.slice(offset + 5, offset + 5 + size).toString().split('\0');
        return pair[1];        
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
    static getNodeBlockOffset(index) {
        return index + (index<<1);
    }
    clear() {
        this._bitset.clear();
        for (let i = 0; i < this._table.length; i++) {
            this._table[i] = EOC;
        }
        return this;
    }
    get(key, check) {
        const hash = Index.calc_hash(key);
        const index = hash % this._table.length;

        let curr_offset = this._table[index];
        for (;;) {
            if (curr_offset === EOC) {
                return -1;
            }
            let _addr = Index.getNodeBlockOffset(curr_offset);
            const curr_hash = this._nodes[_addr];
            const curr_id = this._nodes[_addr + 1];
            if (hash === curr_hash && check(curr_id)) {
                return curr_id;
            } else if (hash > curr_hash) {
                return -1;
            } else {
                curr_offset =  this._nodes[_addr + 2];
            }
        }        
    }
    has(key, check) {
        return this.get(key, check) !== -1;
    }
    set(id, key, check) {
        const hash = Index.calc_hash(key);
        const index = hash % this._table.length;

        let pred_offset = EOC;
        let curr_offset = this._table[index];
        for (;;) {
            let _addr = Index.getNodeBlockOffset(curr_offset);
            const curr_hash = this._nodes[_addr];
            const curr_id = this._nodes[_addr + 1];
            const curr_next = this._nodes[_addr + 2];
            if (hash === curr_hash && check(curr_id)) {
                return false;            
            } else if (curr_offset === EOC || hash > curr_hash) {
                const new_offset = this._bitset.fetch();
                let _addr = Index.getNodeBlockOffset(new_offset); 
                this._nodes[_addr] = hash;
                this._nodes[_addr + 1] = id;
                this._nodes[_addr + 2] = EOC;
                if (pred_offset === EOC) {
                    this._table[index] = new_offset;
                } else {
                    let _addr = Index.getNodeBlockOffset(pred_offset);
                    this._nodes[_addr + 2] = new_offset;
                }
                return true;
            } else {
                pred_offset = curr_offset;
                curr_offset = curr_next;
            }
        }

    }
    delete(key, check) {        
        const hash = Index.calc_hash(key);
        const index = hash % this._table.length;
    
        let pred_offset = EOC;
        let curr_offset = this._table[index];
        for (;;) {            
            if (curr_offset === EOC) {
                return -1;
            }
            let _addr = Index.getNodeBlockOffset(curr_offset);
            const curr_hash = this._nodes[_addr];
            const curr_id = this._nodes[_addr + 1];
            const curr_next = this._nodes[_addr + 2];
            if (hash === curr_hash && check(curr_id)) {
                if (pred_offset === EOC) {
                    this._table[index] = curr_next;
                } else {
                    let _addr = Index.geNodeBlockOffset(pred_offset);
                    this._nodes[_addr + 2] = curr_next;
                }
                return curr_id;
            } else if (hash > curr_hash) {
                return -1;
            } else {
                pred_offset = curr_offset;
                curr_offset = curr_next;
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
        return this._index.has(key,
            (id) => {
                return this._storage.getKey(id) === key;
            }
        );
    }
    set(key, value) {
        const id = this._storage.save(key, value);
        if (id === -1) {
            return false;
        }
        return this._index.set(id, key,
            (id) => {
                return this._storage.getKey(id) === key;
            }
        );
    }
    get(key) {
        const id = this._index.get(key,
            (id) => {
                return this._storage.getKey(id) === key;
            }
        );
        if (id === -1) {
            return;
        }
        return this._storage.getValue(id);
    }
    delete(key) {
        const id = this._index.delete(key,
            (id) => {
                return this._storage.getKey(id) === key;
            }
        );
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
const PROTOCOL = 'HTTP/1.1';
const PORT = process.env.PORT || 17878;
const LN = '\r\n';

net.createServer(
    (socket) => {
        const reply = (code, body) => {
            socket.end(PROTOCOL + ' ' + code + LN + LN + (body || ''));
        };
        socket.on('data', (chunk) => {
            const [header, body] = chunk.toString().split(LN + LN);
            const [method, path] = header.split(LN)[0].split(' ');            
            const key = path.startsWith('/') ? path.slice(1) : path;
            switch(method) {
                case 'HEAD': 
                    return reply(nosql.has(key) ? 200 : 404);
                
                case 'GET': 
                    const value = nosql.get(key);
                    return reply(value ? 200 : 404, value);

                case 'PUT':
                    nosql.delete(key);
                case 'POST':
                    return reply(nosql.set(key, body) ? 200 : 500);

                case 'DELETE': 
                    return reply(nosql.delete(key) ? 200 : 404);                
            }
            return reply(501);
        });
    })
    .listen(PORT, () => {
        console.log('tinyfly is opened server on', PORT);
    })
;

