/*
 tinyfly is an open-source in-memory database project implementing a networked, in-memory key-value store
 Copyright (c) 2017 Yaroslav Gaponov <yaroslav.gaponov@gmail.com>
*/

'use strict';

const assert = require('assert');

const debuggify = (object) => {
    const wrapper = Object.create(object);
    for (let name of Object.getOwnPropertyNames(Object.getPrototypeOf(object))) {
          let method = object[name];
          if (method instanceof Function && name !== 'constructor') {
            wrapper[name] = function() {                
                let result = method.apply(object, arguments);
                console.log(
                    'DEBUG:',
                    object.constructor.name + '.' + name,
                    '(' +
                        Array.prototype.slice.call(arguments)
                            .map(p => {return p instanceof Function ? '<function>' : p;})
                                .toString() +
                    ') => ', result
                );
                return result;
            };
          }
    }
    return wrapper;
};

class BitMap {
    constructor(array) {

        assert(array);
        assert(array instanceof Buffer);
        assert(array.length > 0);

        this._array = array;
    }
    clear() {
        for (let i = 0; i < this._array.length; i++) {
            this._array[i] = 0;
        }
    }
    fetch() {
        for (let base = 0; base < this._array.length; base++) {
            for (let offset = 0; offset < 8; offset++) {
                if (((this._array[base] >> offset) & 1) === 0) {
                    this._array[base] |= 1 << offset;
                    return (base << 3) | offset;
                }
            }
        }
        return -1;
    }
    free(id) {

        assert(!isNaN(id));
        assert(id >= 0);
        assert(id < (this._array.length<<3));

        const base = id >> 3;
        const offset = id & 7;
        this._array[base] &= ~(1 << offset);
    }

}

class BloomFilter {
    constructor(buffer, hfunc, seeds) {
        this._buffer = buffer;
        this._hfuncs = seeds.map(
            seed => {
                return hfunc(seed);
            }
        );
    }
    clear() {
        for(let i=0; i<this._buffer.length; i++) {
            this._buffer[i] = 0;
        }
        return this;
    }
    add(key) {
        this._hfuncs
            .map(hfunc => {
                    return hfunc(key) % this._buffer.length;
                }
            )
            .forEach(id => {
                    const base = id >> 3;
                    const offset = id & 7;
                    this._buffer[base] |= 1 << offset;
                }
            )
        ;
    }
    remove(key) {
        this._hfuncs
            .map(hfunc => {
                    return hfunc(key) % this._buffer.length;
                }
            )
            .forEach(id => {
                    const base = id >> 3;
                    const offset = id & 7;
                    this._buffer[base] &= ~(1 << offset);
                }
            )
        ;        
    }
    has(key) {
        return this._hfuncs
            .map(hfunc => {
                    return hfunc(key) % this._buffer.length;
                }
            )
            .map(id => {
                    const base = id >> 3;
                    const offset = id & 7;
                    return ((this._buffer[base] >> offset) & 1) === 1;
                }
            )
            .filter(Boolean)
            .length === this._hfuncs.length
        ;
    }
}

const BLOCK =  Object.freeze({
    FREE: 0,
    BUSY: 1
});

class Storage {
    constructor(buffer) {

        assert(buffer);
        assert(buffer instanceof Buffer);
        assert(buffer.length > 0);

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
            assert(offset >= 0);
            assert(offset < this._buffer.length);
            const flag = this._buffer.readUInt8(offset);
            assert(flag === BLOCK.FREE || flag === BLOCK.BUSY);
            const size = this._buffer.readUInt32BE(offset + 1);
            assert(size > 0);
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
        assert(offset >= 0);
        assert(offset < this._buffer.length);
        const flag = this._buffer.readUInt8(offset);
        assert(flag === BLOCK.FREE || flag === BLOCK.BUSY);
        if (flag === BLOCK.FREE) {
            return null;
        }
        const size = this._buffer.readUInt32BE(offset + 1);
        assert(size > 0);
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
        const bitmap_length = nodes_length >> 5;
        const bloom_length = length >> 5;
        const htable_length = length - nodes_length - bitmap_length - bloom_length;
        

        this._bitmap = debuggify(new BitMap(buffer.slice(0, bitmap_length)));
        this._bloom = debuggify(new BloomFilter(buffer.slice(bitmap_length, bitmap_length + bloom_length), Index.get_calc_hash_func, [1087, 1697, 2039, 2843, 3041]));
        this._table = new Uint32Array(buffer.slice(bitmap_length + bloom_length, bitmap_length + bloom_length + htable_length));
        this._nodes = new Uint32Array(buffer.slice(bitmap_length + bloom_length + htable_length));
    }
    static get_calc_hash_func(seed) {
        return (s) => {
            let hash = seed;
            if (s.length === 0) {
                return hash;
            }
            for (let i = 0; i < s.length; i++) {
                let code = s.charCodeAt(i);
                hash = ((hash << 5) - hash) + code;
                hash = hash & hash;
            }
            return hash >>> 0;            
        };
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
        this._bitmap.clear();
        this._bloom.clear();
        for (let i = 0; i < this._table.length; i++) {
            this._table[i] = EOC;
        }
        return this;
    }
    get(key, check) {
        assert(key);
        if (!this._bloom.has(key)) {
            return -1;
        }

        const hash = Index.calc_hash(key);
        const index = hash % this._table.length;

        let curr_offset = this._table[index];
        for (;;) {
            assert(curr_offset >= 0);
            assert(curr_offset < this._table.length);
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
        assert(key);

        if (!this._bloom.has(key)) {
            return -1;
        }

        return this.get(key, check) !== -1;
    }
    set(id, key, check) {

        assert(!isNaN(id));
        assert(id >= 0);
        assert(key);

        const hash = Index.calc_hash(key);
        const index = hash % this._table.length;

        let pred_offset = EOC;
        let curr_offset = this._table[index];
        
        for (;;) {
            if (curr_offset === EOC) {
                let new_offset = this._bitmap.fetch();
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
                this._bloom.add(key);
                return true;
            }

            let _addr = Index.getNodeBlockOffset(curr_offset);
            const curr_hash = this._nodes[_addr];
            const curr_id = this._nodes[_addr + 1];
            const curr_next = this._nodes[_addr + 2];

            if (hash === curr_hash && check(curr_id)) {
                return false;
            }

            if (hash > curr_hash) {
                let new_offset = this._bitmap.fetch();
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
                this._bloom.add(key);
                return true;
            }

            pred_offset = curr_offset;
            curr_offset = curr_next;
        }

    }
    delete(key, check) {
        assert(key);

        if (!this._bloom.has(key)) {
            return -1;
        }

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
                this._bitmap.free(curr_offset);
                this._bloom.remove(key);
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
        assert(index && index instanceof Index);
        assert(storage && storage instanceof Storage);

        this._index = index;
        this._storage = storage;
    }
    has(key) {
        assert(key);
        return this._index.has(key,
            (id) => {
                return this._storage.getKey(id) === key;
            }
        );
    }
    set(key, value) {
        assert(key);
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
        assert(key);
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
        assert(key);
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
const nosql = debuggify(
    new NoSql(
        debuggify(new Index(space.slice(0, 0xffff)).clear()),
        debuggify(new Storage(space.slice(0xffff)).clear())
    )
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
            const key = path.slice(1).split('?')[0];
            switch(method) {
                case 'HEAD': 
                    return reply(nosql.has(key) ? 200 : 404);
                
                case 'GET': 
                    const value = nosql.get(key);
                    return reply(value ? 200 : 404, value);

                case 'PUT':
                    nosql.delete(key);
                    return reply(nosql.set(key, body) ? 200 : 500);

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

