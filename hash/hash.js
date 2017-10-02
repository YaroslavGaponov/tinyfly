'use strict';

const hash = 'WebAssembly' in global ?
    (seed = 0) => {
        const bytes = require('fs').readFileSync(__dirname + '/hash.wasm');
        return (str) => {
            return WebAssembly
               .compile(bytes)
               .then(module => {
                   return new WebAssembly.Instance(module, {});
               })
               .then(instance => {
                    const buf = new Uint8Array(instance.exports.memory.buffer);
                    for(let i=0; i<str.length ;i++) {
                        buf[i] = str.charCodeAt(i);
                    }
                    return instance.exports.hash(seed);
               });
        };
    }   :
    (seed = 0) => {
        return (str) => {
            return new Promise(
                resolve => {
                    let hash = seed;
                    if (str.length === 0) {
                        return hash;
                    }
                    for (let i = 0; i < str.length; i++) {
                        const code = str.charCodeAt(i);
                        hash = ((hash << 5) - hash) + code;
                        hash = hash & hash;
                    }
                    return resolve(hash >>> 0);
                }
            );
        };
    }
;

module.exports = hash;
