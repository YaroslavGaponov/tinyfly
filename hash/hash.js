'use strict';

const hash = WebAssembly ?
    (seed = 0) => {
        return (str) => {
            return new Promise(
                resolve => {
                    WebAssembly
                       .compile(require('fs').readFileSync(__dirname + '/hash.wasm'))
                       .then(module => {
                           return new WebAssembly.Instance(module, {});
                       })
                       .then(instance => {
                            const buf = new Uint8Array(instance.exports.memory.buffer);
                            for(let i=0; i<str.length ;i++) {
                                buf[i] = str.charCodeAt(i);
                            }
                            return resolve(instance.exports.hash(seed));
                       });
                }
            );
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