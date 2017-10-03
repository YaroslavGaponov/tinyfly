'use strict';

const fs = require('fs');

const hash = 'WebAssembly' in global ?
    () => {        
        return new Promise(
            (resolve, reject) => {
                fs.readFile(__dirname + '/hash.wasm', (err, data) => {
                    if (err) {
                        return reject(err);
                    } else {
                        return resolve(data);
                    }
                });
            })
            .then(WebAssembly.compile)
            .then(module => { return new WebAssembly.Instance(module, {}); })
            .then(instance => {
                 return (seed = 0) => {
                     return (str) => {                    
                         const buf = new Uint8Array(instance.exports.memory.buffer);
                         for(let i=0; i<str.length ;i++) {
                             buf[i] = str.charCodeAt(i);
                         }                    
                         return instance.exports.hash(seed) >>> 0;
                     };
                 };
            });
    }   :
    () => {
        return new Promise(() => {
            return (seed = 0) => {
                return (str) => {
                    let hash = seed;
                    if (str.length === 0) {
                        return hash;
                    }
                    for (let i = 0; i < str.length; i++) {
                        const code = str.charCodeAt(i);
                        hash = ((hash << 5) - hash) + code;
                        hash = hash & hash;
                    }
                    return hash >>> 0;
                };
            };
        });
    }
;

module.exports.load = () => {
    return hash();
}
