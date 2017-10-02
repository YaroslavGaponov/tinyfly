FROM node:boron
WORKDIR /usr/src/tinyfly
COPY . .
EXPOSE 17878
CMD ["node", "--expose-wasm", "tinyfly.js"]