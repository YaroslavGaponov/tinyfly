Tinyfly
========

## Owerview
tinyfly is an open-source in-memory database project implementing a networked, in-memory key-value store

### Run

##### 1 From Github
```output
git clone https://github.com/YaroslavGaponov/tinyfly
cd tinyfly
PORT=17878 node ./tinyfly.js 
```

##### 2 Simple
```output
curl -sS https://raw.githubusercontent.com/YaroslavGaponov/tinyfly/master/tinyfly.js | PORT=17878 node
```

#### 3 Docker
````output
git clone https://github.com/YaroslavGaponov/tinyfly
cd tinyfly
docker build -t tinyfly .
docker run -p 17878:17878 -d tinyfly
````

## Rest Api

```output
curl -XPOST http://localhost:17878/key1 -d 'hello1'
curl -XGET http://localhost:17878/key1
curl -XHEAD http://localhost:17878/key1
curl -XDELETE http://localhost:17878/key1
```

