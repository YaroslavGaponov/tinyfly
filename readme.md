Tinyfly
========

## Owerview
tinyfly is an open-source in-memory database project implementing a networked, in-memory key-value store

### Run

##### 1 
```output
git clone https://github.com/YaroslavGaponov/tinyfly
cd tinyfly
PORT=17878  node ./tinyfly.js 
```

##### 2
```output
curl -sS https://raw.githubusercontent.com/YaroslavGaponov/tinyfly/master/tinyfly.js | PORT=17878 node
```

#### 3
````output

````

## Rest Api

```output
curl -XPOST http://localhost:17878/key1 -d 'hello1'
curl -XGET http://localhost:17878/key1
curl -XHEAD http://localhost:17878/key1
curl -XDELETE http://localhost:17878/key1
```

