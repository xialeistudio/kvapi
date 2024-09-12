# kvapi

A simple key-value store API to learn about Raft consensus algorithm.

## Get Started

### Build
```bash
cd kvapi && go build
rm -rf *.dat
```
### Launch cluster
**Terminal 1**
```bash
./kvapi --node 0 --http :2020 --cluster "0,:3030;1,:3031;2,:3032"
```

**Terminal 2**
```bash
./kvapi --node 1 --http :2021 --cluster "0,:3030;1,:3031;2,:3032"
```

**Terminal 3**
```bash
./kvapi --node 2 --http :2022 --cluster "0,:3030;1,:3031;2,:3032"
```

### Set key-value
```bash
curl http://localhost:2020/set?key=y&value=hello
```

### Get key-value
```bash
curl http://localhost:2020/get\?key\=y
```