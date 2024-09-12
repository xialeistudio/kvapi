package main

import (
	"bytes"
	crypto "crypto/rand"
	"encoding/binary"
	"fmt"
	"kvapi/pkg/raft"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

type stateMachine struct {
	db     *sync.Map
	server int
}

type commandKind uint8

const (
	SetCommand commandKind = iota
	GetCommand
)

type command struct {
	kind  commandKind
	key   string
	value string
}

func (s *stateMachine) Apply(cmd []byte) ([]byte, error) {
	c := decodeCommand(cmd)
	switch c.kind {
	case SetCommand:
		s.db.Store(c.key, c.value)
	case GetCommand:
		value, ok := s.db.Load(c.key)
		if !ok {
			return nil, fmt.Errorf("key not found")
		}
		return []byte(value.(string)), nil
	default:
		return nil, fmt.Errorf("unknow command: %x", cmd)
	}
	return nil, nil
}

func encodeCommand(c command) []byte {
	msg := bytes.NewBuffer(nil)
	err := msg.WriteByte(uint8(c.kind))
	if err != nil {
		panic(err)
	}
	err = binary.Write(msg, binary.LittleEndian, uint64(len(c.key)))
	if err != nil {
		panic(err)
	}
	msg.WriteString(c.key)

	err = binary.Write(msg, binary.LittleEndian, uint64(len(c.value)))
	if err != nil {
		panic(err)
	}
	msg.WriteString(c.value)
	return msg.Bytes()
}

func decodeCommand(cmd []byte) command {
	var c command
	c.kind = commandKind(cmd[0])
	keyLen := binary.LittleEndian.Uint64(cmd[1:9])
	c.key = string(cmd[9 : 9+keyLen])
	if c.kind == SetCommand {
		valueLen := binary.LittleEndian.Uint64(cmd[9+keyLen : 17+keyLen])
		c.value = string(cmd[17+keyLen : 17+keyLen+valueLen])
	}
	return c
}

type httpServer struct {
	raft *raft.Server
	db   *sync.Map
}

func (hs httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	var c command
	c.kind = SetCommand
	c.key = r.URL.Query().Get("key")
	c.value = r.URL.Query().Get("value")

	_, err := hs.raft.Apply([][]byte{encodeCommand(c)})
	if err != nil {
		log.Printf("could not write key-value: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
}

func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	var c command
	c.kind = GetCommand
	c.key = r.URL.Query().Get("key")

	var (
		value []byte
		err   error
	)
	if r.URL.Query().Get("relaxed") == "true" {
		v, ok := hs.db.Load(c.key)
		if !ok {
			err = fmt.Errorf("key not found")
		} else {
			value = []byte(v.(string))
		}
	} else {
		var results []raft.ApplyResult
		results, err = hs.raft.Apply([][]byte{encodeCommand(c)})
		if err == nil {
			if len(results) != 1 {
				err = fmt.Errorf("expected 1 result, got %d", len(results))
			} else if results[0].Error != nil {
				err = results[0].Error
			} else {
				value = results[0].Result
			}
		}
	}
	if err != nil {
		log.Printf("could not read key-value: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	written := 0
	for written < len(value) {
		n, err := w.Write(value[written:])
		if err != nil {
			log.Printf("could not write key-value: %s", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		written += n
	}
}

type config struct {
	cluster []raft.ClusterMember
	index   int
	id      string
	address string
	http    string
}

func getConfig() config {
	var (
		cfg  config
		node string
	)
	for i, arg := range os.Args[1:] {
		if arg == "--node" {
			var err error
			node = os.Args[i+2]
			cfg.index, err = strconv.Atoi(node)
			if err != nil {
				log.Fatal("Expected $value to be a valid integer in `--node $value`", node)
			}
			i++
			continue
		}
		if arg == "--http" {
			cfg.http = os.Args[i+2]
			i++
			continue
		}
		if arg == "--cluster" {
			cluster := os.Args[i+2]
			var clusterEntry raft.ClusterMember
			for _, part := range strings.Split(cluster, ";") {
				ipAddress := strings.Split(part, ",")
				var err error
				clusterEntry.Id, err = strconv.ParseUint(ipAddress[0], 10, 64)
				if err != nil {
					log.Fatal("Expected $value to be a valid integer in `--cluster $value`", ipAddress[0])
				}
				clusterEntry.Address = ipAddress[1]
				cfg.cluster = append(cfg.cluster, clusterEntry)
			}
			i++
			continue
		}
	}
	if node == "" {
		log.Fatal("Missing required parameter: --node $index")
	}

	if cfg.http == "" {
		log.Fatal("Missing required parameter: --http $address")
	}

	if len(cfg.cluster) == 0 {
		log.Fatal("Missing required parameter: --cluster $node1Id,$node1Address;...;$nodeNId,$nodeNAddress")
	}

	return cfg
}

func main() {
	var b [8]byte
	_, err := crypto.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))
	cfg := getConfig()
	var db sync.Map
	var sm stateMachine
	sm.db = &db
	sm.server = cfg.index

	s := raft.NewServer(cfg.cluster, &sm, ".", cfg.index, true)
	go s.Start()

	hs := httpServer{raft: s, db: &db}
	http.HandleFunc("/set", hs.setHandler)
	http.HandleFunc("/get", hs.getHandler)
	log.Fatal(http.ListenAndServe(cfg.http, nil))
}
