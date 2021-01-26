package snapshots

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"net/rpc"

	"github.com/syndtr/goleveldb/leveldb"
)

func Run(o Operator) (err error) {
	if err = o.Init(); err != nil {
		// TODO: add log
		panic(err)
	}
	return o.Process()
}

type NodeConfig struct {
	Downstreams   []string `json:"down_streams"`
	ListenAddress string   `json:"listen_address"`
	LevelDBPath   string   `json:"leveldb_path"`
}

type KV struct {
	Key, Value []byte
}

type Message struct {
	KVs []KV
}

type HandlerRouter struct {
}

func (r *HandlerRouter) Do(req *Message, resp *int) error {
	println("do")
	return nil
}

type StgNode struct {
	DownClients []*rpc.Client
	router      *HandlerRouter
	db          *leveldb.DB
}

func (n *StgNode) Router() *HandlerRouter {
	return n.router
}

func (n *StgNode) Init() error {
	confData, err := ioutil.ReadFile("config.json")
	if err != nil {
		return err
	}
	var conf NodeConfig
	if err = json.Unmarshal(confData, &conf); err != nil {
		return err
	}

	n.DownClients = make([]*rpc.Client, 0, len(conf.Downstreams))
	for _, stream := range conf.Downstreams {
		client, err := rpc.DialHTTP("tcp", stream)
		if err != nil {
			return err
		}
		n.DownClients = append(n.DownClients, client)
	}

	if len(conf.ListenAddress) > 0 {
		n.router = new(HandlerRouter)
		if err = rpc.Register(n.router); err != nil {
			return err
		}
		rpc.HandleHTTP()

		l, err := net.Listen("tcp", conf.ListenAddress)
		if err != nil {
			return err
		}
		go http.Serve(l, nil)
	}

	if len(conf.LevelDBPath) == 0 {
		return errors.New("Invalid empty leveldb path")
	}
	db, err := leveldb.OpenFile(conf.LevelDBPath, nil)
	if err != nil {
		return err
	}
	n.db = db

	return nil
}

func (n *StgNode) Close() {
	if n.db != nil {
		n.db.Close()
	}
}
