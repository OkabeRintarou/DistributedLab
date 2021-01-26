package main

import (
	"fmt"
	"snapshots/pkg/snapshots"
	"time"
)

type Node struct {
	snapshots.StgNode
}

func (n *Node) Init() (err error) {
	if err = n.StgNode.Init(); err != nil {
		return
	}

	if len(n.DownClients) != 2 {
		return fmt.Errorf("expect 2 down clients, actual: %d", len(n.DownClients))
	}
	return nil
}

func (n *Node) Process() error {

	defer n.StgNode.Close()

	println("child process")
	time.Sleep(2000 * time.Second)
	return nil
}

func main() {
	var op snapshots.Operator = &Node{}
	snapshots.Run(op)
}
