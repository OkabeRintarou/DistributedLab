package main

import (
	"snapshots/pkg/snapshots"
)

type Node struct {
	snapshots.StgNode
}

func (n *Node) Init() (err error) {
	if err = n.StgNode.Init(); err != nil {
		return
	}

	return nil
}

func (n *Node) Process() error {

	defer n.StgNode.Close()

	return nil
}

func main() {
	var op snapshots.Operator = &Node{}
	snapshots.Run(op)
}
