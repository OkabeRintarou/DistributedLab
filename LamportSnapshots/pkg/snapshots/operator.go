package snapshots

type Operator interface {
	Init() error
	Process() error
}
