package wsworker

import (
	"sync"
)

type worker struct {
	id      string
	offsets []int64
}

type Committer interface {
	Commit(term uint64, partition int32, offset int64) error
}

type OffsetMgr struct {
	*sync.Mutex
	workers    map[string]*worker
	CommitChan chan wsworker.Commit
	stopped    bool
	term       uint64
	par        int32
	committer  Committer
}

func (o *OffsetMgr) Track(id string, offset int64) {
	o.Lock()
	defer o.Unlock()

	w := o.workers[id]
	if w == nil {
		w = &worker{id: id, offsets: make([]int64, 0, 32)}
		o.workers[id] = w
	}

	w.offsets = append(w.offsets, offset)
}

func (o *OffsetMgr) run() {
	o.stopped = false
	for commit := range o.CommitChan {
		o.Lock()
		if o.stopped {
			o.Unlock()
			break
		}
		w := o.workers[commit.Id]
		//println("prepare commiting", commit.Id, commit.Offset)
		if w == nil {
			//println("skip because not found", commit.Id)
			o.Unlock()
			continue
		}
		//println("prepare commiting", commit.Id, commit.Offset)
		for i, off := range w.offsets {
			if commit.Offset < off {
				w.offsets = w.offsets[i:]
				break
			}
			if err := o.committer.Commit(o.term, o.par, off); err != nil {
				println("commiting ERRRRRRRR", err)
			}
		}
		o.Unlock()
	}
}

func (o *OffsetMgr) Stop() {
	o.Lock()
	o.stopped = true
	close(o.CommitChan)
	o.Unlock()
}

func NewOffsetMgr(par int32, committer Committer, term uint64) *OffsetMgr {
	o := &OffsetMgr{
		Mutex:      &sync.Mutex{},
		CommitChan: make(chan wsworker.Commit, 100),
		committer:  committer,
		par:        par,
		stopped:    true,
		workers:    make(map[string]*worker),
		term:       term,
	}
	go o.run()
	return o
}
