package raft

import (
	"bytes"
	"encoding/gob"
)

type Kind uint8

const (
	KindNOP Kind = iota
	KindPut
	KindDeleteRange
)

type Header struct {
	// Unique proposal ID for matching responses.
	ProposalID uint64
}

type PutReq struct {
	Key     []byte
	Value   []byte
	PrevKv  bool
	LeaseID int64
}

type DeleteReq struct {
	Key      []byte
	RangeEnd []byte // nil => single key
	PrevKv   bool
}

type Proposal struct {
	H    Header
	Kind Kind
	Put  *PutReq
	Del  *DeleteReq
}

func Marshal(p *Proposal) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(p); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Unmarshal(b []byte) (*Proposal, error) {
	dec := gob.NewDecoder(bytes.NewReader(b))
	var p Proposal
	if err := dec.Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

func init() {
	gob.Register(&Proposal{})
	gob.Register(&PutReq{})
	gob.Register(&DeleteReq{})
}
