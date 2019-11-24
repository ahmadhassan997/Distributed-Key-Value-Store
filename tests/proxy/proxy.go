package proxy

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"paxosapp/rpc/paxosrpc"
	"time"
)

type Proxy interface {
	HasError() bool
	GetError() []string
	Reset()
	Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error
	GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error
	RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error
	RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error
	RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error
	RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error
	RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error
}

type propStatus int

const (
	UNSET   propStatus = 0
	PREPARE propStatus = 1
	ACCEPT  propStatus = 2
)

type proposal struct {
	num    int
	key    string
	val    uint32
	status propStatus
}

type proxy struct {
	srv  *rpc.Client
	err  []string
	prop map[string]*proposal
}

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

/**
 * Proxy validates the requests going into a PaxosNode and the responses coming out of it.  * It logs errors that occurs during a test.
 */
func NewProxy(nodePort, myPort int) (Proxy, error) {
	p := new(proxy)
	p.prop = make(map[string]*proposal)
	p.err = make([]string, 0)

	// Start server
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", myPort))
	if err != nil {
		LOGE.Println("Failed to listen:", err)
		return nil, err
	}

	// Create RPC connection to paxos node.
	srv, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", nodePort))
	if err != nil {
		LOGE.Println("Failed to dial node %d", nodePort)
		return nil, err
	}
	p.srv = srv

	// register RPC
	rpc.RegisterName("PaxosNode", paxosrpc.Wrap(p))
	rpc.HandleHTTP()
	go http.Serve(l, nil)

	// log.Printf("Proxy started")
	return p, nil
}

func (p *proxy) HasError() bool {
	return len(p.err) > 0
}

func (p *proxy) GetError() []string {
	return p.err
}

func (p *proxy) Reset() {
	for _, v := range p.prop {
		v.status = UNSET
	}
	p.err = make([]string, 0)
}

func (p *proxy) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	err := p.srv.Call("PaxosNode.Propose", args, reply)
	return err
}

func (p *proxy) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	err := p.srv.Call("PaxosNode.GetNextProposalNumber", args, reply)
	return err
}

func (p *proxy) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	err := p.srv.Call("PaxosNode.GetValue", args, reply)
	return err
}

func (p *proxy) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	if args.Key == "" {
		p.err = append(p.err, "FAIL: no key specified in prepare")
	}
	p.prop[args.Key] = &proposal{status: PREPARE, num: args.N, key: args.Key}
	err := p.srv.Call("PaxosNode.RecvPrepare", args, reply)
	return err
}

func (p *proxy) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	time.Sleep(100 * time.Millisecond)
	if _, ok := p.prop[args.Key]; !ok {
		p.err = append(p.err, "FAIL: received an unseen key in accept phase.")
		return nil
	}
	if p.prop[args.Key].status != PREPARE {
		p.err = append(p.err, "FAIL: no prepare sent for proposal before accept")
	}
	if p.prop[args.Key].num != args.N {
		p.err = append(p.err, "FAIL: incorrect proposal number received at accept")
	}
	if p.prop[args.Key].key != args.Key {
		p.err = append(p.err, "FAIL: incorrect key received for proposal at accept")
	}

	p.prop[args.Key].status = ACCEPT
	p.prop[args.Key].val = args.V.(uint32)
	err := p.srv.Call("PaxosNode.RecvAccept", args, reply)
	return err
}

func (p *proxy) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	time.Sleep(200 * time.Millisecond)
	if p.prop[args.Key].status != ACCEPT {
		p.err = append(p.err, "FAIL: no accept sent for proposal before commit")
	} else if p.prop[args.Key].key != args.Key {
		p.err = append(p.err, "FAIL: incorrect key received for proposal at commit")
	} else if p.prop[args.Key].val != args.V {
		p.err = append(p.err, "FAIL: incorrect value received for proposal at commit")
	}

	err := p.srv.Call("PaxosNode.RecvCommit", args, reply)
	p.prop[args.Key].status = UNSET
	return err
}

func (p *proxy) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	err := p.srv.Call("PaxosNode.RecvReplaceServer", args, reply)
	return err
}

func (p *proxy) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	err := p.srv.Call("PaxosNode.RecvReplaceCatchup", args, reply)
	return err
}
