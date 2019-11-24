package paxos

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"paxosapp/rpc/paxosrpc"
	"strconv"
	"time"
)

//proposeTimeout sets the timeout between propose and failure to propose
var proposeTimeout = 15 * time.Second

type paxosNode struct {
	minProp         map[string]int         // list of maximum proposal number seen for each Key so far
	kvStore         map[string]interface{} // Key-Value Store for our Node
	clientMap       map[int]*rpc.Client    // map containing all the connections on which we call the RPC
	acceptPropMap   map[string]acceptPair  // used to keep track of accepted proposals
	nextPropNum     chan passToChan        // chan used to generate proposal numbers
	nextGetVal      chan passToChan        // chan used to get value from key-value store
	nextRecvPrepare chan passToChan        // chan used to process one prepere request at a time for each node
	nextRecvAccept  chan passToChan        // chan used to process one accept message at a time for each node
	nextRecvCommit  chan passToChan        // chan used to commit the value in key-value store
	replacePort     chan passToChan        // chan used to process replace RPCs
	catchup         chan passToChan        // chan used to send commited values in key-value store
	numNodes        int
	srvID           int
}

// passToChan is a single struct used for sending and receiving data on channels
type passToChan struct {
	key         string
	val         interface{}
	N           int
	propNum     chan int
	getVal      chan interface{}
	recvPrepare chan propVal
	recvAccept  chan paxosrpc.Status
	recvCommit  chan bool
	replaced    chan bool
	dataChan    chan []byte
}

// propVal struct to keep track of proposed values
type propVal struct {
	pNum int
	N    int
	val  interface{}
}

// acceptPair struct to keep track of accepted values
type acceptPair struct {
	N   int
	val interface{}
}

//stateHandler handles all state access and all its related operations to make program race
//condition safe
func stateHandler(pn *paxosNode) {
	for {
		select {
		case nextPropNum := <-pn.nextPropNum:
			pNum := pn.minProp[nextPropNum.key] // retrive proposal number from my state
			pNum++                              // increment as is standard procedure
			pNum = pNum*10 + pn.srvID           // append srvID to proposalNumber
			pn.minProp[nextPropNum.key] = pNum  // store this as my new updated proposal number
			nextPropNum.propNum <- pNum
		case nextGetVal := <-pn.nextGetVal:
			val, ok := pn.kvStore[nextGetVal.key]
			if ok {
				nextGetVal.getVal <- val
			} else {
				nextGetVal.getVal <- -1
			}
		case nextRecvPrepare := <-pn.nextRecvPrepare:
			N := nextRecvPrepare.N
			pNum, ok := pn.minProp[nextRecvPrepare.key]
			if N > pNum || !ok {
				pn.minProp[nextRecvPrepare.key] = N
			} else {
				N = pNum
			}
			pair, ok := pn.acceptPropMap[nextRecvPrepare.key]
			if ok {
				nextRecvPrepare.recvPrepare <- propVal{N, pair.N, pair.val}
			} else {
				nextRecvPrepare.recvPrepare <- propVal{N, -1, -1}
			}
		case nextRecvAccept := <-pn.nextRecvAccept:
			N := nextRecvAccept.N
			pNum := pn.minProp[nextRecvAccept.key]
			if N >= pNum {
				pn.acceptPropMap[nextRecvAccept.key] = acceptPair{N, nextRecvAccept.val}
				nextRecvAccept.recvAccept <- paxosrpc.OK
			} else {
				nextRecvAccept.recvAccept <- paxosrpc.Reject
			}
		case nextRecvCommit := <-pn.nextRecvCommit:
			val := nextRecvCommit.val
			pn.kvStore[nextRecvCommit.key] = val
			nextRecvCommit.recvCommit <- true
		case replace := <-pn.replacePort:
			client, err := rpc.DialHTTP("tcp", replace.key)
			if err != nil {
				replace.replaced <- false
			} else {
				pn.clientMap[replace.N] = client
				replace.replaced <- true
			}
		case catchupData := <-pn.catchup:
			b, _ := json.Marshal(pn.kvStore)
			catchupData.dataChan <- b
		default:
			continue
		}
	}
}

// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes numRetries times.
//
// Params:
// myHostPort: the hostport string of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than hostMap[srvId]
// hostMap: a map from all node IDs to their hostports.
//				Note: Please connect to hostMap[srvId] rather than myHostPort
//				when this node try to make rpc call to itself.
// numNodes: the number of nodes in the ring
// numRetries: if we can't connect with some nodes in hostMap after numRetries attempts, an error should be returned
// replace: a flag which indicates whether this node is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvID, numRetries int, replace bool) (PaxosNode, error) {
	newPaxosNode := new(paxosNode)
	newPaxosNode.acceptPropMap = make(map[string]acceptPair)
	newPaxosNode.nextRecvPrepare = make(chan passToChan)
	newPaxosNode.nextRecvCommit = make(chan passToChan)
	newPaxosNode.kvStore = make(map[string]interface{})
	newPaxosNode.nextRecvAccept = make(chan passToChan)
	newPaxosNode.clientMap = make(map[int]*rpc.Client)
	newPaxosNode.nextPropNum = make(chan passToChan)
	newPaxosNode.replacePort = make(chan passToChan)
	newPaxosNode.nextGetVal = make(chan passToChan)
	newPaxosNode.catchup = make(chan passToChan)
	newPaxosNode.minProp = make(map[string]int)
	newPaxosNode.numNodes = numNodes
	newPaxosNode.srvID = srvID
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		fmt.Println("Couldn't listen on Port", srvID)
		return nil, err
	}
	rpcServer := rpc.NewServer()
	rpcServer.Register(paxosrpc.Wrap(newPaxosNode))
	http.DefaultServeMux = http.NewServeMux()
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	go http.Serve(listener, nil)
	for i := 0; i < numNodes; i++ {
		retry := true // used to retry if not connected until given number of Retries
		for j := 0; j <= numRetries && retry; j++ {
			client, err := rpc.DialHTTP("tcp", hostMap[i]) // listen on hostMap ports to call RPCs on these connections
			if err == nil {
				retry = false
				newPaxosNode.clientMap[i] = client
			} else {
				time.Sleep(time.Second)
			}
		}
		if retry == true {
			fmt.Println("can't dial to node ", i)
			return nil, errors.New("Can't Dial to Node " + strconv.Itoa(i))
		}
	}
	go stateHandler(newPaxosNode)
	if replace {
		replaceArgs := &paxosrpc.ReplaceServerArgs{SrvID: srvID, Hostport: hostMap[srvID]}
		for i := 0; i < newPaxosNode.numNodes; i++ {
			var replaceReply paxosrpc.ReplaceServerReply
			newPaxosNode.clientMap[i].Call("PaxosNode.RecvReplaceServer", replaceArgs, &replaceReply)
		}
		catcupArgs := &paxosrpc.ReplaceCatchupArgs{}
		var catchupReply paxosrpc.ReplaceCatchupReply
		newPaxosNode.clientMap[0].Call("PaxosNode.RecvReplaceCatchup", catcupArgs, &catchupReply)
		var buf map[string]interface{}
		err := json.Unmarshal(catchupReply.Data, &buf)
		if err != nil {
			fmt.Println("Can't Unmarshal")
		}
		for key, value := range buf {
			newPaxosNode.kvStore[key] = uint32(value.(float64))
		}
	}
	return newPaxosNode, nil
}

// Desc:
// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat for a key, and for a particular
// <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key
func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	getPropChan := make(chan int)
	pn.nextPropNum <- passToChan{key: args.Key, propNum: getPropChan}
	reply.N = <-getPropChan
	return nil
}

// Desc:
// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or proposeTimeout seconds have passed.
//
// Params:
// args: the key, value pair to propose together with the proposal number returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	// PHASE-1
	start := time.Now()
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	max := args.N
	value := args.V
	majorityPrepare := 0
	prepareArgs := &paxosrpc.PrepareArgs{Key: args.Key, N: args.N, RequesterId: pn.srvID}
	for i := 0; i < pn.numNodes; i++ {
		var prepareReply paxosrpc.PrepareReply
		pn.clientMap[i].Call("PaxosNode.RecvPrepare", prepareArgs, &prepareReply)
		if prepareReply.Status == paxosrpc.OK {
			if prepareReply.N_a > max {
				max = prepareReply.N_a
				value = prepareReply.V_a
			}
			majorityPrepare++
		}
	}
	for majorityPrepare < pn.numNodes/2+1 && time.Since(start) < proposeTimeout {
		time.Sleep(time.Duration(r1.Intn(4)) * time.Second)
		max, value, majorityPrepare = broadcastPrepare(pn, args)
	}
	if majorityPrepare < pn.numNodes/2+1 {
		return errors.New("Propose Quorum Not Formed " + strconv.Itoa(pn.srvID))
	}
	reply.V = value
	// PHASE-2
	if !broadcastAccept(pn, args.Key, max, value) {
		time.Sleep(time.Duration(r1.Intn(4)) * time.Second)
		max, value, majorityPrepare = broadcastPrepare(pn, args)
		reply.V = value
		broadcastAccept(pn, args.Key, max, value)
	}
	commitArgs := &paxosrpc.CommitArgs{Key: args.Key, V: value, RequesterId: pn.srvID}
	for i := 0; i < pn.numNodes; i++ {
		var commitReply paxosrpc.CommitReply
		pn.clientMap[i].Call("PaxosNode.RecvCommit", commitArgs, &commitReply)
	}
	return nil
}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	nextVal := make(chan interface{})
	pn.nextGetVal <- passToChan{key: args.Key, getVal: nextVal}
	recVal := <-nextVal
	if recVal != -1 {
		reply.Status = paxosrpc.KeyFound
		reply.V = recVal
	} else {
		reply.Status = paxosrpc.KeyNotFound
		reply.V = nil
	}
	return nil
}

// Desc:
// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Prepare Message, you must include RequesterId when you call this API
// reply: the Prepare Reply Message
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	nextPrepare := make(chan propVal)
	pn.nextRecvPrepare <- passToChan{key: args.Key, N: args.N, recvPrepare: nextPrepare}
	retPair := <-nextPrepare
	if retPair.pNum == args.N {
		reply.Status = paxosrpc.OK
	} else {
		reply.Status = paxosrpc.Reject
	}
	if retPair.N == -1 {
		reply.N_a = -1
		reply.V_a = nil
	} else {
		reply.N_a = retPair.N
		reply.V_a = retPair.val
	}
	return nil
}

// Desc:
// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Please Accept Message, you must include RequesterId when you call this API
// reply: the Accept Reply Message
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	nextAccept := make(chan paxosrpc.Status)
	pn.nextRecvAccept <- passToChan{key: args.Key, N: args.N, val: args.V, recvAccept: nextAccept}
	ret := <-nextAccept
	reply.Status = ret
	return nil
}

// Desc:
// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message.
//
// Params:
// args: the Commit Message, you must include RequesterId when you call this API
// reply: the Commit Reply Message
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	nextCommit := make(chan bool)
	pn.nextRecvCommit <- passToChan{key: args.Key, val: args.V, recvCommit: nextCommit}
	<-nextCommit
	return nil
}

// Desc:
// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
//
// Params:
// args: the id and the hostport of the server being replaced
// reply: no use
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	replace := make(chan bool)
	pn.replacePort <- passToChan{key: args.Hostport, N: args.SrvID, replaced: replace}
	<-replace
	return nil
}

// Desc:
// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
//
// Params:
// args: no use
// reply: a byte array containing necessary data used by replacement server to recover
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	data := make(chan []byte)
	pn.catchup <- passToChan{dataChan: data}
	reply.Data = <-data
	return nil
}

// broadcastPrepare to send prepare rpc to all nodes
func broadcastPrepare(pn *paxosNode, args *paxosrpc.ProposeArgs) (int, interface{}, int) {
	max := args.N
	value := args.V
	majorityPrepare := 0
	for i := 0; i < pn.numNodes; i++ {
		proposalArgs := &paxosrpc.ProposalNumberArgs{Key: args.Key}
		var proposalReply paxosrpc.ProposalNumberReply
		pn.clientMap[i].Call("PaxosNode.GetNextProposalNumber", proposalArgs, &proposalReply)
		prepareArgs := &paxosrpc.PrepareArgs{Key: args.Key, N: proposalReply.N, RequesterId: pn.srvID}
		var prepareReply paxosrpc.PrepareReply
		pn.clientMap[i].Call("PaxosNode.RecvPrepare", prepareArgs, &prepareReply)
		if prepareReply.Status == paxosrpc.OK {
			if prepareReply.N_a > max {
				max = prepareReply.N_a
				value = prepareReply.V_a
			}
			majorityPrepare++
		}
	}
	return max, value, majorityPrepare
}

// broadcastAccept to send accept rpc to all nodes
func broadcastAccept(pn *paxosNode, key string, max int, value interface{}) bool {
	majorityAccept := 0
	acceptArgs := &paxosrpc.AcceptArgs{Key: key, N: max, V: value, RequesterId: pn.srvID}
	for i := 0; i < pn.numNodes; i++ {
		var acceptReply paxosrpc.AcceptReply
		pn.clientMap[i].Call("PaxosNode.RecvAccept", acceptArgs, &acceptReply)
		if acceptReply.Status == paxosrpc.OK {
			majorityAccept++
		}
	}
	return majorityAccept >= pn.numNodes/2+1
}
