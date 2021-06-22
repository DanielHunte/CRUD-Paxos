package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"net"
	"sync"
	"math/rand"
	"strconv"
	"strings"
)
var paxosRound int

// Message queues for the proposer, acceptor, and learner
var proposerQueue  = make(chan Message, 50)
var acceptorQueue  = make(chan Message, 50)
var learnerQueue  = make(chan Message, 50)

//Struct to store a person's name and age
type Person struct {
	Name string
	Age  int
}

// Struct to store proposal value info
type ProposalVal struct{
	Command string
	Id int
	Person Person
}

// Struct to store paxos message info
type PaxosMessage struct {
	Type string
	Id string
	Val ProposalVal
	UniqId int
}

//Struct to store client Message info. The Person and Id fields may or may not be used.
type Message struct {
	Type   string
	Receiver string
	SenderPortNo string
	Pmessage PaxosMessage
	Person Person
	Id  int // An Id of -1 means send all students
}

// Sync object used when syncing with other backends n startup
type Sync struct {
	Id string
	PaxosRound int
	students map[int]Person
}

//Global variable to store information of students
var students = map[int]Person{0: Person{"Daniel", 23},1: Person{"Naman", 22}}
var dbMutex sync.RWMutex

/*
Synchronizes with all other backends and updates its databse if needed
backends: list of portNos for backends
id: current proposal number
*/
func synchronize(backends []string, id string) {
	databases := make(map[string]Sync)
	for _,backend := range backends {
		conn, err := net.Dial("tcp", backend)
		if err != nil {
			fmt.Println(fmt.Sprintf("Detected failure on %s", backend))
		} else {
			enc := gob.NewEncoder(conn)
			enc.Encode(Message{Type: "Sync"})
			
			var database Sync
			dec := gob.NewDecoder(conn)
			dec.Decode(&database.Id)
			dec.Decode(&database.PaxosRound)
			dec.Decode(&database.students)
			databases[database.Id] = database

			conn.Close()
		}
	}
	paxosRound = 0
	latest := id
	for _,database := range databases {
		if database.PaxosRound > paxosRound {
			latest = database.Id
			paxosRound = database.PaxosRound
		}
	}
	if latest != id {
		dbMutex.Lock()
		students = databases[latest].students
		dbMutex.Unlock()
	}
	fmt.Println("Done syncing with other backends")
}

/*
Handles all reads from frontend and reroutes messages to relevent message queues
portNo: portNo for this backend
id: unique id for this backend
*/
func handleMessgaes(portNo string, id string) {
	listener, err := net.Listen("tcp", ":" + portNo)
	fmt.Println(fmt.Sprintf("Listening on port %s", portNo))
	if err != nil {
		fmt.Println(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
		} else {
			defer conn.Close()
			var msg Message
			dec := gob.NewDecoder(conn)
			dec.Decode(&msg)
			switch msg.Type {
			case "Heartbeat":
				enc := gob.NewEncoder(conn)
				UniqId, _ := strconv.Atoi(id)
				enc.Encode(UniqId)
			case "Read":
				enc := gob.NewEncoder(conn)
				// An Id of -1 means send all students
				if msg.Id == -1 {
					dbMutex.RLock()
					enc.Encode(students)
					dbMutex.RUnlock()
				} else {
					dbMutex.RLock()
					if _, ok := students[msg.Id]; ok {
						enc.Encode([]Person{students[msg.Id]})
						dbMutex.RUnlock()
					} else {
						dbMutex.RUnlock()
						enc.Encode([]Person{})
					}
				}
			case "Sync":
				enc := gob.NewEncoder(conn)
				enc.Encode(id)
				enc.Encode(paxosRound)
				dbMutex.RLock()
				enc.Encode(students)
				dbMutex.RUnlock()
			default:
				switch msg.Receiver {
					case "Proposer":
						proposerQueue <- msg
					case "Acceptor":
						acceptorQueue <- msg
					case "Learner":
						learnerQueue <- msg
				}
			}
		}
	}
}

/*
Determines if a proposal number is less than another proposal number
left: proposal number to the left of < symbol
right: proposal number to the right of < symbol
*/
func gt(left string, right string) bool {
	list := strings.Split(left, ".")
	num1, _ := strconv.Atoi(list[0])

	list = strings.Split(right ,".")
	num2, _ := strconv.Atoi(list[0])
	return num1 > num2
}

/*
Determines if a proposal number is less than or equal to another proposal number
left: proposal number to the left of <= symbol
right: proposal number to the right of <= symbol
*/
func gte(left string, right string) bool {
	list := strings.Split(left, ".")
	num1, _ := strconv.Atoi(list[0])

	list = strings.Split(right ,".")
	num2, _ := strconv.Atoi(list[0])
	return num1 >= num2
}

/*
Acceptor handles all request reveived from proposers
portNo: portNo for this backend
nodeId: unique id for this backend
proposalId: proposal number pointer
idMutex: mutex for proposal number
*/
func acceptor(portNo string, nodeId string, proposalId *string, idMutex *sync.RWMutex) {
	var promised string
	var val ProposalVal
	for {
		msg := <- acceptorQueue
		switch msg.Pmessage.Type {
			case "Prepare":
				if gt(msg.Pmessage.Id, promised) {
					promised = msg.Pmessage.Id
					idMutex.Lock()
					if gt(msg.Pmessage.Id, *proposalId) {
						list := strings.Split(msg.Pmessage.Id, ".")
						num, _ := strconv.Atoi(list[0])
						*proposalId = strconv.Itoa(num + 1) + "." + nodeId
					}
					idMutex.Unlock()
					pMessage := PaxosMessage{Type: "Promise", Id: msg.Pmessage.Id, Val: val, UniqId: msg.Pmessage.UniqId}
					response := Message{SenderPortNo: portNo, Receiver: "Proposer", Pmessage: pMessage}
					if msg.SenderPortNo == portNo {
						proposerQueue <- response
					} else {
						outgoing, err := net.Dial("tcp", msg.SenderPortNo)
						if err != nil {
							fmt.Println(err)
						}
						defer outgoing.Close()
						enc := gob.NewEncoder(outgoing)
						enc.Encode(response)
					}
				} else {
					pMessage := PaxosMessage{Type: "Promise", Id: promised, Val: val, UniqId: msg.Pmessage.UniqId}
					response := Message{SenderPortNo: portNo, Receiver: "Proposer", Pmessage: pMessage}
					if msg.SenderPortNo == portNo {
						proposerQueue <- response
					} else {
						outgoing, err := net.Dial("tcp", msg.SenderPortNo)
						if err != nil {
							fmt.Println(err)
						}
						defer outgoing.Close()
						enc := gob.NewEncoder(outgoing)
						enc.Encode(response)
					}
				}
			case "Accept":
				if gte(msg.Pmessage.Id, promised) {
					idMutex.Lock()
					if gt(msg.Pmessage.Id, *proposalId) {
						list := strings.Split(msg.Pmessage.Id, ".")
						num, _ := strconv.Atoi(list[0])
						*proposalId = strconv.Itoa(num + 1) + "." + nodeId
					}
					idMutex.Unlock()
					promised = msg.Pmessage.Id
					val = msg.Pmessage.Val
					pMessage := PaxosMessage{Type: "Ack", Id: msg.Pmessage.Id, Val: val, UniqId: msg.Pmessage.UniqId}
					response := Message{SenderPortNo: portNo, Receiver: "Proposer", Pmessage: pMessage}
					if msg.SenderPortNo == portNo {
						proposerQueue <- response
					} else {
						outgoing, err := net.Dial("tcp", msg.SenderPortNo)
						if err != nil {
							fmt.Println(err)
						}
						defer outgoing.Close()
						enc := gob.NewEncoder(outgoing)
						enc.Encode(response)
					}
				} else {
					pMessage := PaxosMessage{Type: "Nack", Id: promised, Val: val, UniqId: msg.Pmessage.UniqId}
					response := Message{SenderPortNo: portNo, Receiver: "Proposer", Pmessage: pMessage}
					if msg.SenderPortNo == portNo {
						proposerQueue <- response
					} else {
						outgoing, err := net.Dial("tcp", msg.SenderPortNo)
						if err != nil {
							fmt.Println(err)
						}
						defer outgoing.Close()
						enc := gob.NewEncoder(outgoing)
						enc.Encode(response)
					}
				}
				
		}
	}
}

/*
Proposer proposed all write requests received from the frontend
portNo: portNo for this backend
backends: list of port numbers of all other backends
nodeId: unique id for this backend
proposalId: proposal number pointer
idMutex: mutex for proposal number
*/
func proposer(portNo string, backends []string, nodeId string, proposalId *string, idMutex *sync.RWMutex) {
	proposalMap := make(map[int]chan Message)
	var mapMutex sync.RWMutex
	for {
		msg := <- proposerQueue
		switch msg.Type{
			case "Create":
				mapId := makeProposalQueue(proposalMap, mapMutex)
				go propose(portNo, nodeId, mapId, proposalMap,
						   mapMutex, proposalId,
						   idMutex, ProposalVal{Command: "Create", Person: msg.Person}, backends)
			case "Update":
				mapId := makeProposalQueue(proposalMap, mapMutex)
				go propose(portNo, nodeId, mapId, proposalMap,
						   mapMutex, proposalId,
						   idMutex, ProposalVal{Command: "Update", Id: msg.Id, Person: msg.Person}, backends)
			case "Delete":
				mapId := makeProposalQueue(proposalMap, mapMutex)
				go propose(portNo, nodeId, mapId, proposalMap,
						   mapMutex, proposalId,
						   idMutex, ProposalVal{Command: "Delete", Id: msg.Id}, backends)
			default:
				if _, ok := proposalMap[msg.Pmessage.UniqId]; ok {
					proposalMap[msg.Pmessage.UniqId] <- msg
				}
		}
	}
}

/*
Creates and inserts a new queue into the proposalMap and return the unique id used to access it
proposalMap: map containing all the message channels for all proposals being processed
mapMutex: mutex for proposalMap
*/
func makeProposalQueue(proposalMap map[int]chan Message, mapMutex sync.RWMutex) int {
	mapId := rand.Int()
	mapMutex.RLock()
	_, ok := proposalMap[mapId]
	for ok {
		mapId := rand.Int()
		_, ok = proposalMap[mapId]
	}
	mapMutex.RUnlock()
	mapMutex.Lock()
	proposalMap[mapId] = make(chan Message, 50)
	mapMutex.Unlock()
	return mapId
}

/*
Proposes the value passed to it
portNo: portNo for this backend
nodeId: unique id for this backend
mapId: unique map id for this proposal
proposalMap: map containing all the message channels for all proposals being processed
mapMutex: mutex for proposalMap
proposalId: proposal number pointer
idMutex: mutex for proposal number
val: value to be proposed
backeds: list of port numbers of all other backends
*/
func propose(portNo string, nodeId string, mapId int, proposalMap map[int]chan Message, 
			 mapMutex sync.RWMutex, proposalId *string, 
			 idMutex *sync.RWMutex, val ProposalVal, backends []string) {
	idMutex.Lock()
	list := strings.Split(*proposalId, ".")
	num, _ := strconv.Atoi(list[0])
	*proposalId = strconv.Itoa(num + 1) + "." + nodeId
	initialProposalId := *proposalId
	idMutex.Unlock()
	fmt.Println(fmt.Sprintf("Proposal No. = %s", initialProposalId))
	fmt.Print("Proposing")
	fmt.Println(val)

	pMessage := PaxosMessage{Type: "Prepare", Id: initialProposalId, Val: val, UniqId: mapId}
	acceptorQueue <- Message{SenderPortNo: portNo, Receiver: "Acceptor", Pmessage: pMessage}
	for _,backend := range backends {
		outgoing, err := net.Dial("tcp", backend)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer outgoing.Close()
		enc := gob.NewEncoder(outgoing)
		idMutex.RLock()
		pMessage := PaxosMessage{Type: "Prepare", Id: initialProposalId, Val: val, UniqId: mapId}
		idMutex.RUnlock()
		enc.Encode(Message{SenderPortNo: portNo, Receiver: "Acceptor", Pmessage: pMessage})
	}

	maxProposalId := initialProposalId
	quorom := ((len(backends) + 1)/2) + 1

	numOfPromises := 0
	for numOfPromises < quorom {
		msg := <- proposalMap[mapId]
		if msg.Pmessage.Type == "Promise" {
			if gt(msg.Pmessage.Id, maxProposalId) {
				*proposalId = strconv.Itoa(num + 1) + "." + nodeId
				maxProposalId = msg.Pmessage.Id
				val = msg.Pmessage.Val
			}
			numOfPromises += 1
		}
	}

	idMutex.Lock()
	if gt(*proposalId, maxProposalId) {
		maxProposalId = *proposalId
	} else if gt(maxProposalId, *proposalId) {
		*proposalId = maxProposalId
	}
	idMutex.Unlock()

	pMessage = PaxosMessage{Type: "Accept", Id: maxProposalId, Val: val, UniqId: mapId}
	acceptorQueue <- Message{SenderPortNo: portNo, Receiver: "Acceptor", Pmessage: pMessage}
	for _,backend := range backends {
		outgoing, err := net.Dial("tcp", backend)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer outgoing.Close()
		enc := gob.NewEncoder(outgoing)
		idMutex.RLock()
		pMessage := PaxosMessage{Type: "Accept", Id: maxProposalId, Val: val, UniqId: mapId}
		idMutex.RUnlock()
		enc.Encode(Message{SenderPortNo: portNo, Receiver: "Acceptor", Pmessage: pMessage})
	}

	numOfAcks := 0
	for numOfAcks < quorom {
		msg := <- proposalMap[mapId]
		if msg.Pmessage.Type == "Ack" {
			if msg.Pmessage.Id == maxProposalId {
				fmt.Println(fmt.Sprintf("Accept from %s", msg.SenderPortNo))
				numOfAcks += 1
			}
		}
	}

	if numOfAcks >= quorom { // commit
		msg := Message{Receiver: "Learner", Pmessage: PaxosMessage{Id: maxProposalId, Val: val}}
		learnerQueue <- msg
		for _,backend := range backends {
			outgoing, err := net.Dial("tcp", backend)
			if err != nil {
				fmt.Println(err)
				continue
			}
			defer outgoing.Close()
			enc := gob.NewEncoder(outgoing)
			enc.Encode(msg)
		}
	}
	mapMutex.Lock()
	delete(proposalMap, mapId)
	mapMutex.Unlock()
	fmt.Println("Propose complete")
}

// Commits all write requests appropriately and increments the paxos round number
func learner() {
	for {
		msg := <- learnerQueue
		list := strings.Split(msg.Pmessage.Id, ".")
		personId, _ := strconv.Atoi(list[0])
		val := msg.Pmessage.Val
		switch val.Command {
		case "Create":
			dbMutex.Lock()
			students[personId] = val.Person
			fmt.Print("Learner created ")
			fmt.Println(students[personId])
			dbMutex.Unlock()
			personId += 1
		case "Update":
			dbMutex.RLock()
			if _, ok := students[val.Id]; ok {
				dbMutex.RUnlock()
				dbMutex.Lock()
				students[val.Id] = val.Person
				fmt.Print("Leaner updated ")
				fmt.Println(students[val.Id])
				dbMutex.Unlock()
			} else {
				dbMutex.RUnlock()
			}
		case "Delete":
			dbMutex.RLock()
			if _, ok := students[val.Id]; ok {
				dbMutex.RUnlock()
				dbMutex.Lock()
				fmt.Print("Learner deleted ")
				fmt.Println(students[val.Id])
				delete(students, val.Id)
				dbMutex.Unlock()
			} else {
				dbMutex.RUnlock()
			}
			
		}
		paxosRound += 1
		fmt.Println(fmt.Sprintf("Paxos Round = %d\n", paxosRound))
	}
}

//Main driver function for backend
func main() {
	portNo := flag.String("listen", "8090", "listen port number")
	backends := flag.String("backend", ":8090,:8091", "other backend port numbers")
	nodeId := flag.String("id", "1", "unique id for backend")
	flag.Parse()

	go handleMessgaes(*portNo, *nodeId)

	backendsList := strings.Split(*backends, ",")
	go synchronize(backendsList, *nodeId)

	proposalId := "1." + *nodeId
	var idMutex sync.RWMutex
	go proposer(":" + *portNo, backendsList, *nodeId, &proposalId, &idMutex)
	go acceptor(":" + *portNo, *nodeId, &proposalId, &idMutex)
	go learner()

	block := make(chan string)
	<- block
}
