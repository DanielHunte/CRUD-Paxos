package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kataras/iris/v12"
)

//Struct to store a person's name and age
type Person struct {
	Name string
	Age  int
}

//Struct to store client Message info
type Message struct {
	Type         string
	Receiver     string
	SenderPortNo string
	Pmessage PaxosMessage
	Person       Person
	Id           int // An Id of -1 means send all students
}

// Struct to store proposal value info
type ProposalVal struct {
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

/*
Creates the html text for the home page based on students
students: slice of Person containing all students
return: html text for homepage
*/
func homepage(students map[int]Person) string {
	res := "<h1>Students in Jeff Epstein's Class</h1>\n"
	for id, person := range students {
		res += fmt.Sprintf("\t<br><a href=\"/person/%d\">%s</a>\n", id, person.Name)
	}
	res += "<br><br><a href=\"/create\"> Create</a>"
	return res
}

/*
Requests a heartbeat from a backend and when received, returns that backend's nodeId
backend: portNo for backend
numOfBackends: total number of backends
return: the nodeId of the backend
*/
func checkBackend(backend string, numOfBackends int) int {
	conn, err := net.Dial("tcp", backend)
	if err != nil {
		fmt.Println(fmt.Sprintf("Detected failure on %s", backend))
		return numOfBackends + 1
	} else {
		enc := gob.NewEncoder(conn)
		enc.Encode(Message{Type: "Heartbeat"})

		var id int
		dec := gob.NewDecoder(conn)
		dec.Decode(&id)

		conn.Close()
		return id
	}
}

/* Checks connection every 2s and assigns primary, prints error message if down.
backends: list of port numbers for all backends
primary: string to store the portNo of the primary
mutex: mutex for the primary string
*/
func setPrimary(backends []string, primary *string, mutex sync.RWMutex) {
	for {
		//fmt.Println(backends)
		portNos := make(map[int]string)
		for _, backend := range backends {
			portNos[checkBackend(backend, len(backends))] = backend
		}
		if _, ok := portNos[len(backends) + 1]; ok {
			fmt.Println("")
		}
		minId := len(backends) + 1
		for id, _ := range portNos {
			if id < minId {
				minId = id
			}
		}
		if minId != len(backends)+1 {
			portNo := portNos[minId]
			mutex.RLock()
			if portNo != *primary {
				mutex.RUnlock()
				mutex.Lock()
				*primary = portNo
				mutex.Unlock()
				fmt.Println(fmt.Sprintf("\nCurrent primary: %s\n", portNo))
			} else {
				mutex.RUnlock()
			}
		}
		time.Sleep(3 * time.Second)
	}
}

//Main driver function for fronted
func main() {
	portNo := flag.String("listen", "8080", "port number")
	backends := flag.String("backend", ":8090,:8091,:8092", "primary port numbers")
	flag.Parse()

	backendsList := strings.Split(*backends, ",")
	var mutex sync.RWMutex
	var primary *string
	dummy := ""
	primary = &dummy

	go setPrimary(backendsList, primary, mutex)

	app := iris.Default()

	app.RegisterView(iris.HTML("./files", ".html"))

	// Handler for the homeapage
	app.Get("/", func(ctx iris.Context) {
		mutex.RLock()
		conn, err := net.Dial("tcp", *primary)
		mutex.RUnlock()
		if err != nil {
			fmt.Print(err)
		}
		defer conn.Close()

		enc := gob.NewEncoder(conn)
		enc.Encode(Message{Type: "Read", Id: -1})
		var students map[int]Person
		dec := gob.NewDecoder(conn)
		dec.Decode(&students)

		ctx.HTML(homepage(students))
	})

	// Handler for the person view page
	app.Get("/person/{num}", func(ctx iris.Context) {
		i, err := strconv.Atoi(ctx.Params().Get("num"))
		if err != nil {
			fmt.Println(err)
		}
		mutex.RLock()
		conn, err := net.Dial("tcp", *primary)
		mutex.RUnlock()
		if err != nil {
			fmt.Println(err)
		}
		defer conn.Close()

		enc := gob.NewEncoder(conn)
		enc.Encode(Message{Type: "Read", Id: i})

		var student []Person
		dec := gob.NewDecoder(conn)
		dec.Decode(&student)

		if len(student) == 0 {
			ctx.View("absent.html")
		} else {
			ctx.ViewData("Name", student[0].Name)
			ctx.ViewData("Age", student[0].Age)
			ctx.ViewData("i", i)
			ctx.View("personView.html")
		}
	})

	//Handler for deleting a person.
	app.Get("/person/{num}/delete", func(ctx iris.Context) {
		i, err := strconv.Atoi(ctx.Params().Get("num"))
		if err != nil {
			fmt.Println(err)
		}

		mutex.RLock()
		conn, err := net.Dial("tcp", *primary)
		mutex.RUnlock()
		if err != nil {
			fmt.Println(err)
		}
		defer conn.Close()

		enc := gob.NewEncoder(conn)
		enc.Encode(Message{Type: "Delete", Receiver: "Proposer", Id: i})

		ctx.Redirect("/")
	})

	//Handler for editing a person's information.
	app.Get("/person/{num}/edit", func(ctx iris.Context) {
		i, err := strconv.Atoi(ctx.Params().Get("num"))
		if err != nil {
			fmt.Println(err)
		}
		mutex.RLock()
		conn, err := net.Dial("tcp", *primary)
		mutex.RUnlock()
		if err != nil {
			fmt.Println(err)
		}
		defer conn.Close()

		enc := gob.NewEncoder(conn)
		enc.Encode(Message{Type: "Read", Id: i})
		var student []Person
		dec := gob.NewDecoder(conn)
		dec.Decode(&student)

		ctx.ViewData("Name", student[0].Name)
		ctx.ViewData("Age", student[0].Age)
		ctx.ViewData("i", i)
		ctx.View("edit.html")
	})

	//Handler for executing changes to a person. It redirects back to the homepage.
	app.Post("/update/{num}", func(ctx iris.Context) {
		i, err := strconv.Atoi(ctx.Params().Get("num"))
		if err != nil {
			fmt.Println(err)
		}
		name := ctx.PostValue("name")
		age := ctx.PostValue("age")
		ageInt, err := strconv.Atoi(age)
		if err != nil {
			fmt.Println(err)
		}

		mutex.RLock()
		conn, err := net.Dial("tcp", *primary)
		mutex.RUnlock()
		if err != nil {
			fmt.Println(err)
		}
		defer conn.Close()

		enc := gob.NewEncoder(conn)
		enc.Encode(Message{Type: "Update", Receiver: "Proposer", Person: Person{Name: name, Age: ageInt}, Id: i})

		ctx.Redirect("/")
	})

	//Handler for creating a person.
	app.Get("/create", func(ctx iris.Context) {
		ctx.View("create.html")
	})

	//Handler for applying changes to internal data after creating a person. It redirects to homepage.
	app.Post("/created", func(ctx iris.Context) {
		name := ctx.PostValue("name")
		age := ctx.PostValue("age")
		ageInt, err := strconv.Atoi(age)
		if err != nil {
			fmt.Println(err)
		}

		mutex.RLock()
		conn, err := net.Dial("tcp", *primary)
		mutex.RUnlock()
		if err != nil {
			fmt.Println(err)
		}
		defer conn.Close()

		enc := gob.NewEncoder(conn)
		enc.Encode(Message{Type: "Create", Receiver: "Proposer", Person: Person{Name: name, Age: ageInt}})

		ctx.Redirect("/")
	})

	app.Run(iris.Addr(":" + *portNo))
}
