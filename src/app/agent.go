package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

type AgentHandler func(ud interface{}, params interface{}) (interface{}, error)

type AgentSvr struct {
	ln      net.Listener
	db      *leveldb.DB
	handers map[string][]interface{}
	wg      sync.WaitGroup
}

type Request struct {
	Id     uint32
	Method string
	Params interface{}
}

type Response struct {
	Id     uint32      `json:"id"`
	Result interface{} `json:"result"`
	Error  interface{} `json:"error"`
}

func (srv *AgentSvr) dispatchRequst(conn *net.TCPConn, req *Request) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[ERROR]handle agent connection:%v failed:%v", conn.RemoteAddr(), err)
		}
	}()
	cb, ok := srv.handers[req.Method]
	if ok {
		ud := cb[0]
		handler := cb[1].(AgentHandler)
		var resp Response
		resp.Id = req.Id
		if result, err := handler(ud, req.Params); err != nil {
			resp.Error = err
		} else {
			resp.Result = result
		}
		body, err := json.Marshal(resp)
		if err != nil {
			log.Panicf("marshal response conn:%v, failed:%v", conn.RemoteAddr(), err)
		}

		length := uint32(len(body))
		buf := bytes.NewBuffer(nil)
		binary.Write(buf, binary.BigEndian, length)
		buf.Write(body)
		chunk := buf.Bytes()
		if _, err = conn.Write(chunk); err != nil {
			log.Panicf("write response conn:%v, failed:%v", conn.RemoteAddr(), err)
		}
	} else {
		log.Printf("[ERROR]unknown request:%v", req)
	}
}

func (srv *AgentSvr) handleConnection(conn *net.TCPConn) {
	defer conn.Close()
	defer srv.wg.Done()
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[ERROR]handle agent connection:%v failed:%v", conn.RemoteAddr(), err)
		}
	}()

	log.Printf("new agent connection:%v", conn.RemoteAddr())
	for {
		var sz uint32
		err := binary.Read(conn, binary.BigEndian, &sz)
		if err != nil {
			log.Printf("[ERROR]read conn failed:%v, err:%v", conn.RemoteAddr(), err)
			break
		}
		buf := make([]byte, sz)
		_, err = io.ReadFull(conn, buf)
		if err != nil {
			log.Printf("[ERROR]read conn failed:%v, err:%v", conn.RemoteAddr(), err)
			break
		}
		var req Request
		if err = json.Unmarshal(buf, &req); err != nil {
			log.Printf("[ERROR]parse request failed:%v, err:%v", conn.RemoteAddr(), err)
		}

		go srv.dispatchRequst(conn, &req)
	}
}

func (srv *AgentSvr) Start() {
	srv.wg.Add(1)
	defer srv.wg.Done()

	ln, err := net.Listen("tcp", setting.Agent.Addr)
	if err != nil {
		log.Panicf("resolve local addr failed:%s", err.Error())
	}
	log.Printf("start agent succeed:%s", setting.Agent.Addr)

	// register handler
	srv.Register("Get", srv, handlerGet)

	srv.ln = ln
	for {
		conn, err := srv.ln.Accept()
		if err != nil {
			log.Printf("[ERROR]accept failed:%v", err)
			if opErr, ok := err.(*net.OpError); ok {
				if !opErr.Temporary() {
					break
				}
			}
			continue
		}
		srv.wg.Add(1)
		go srv.handleConnection(conn.(*net.TCPConn))
	}
}

func (srv *AgentSvr) Stop() {
	if srv.ln != nil {
		srv.ln.Close()
	}
	srv.wg.Wait()
}

func (srv *AgentSvr) Register(cmd string, ud interface{}, handler AgentHandler) {
	srv.handers[cmd] = []interface{}{ud, handler}
}

func handlerGet(ud interface{}, params interface{}) (result interface{}, err error) {
	agent := ud.(*AgentSvr)
	key := params.(string)
	log.Printf("agent get:%v", key)
	chunk, err := agent.db.Get([]byte(key), nil)
	if chunk == nil || err != nil {
		log.Printf("[ERROR]query key:%s failed:%v", key, err)
		return
	}
	var data map[string]string
	if err = json.Unmarshal(chunk, &data); err != nil {
		log.Printf("[ERROR]unmarshal key:%s failed:%v", key, err)
		return
	}

	result = data
	return
}

//Use agent to read data from leveldb
func NewAgent(db *leveldb.DB) *AgentSvr {
	agent := new(AgentSvr)
	agent.db = db
	agent.handers = make(map[string][]interface{})
	return agent
}
