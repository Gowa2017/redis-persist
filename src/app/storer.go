package main

import (
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	"src/src/redis"

	"github.com/syndtr/goleveldb/leveldb"
)

type Storer struct {
	cli *redis.Redis
	db  *leveldb.DB
}

func (s *Storer) reconnect() {
	times := 0
	for {
		wait := times
		times = times + 1

		if wait > 30 {
			wait = 30
		}
		log.Printf("try to reconnect storer, times:%d, wait:%d", times, wait)
		time.Sleep(time.Duration(wait) * time.Second)

		err := s.cli.ReConnect()
		if err != nil {
			log.Printf("[ERROR]reconnect storer failed:%v", err)
			continue
		} else {
			break
		}
	}
}

func (s *Storer) retry(key string, err error) {
	log.Printf("[ERROR]recv message failed, try to reconnect to redis:%v", err)
	s.reconnect()
	s.save(key)
}

func (s *Storer) expire(key string, resp map[string]string) {
	value, ok := resp["expire"]
	if !ok {
		return
	}
	seconds, err := strconv.Atoi(value)
	if err != nil {
		return
	}
	if seconds > 0 {
		log.Printf("expire key:%s, seconds:%d", key, seconds)
		s.cli.Exec("expire", key, seconds)
	}
}

func (s *Storer) save(key string) {
	name, err := s.cli.Type(key)
	if err != nil {
		s.retry(key, err)
		return
	}

	if name != "hash" {
		log.Printf("[ERROR]unexpected key type, key:%s, type:%s", key, name)
		return
	}

	resp := make(map[string]string)
	err = s.cli.Hgetall(key, resp)
	if err != nil {
		s.retry(key, err)
		return
	}

	chunk, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[ERROR]marshal obj failed, key:%s, obj:%v, err:%v", key, resp, err)
		return
	}

	index_key := indexKey(key)
	version := []byte(resp["version"])
	batch := new(leveldb.Batch)
	batch.Put([]byte(index_key), version)
	batch.Put([]byte(key), chunk)
	err = s.db.Write(batch, nil)
	if err != nil {
		log.Printf("[ERROR]save key:%s failed, err:%v", key, err)
		return
	}

	// expire key
	if setting.Redis.Expire {
		s.expire(key, resp)
	}

	log.Printf("save key:%s, data len:%d", key, len(chunk))
}

func (s *Storer) Start(queue chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	err := s.cli.Connect()
	if err != nil {
		log.Panicf("start Storer failed:%v", err)
	}

	log.Printf("start storer succeed")

	for key := range queue {
		s.save(key)
	}
	log.Printf("queue is closed, storer will exit")
}

func NewStorer(db *leveldb.DB) *Storer {
	cli := redis.NewRedis(setting.Redis.Host, setting.Redis.Password, setting.Redis.Db)
	return &Storer{cli, db}
}

type StorerMgr struct {
	instances []*Storer
	queues    []chan string
	wg        sync.WaitGroup
}

func _hash(str string) int {
	h := 0
	for _, c := range str {
		h += int(c)
	}
	return h
}

func (m *StorerMgr) Start(queue chan string) {
	m.wg.Add(1)
	defer m.wg.Done()

	for i, instance := range m.instances {
		m.wg.Add(1)
		go instance.Start(m.queues[i], &m.wg)
	}

	// dispatch msg
	max := len(m.queues)
	for key := range queue {
		i := _hash(key) % max
		m.queues[i] <- key
	}

	log.Printf("queue is closed, all storer will exit")
	for _, queue := range m.queues {
		close(queue)
	}
}

func (m *StorerMgr) Stop() {
	m.wg.Wait()
}

// Use n storer to save to leveldb
// Data from sync_queue fo monitor
func NewStorerMgr(db *leveldb.DB, numInstances int) *StorerMgr {
	m := new(StorerMgr)
	m.instances = make([]*Storer, numInstances)
	m.queues = make([]chan string, numInstances)
	for i := 0; i < numInstances; i++ {
		m.instances[i] = NewStorer(db)
		m.queues[i] = make(chan string, 256)
	}
	return m
}
