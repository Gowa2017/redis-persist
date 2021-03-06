package main

import (
	"fmt"
	"log"
	"time"

	"src/src/redis"
)

// Subscribe to redis , and receive notifies
type Monitor struct {
	cli                 *redis.Redis // redis client
	notification_config string       // notification config to be set on server
	event               string       // subscribe event
	qlen                int          // queue  length
	quit_flag           bool
	quit_chan           chan int
}

func (m *Monitor) subscribe() error {
	config_key := "notify-keyspace-events"
	_, err := m.cli.Exec("config", "set", config_key, m.notification_config)
	if err != nil {
		return err
	}
	log.Printf("config set %s = %s", config_key, m.notification_config)

	_, err = m.cli.Exec("subscribe", m.event)
	if err != nil {
		return err
	}

	log.Printf("subscribe: %s", m.event)
	return nil
}

func (m *Monitor) reconnect() bool {
	times := 0
	for {
		if m.quit_flag {
			log.Printf("[ERROR]close redis connection, monitor will exit")
			return false
		}

		wait := times
		times = times + 1

		if wait > 30 {
			wait = 30
		}
		log.Printf("try to reconnect monitor, times:%d, wait:%d", times, wait)
		time.Sleep(time.Duration(wait) * time.Second)

		err := m.cli.ReConnect()
		if err != nil {
			log.Printf("[ERROR]reconnect monitor failed:%v", err)
			continue
		}
		err = m.subscribe()
		if err != nil {
			log.Printf("[ERROR]subscribe monitor failed:%v", err)
			continue
		} else {
			break
		}
	}
	return true
}

// In a endless loop, wait for the redis's notify,
// then put the key to the sync queue, storemanager will use it.
func (m *Monitor) Start(queue chan string) {
	err := m.cli.Connect()
	if err != nil {
		log.Panicf("start monitor failed:%v", err)
	}
	err = m.subscribe()
	if err != nil {
		log.Panicf("start monitor failed:%v", err)
	}
	log.Printf("start monitor succeed")

	for {
		resp, err := m.cli.ReadResponse()
		if err != nil {
			log.Printf("[ERROR]recv message failed, try to reconnect to redis:%v", err)
			if m.reconnect() {
				continue
			} else {
				close(queue)
				break
			}
		}
		if data, ok := resp.([]string); ok {
			if len(data) != 3 || data[0] != "message" {
				log.Printf("[ERROR]receive unexpected message, %v", data)
			} else {
				event := data[1]
				key := data[2]
				log.Printf("receive [%s], value[%s]", event, key)
				queue <- key

				qlen := len(queue)
				if qlen > m.qlen {
					log.Printf("[ERROR]queue grow, current length:%d", qlen)
				}
				m.qlen = qlen
			}
		} else {
			log.Printf("[ERROR]receive unexpected message, %v", resp)
		}
	}
	m.quit_chan <- 1
}

func (m *Monitor) Stop() {
	m.quit_flag = true
	if m.cli != nil {
		m.cli.Close()
	}
	<-m.quit_chan
}

// Monitor use to subscribe redis
// It will put the to sync data to sync_queue
func NewMonitor() *Monitor {
	cli := redis.NewRedis(setting.Redis.Host, setting.Redis.Password, setting.Redis.Db)
	event := fmt.Sprintf("__keyevent@%d__:%s", setting.Redis.Db, setting.Redis.Event)
	return &Monitor{cli, setting.Redis.NotificationConfig, event, 0, false, make(chan int)}
}
