package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"gopkg.in/redis.v5"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
)

// Default Values
const (
	redis_host          = "localhost"
	redis_port          = "6379"
	redis_channel       = "mappings_channel"
	redis_key           = "mappings_key"
	file_path           = "/mappings.map"
	haproxy_socket_path = "/var/run/haproxy.sock"
)

var filePtr, redisHostPtr, redisPortPtr, redisChannelPtr, redisKeyPtr, haproxySockPtr *string

func init() {

	// User Options to change config

	filePtr = flag.String("iplistpath", file_path, "Specify the file path of blacklisted_ips list")
	logPtr := flag.String("logpath", "", "Specify the logging file path")
	redisHostPtr = flag.String("redishost", redis_host, "Specify the redis endpoint")
	redisPortPtr = flag.String("redisport", redis_port, "Specify the redis port")
	redisChannelPtr = flag.String("redischannel", redis_channel, "Specify the redis channel to Subscribe")
	redisKeyPtr = flag.String("rediskey", redis_key, "Specify the redis key for blacklisted ip members")
	haproxySockPtr = flag.String("haproxysock", haproxy_socket_path, "Specify the unix socket path of haproxy")

	flag.Parse()

	// Logging
	if *logPtr != "" {
		f, err := os.OpenFile(*logPtr, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		check(err)
		log.SetOutput(f)
	} else {
		log.SetOutput(os.Stdout)
	}

}

func check(e error) {
	if e != nil {
		log.Printf("error")
		log.Fatal(e)
	}
}

func remove_map_entry(key string) {
	temp_file_path := "files/temp.map"
	f, err := os.Create(temp_file_path)
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)

	if file, err := os.Open(*filePtr); err == nil {

		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			if strings.Split(scanner.Text(), " ")[0] != key {
				log.Printf("text: %v, key: %v", scanner.Text(), key)
				w.WriteString(scanner.Text() + "\n")
			}
		}
		w.Flush()

		err := os.Rename(temp_file_path, *filePtr)
		check(err)

		err = scanner.Err()
		check(err)

	} else {
		check(err)
	}

}

func add_map_via_socket(key, value string) {
	c, err1 := net.Dial("unix", *haproxySockPtr)
	check(err1)
	defer c.Close()
	cmd := "add map " + *filePtr + " " + key + " " + value
	_, err2 := c.Write([]byte(cmd + "\n"))
	check(err2)
}

func del_map_via_socket(key string) {
	c, err1 := net.Dial("unix", *haproxySockPtr)
	check(err1)
	defer c.Close()
	cmd := "del map " + *filePtr + " " + key
	_, err2 := c.Write([]byte(cmd + "\n"))
	check(err2)
}

func add_map_entry(key, value string) {
	f, err := os.OpenFile(*filePtr, os.O_APPEND|os.O_WRONLY, 0600)
	check(err)
	defer f.Close()
	f.WriteString(key + " " + value + "\n")

}

func add_mappings_at_startup(route_maps map[string]string, filePtr string) {

	f, err := os.Create(filePtr)
	check(err)
	w := bufio.NewWriter(f)
	value := ""
	for k, v := range route_maps {
		log.Printf("Adding entry " + k + " " + v)
		value += k + " " + v + "\n"
	}
	w.WriteString(value)
	w.Flush()

}

func main() {

	log.Printf("Main")
	//Redis Client
	client := redis.NewClient(&redis.Options{
		Addr:     *redisHostPtr + ":" + *redisPortPtr,
		Password: "",
		DB:       0,
	})

	type Message struct {
		Key, Value, Action string
	}

	//Add IP to haproxy at startup
	route_maps, err := client.HGetAll(*redisKeyPtr).Result()
	check(err)
	if len(route_maps) != 0 {
		fmt.Println("keys: ", route_maps)
		add_mappings_at_startup(route_maps, *filePtr)
	}

	// Trigger a haproxy reload
	cmd := "service haproxy reload"
	out, err := exec.Command("sh", "-c", cmd).Output()
	check(err)
	log.Printf("\nOUT:%s\n", out)

	// Redis - PubSub
	pubsub, err := client.Subscribe(*redisChannelPtr)
	check(err)
	defer pubsub.Close()
	for {
		msgi, err := pubsub.ReceiveMessage()

		if err != nil {
			log.Fatal(err)
			break
		}

		// JSON from the client -- Key, Value & Action
		dec := json.NewDecoder(strings.NewReader(msgi.Payload))
		var m Message
		dec.Decode(&m)

		m.Key = strings.TrimSpace(m.Key)
		m.Value = strings.TrimSpace(m.Value)

		if m.Action == "remove" {
			currVal := client.HGet(*redisKeyPtr, m.Key)
			log.Println(currVal)
			if currVal != nil {
				// 1. Remove it from json file, 2. Remove it from redis key, 3. del map
				remove_map_entry(m.Key)
				err := client.HDel(*redisKeyPtr, m.Key).Err()
				check(err)
				del_map_via_socket(m.Key)
				log.Println("Removed ", m.Key, " from the ** map")
			} else {
				log.Println("Entry not found in map. Dont worry!")
			}
		} else if m.Action == "add" {
			currVal := client.HGet(*redisKeyPtr, m.Key)
			if currVal != nil {
				// 1. Add it to lst file, 2. Add it to redis key, 3. add map
				add_map_entry(m.Key, m.Value)
				err := client.HSet(*redisKeyPtr, m.Key, m.Value).Err()
				check(err)
				add_map_via_socket(m.Key, m.Value)
				log.Println("Added ", m.Key, ": ", m.Value, " to the redirect map")
			} else {
				log.Println("Same map entry found already")
			}
		} else {
			log.Println("Wrong action name ", m.Action)
		}
	}

}
