# KafkaSender

## Usage

```go
package main

import (
	"log"
	"os"

	"github.com/coopch/golibs/kafkaSender"
)

func main() {
    // initialize
	brokers := []string{"kafkaserver1:9020", "kafkaserver2:9020", "kafkaserver3:9020"}
	conf := kafkaSender.DefaultConfig()
	    // default config overrides here
        // e.g. conf.ChannelBufferSize = 10
    out := os.Stdout
    clientID := "ExampleSender"

    // create a new sender, 
	ks, err := kafkaSender.NewSender(brokers, out, clientID, conf)
	if err != nil {
		log.Fatal(err)
	}
	// Flush buffer and Close sender when `main()` ends.
	defer ks.Shutdown()

    // send message to kafka
	data := []byte(`{ "message": "Hello World" }`)
	ks.SendBytes(data)
}
```
