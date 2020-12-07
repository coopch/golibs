package kafkaSender

import (
	"io"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

type Sender struct {
	log      io.Writer
	producer sarama.AsyncProducer
}

// NewSender creates a new KafkaSender that's ready to go.
// - `brokers` is a slice of Kafka brokers in the form of "servername:port", e.g. "localhost:9092"
// - `out` is the output stream used for logging
// - `clientID` is used to identify the sender with the broker
// - `config` is a `*sarama.Config` and can be `nil`. If `nil`, DefaultConfig will be called to provide the configuration.
func NewSender(brokers []string, out io.Writer, clientID string, config *sarama.Config) (*Sender, error) {
	sarama.Logger = log.New(out, "[sarama] ", log.Lmicroseconds)
	if config == nil {
		config = DefaultConfig()
	}
	config.ClientID = clientID

	aprod, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	sender := &Sender{
		log:      out,
		producer: aprod,
	}

	go sender.handleSuccesses(sender.producer.Successes())
	go sender.handleErrors(sender.producer.Errors())

	return sender, nil
}

func DefaultConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	config.ChannelBufferSize = 10000

	config.Producer.Return.Successes = false

	return config
}

// Shutdown the producer
func (s *Sender) Shutdown() error {
	l := log.New(s.log, "[kafka.sender] ", log.Lmicroseconds)
	l.Print("Received shutdown. Attepmting to close producer.")
	err := s.producer.Close()
	l.Printf("Producer closed with error: %v", err)
	return err
}

func (s *Sender) handleSuccesses(msgChan <-chan *sarama.ProducerMessage) {
	out := log.New(s.log, "[KAFKA SUCCESS] ", log.Lmicroseconds)
	for msg := range msgChan {
		out.Printf("topic %s, partition %d, offset %d,", msg.Topic, msg.Partition, msg.Offset)
	}
}

func (s *Sender) handleErrors(errChan <-chan *sarama.ProducerError) {
	out := log.New(s.log, "[KAFKA ERROR] ", log.Lmicroseconds)
	for err := range errChan {
		body, _ := err.Msg.Value.Encode()
		out.Printf("[%s] %s", err.Error(), string(body))
	}
}

// SendBytes sends []byte data to kafka topic. Be aware that data is a pointer and sent asyncronously.
// If data is modified by your program, you might run into race conditions!
func (s *Sender) SendBytes(topic string, data []byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	s.producer.Input() <- msg
}
