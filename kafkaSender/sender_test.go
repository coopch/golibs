package kafkaSender

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewSenderAndShutdown(t *testing.T) {
	buf := &bytes.Buffer{}
	mb1 := setupMockBroker(t, 1)
	defer mb1.Close()

	s, err := NewSender([]string{mb1.Addr()}, buf, "NewTestSender", nil)
	assert.NoError(t, err)
	assert.NotNil(t, s)

	err = s.Shutdown()
	assert.NoError(t, err)
}

func TestNewInvalidSetup(t *testing.T) {
	buf := &bytes.Buffer{}
	s, err := NewSender(nil, buf, "NewTestSender", nil)
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestSendBytesSuccess(t *testing.T) {
	buf := &bytes.Buffer{}

	conf := mocks.NewTestConfig()
	conf.Producer.Return.Successes = true

	ap := mocks.NewAsyncProducer(t, conf)
	ap.ExpectInputAndSucceed()

	s := &Sender{log: buf, producer: ap}
	go s.handleSuccesses(s.producer.Successes())
	go s.handleErrors(s.producer.Errors())

	data := []byte("Hello World")
	s.SendBytes("Test", data)

	time.Sleep(10 * time.Millisecond)
	assert.Contains(t, buf.String(), "[KAFKA SUCCESS]")
}

func TestSendBytesFail(t *testing.T) {
	buf := &bytes.Buffer{}

	conf := mocks.NewTestConfig()
	ap := mocks.NewAsyncProducer(t, conf)
	ap.ExpectInputAndFail(errors.New("Test Error"))

	s := &Sender{log: buf, producer: ap}
	go s.handleErrors(s.producer.Errors())

	data := []byte("Hello World")
	s.SendBytes("Test", data)

	time.Sleep(10 * time.Millisecond)
	assert.Contains(t, buf.String(), "[KAFKA ERROR]")
	assert.Contains(t, buf.String(), "Test Error")
	assert.Contains(t, buf.String(), "Hello World")
}

func setupMockBroker(t *testing.T, id int) *sarama.MockBroker {
	broker := sarama.NewMockBroker(t, int32(id))
	broker.SetHandlerByMap(
		map[string]sarama.MockResponse{
			"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()), // SetLeader(cp.Topic(), cp.Partition(), broker.BrokerID()),
			"ProduceRequest":  sarama.NewMockProduceResponse(t),
		},
	)
	return broker
}
