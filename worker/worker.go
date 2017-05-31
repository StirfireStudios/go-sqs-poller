package worker

import (
	"fmt"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// HandlerFunc is used to define the Handler that is run on for each message
type HandlerFunc func(msg *sqs.Message) error

type Config struct {
	QueueURL string
	MaxNumberOfMessage int64
	WaitTimeSecond int64
	Log LoggerIFace
}

func (f HandlerFunc) HandleMessage(msg *sqs.Message) error {
	return f(msg)
}

// Handler interface
type Handler interface {
	HandleMessage(msg *sqs.Message) error
}

type InvalidEventError struct {
	event string
	msg   string
}

func (e InvalidEventError) Error() string {
	return fmt.Sprintf("[Invalid Event: %s] %s", e.event, e.msg)
}

func NewInvalidEventError(event, msg string) InvalidEventError {
	return InvalidEventError{event: event, msg: msg}
}

func DefaultConfigForQueueURL(url string) Config {
	var config = Config {
		QueueURL: url,
		MaxNumberOfMessage: 10,
		WaitTimeSecond: 20,
		Log: &logger{},
	}
	return config
}

// Exported Variables
var (
	DefaultConfig Config = Config{
		// what is the queue url we are connecting to, Defaults to empty
		QueueURL: "",
		// The maximum number of messages to return. Amazon SQS never returns more messages
		// than this value (however, fewer messages might be returned). Valid values
		// are 1 to 10. Default is 10.
		MaxNumberOfMessage: 10,
		// The duration (in seconds) for which the call waits for a message to arrive
		// in the queue before returning. If a message is available, the call returns
		// sooner than WaitTimeSeconds.
		WaitTimeSecond: 20,
		Log: &logger{},
	}
)

// Start starts the polling and will continue polling till the application is forcibly stopped
func Start(config *Config, svc *sqs.SQS, h Handler) {
	for {
		if config == nil {
			config = &DefaultConfig
		}
		config.Log.Debug("worker: Start Polling")
		params := &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(config.QueueURL), // Required
			MaxNumberOfMessages: aws.Int64(config.MaxNumberOfMessage),
			MessageAttributeNames: []*string{
				aws.String("All"), // Required
			},
			WaitTimeSeconds: aws.Int64(config.WaitTimeSecond),
		}

		resp, err := svc.ReceiveMessage(params)
		if err != nil {
			log.Println(err)
			continue
		}
		if len(resp.Messages) > 0 {
			run(config, svc, h, resp.Messages)
		}
	}
}

// poll launches goroutine per received message and wait for all message to be processed
func run(config *Config, svc *sqs.SQS, h Handler, messages []*sqs.Message) {
	numMessages := len(messages)
	config.Log.Info(fmt.Sprintf("worker: Received %d messages", numMessages))

	var wg sync.WaitGroup
	wg.Add(numMessages)
	for i := range messages {
		go func(m *sqs.Message) {
			// launch goroutine
			defer wg.Done()
			if err := handleMessage(config, svc, m, h); err != nil {
				config.Log.Error(err.Error())
			}
		}(messages[i])
	}

	wg.Wait()
}

func handleMessage(config *Config, svc *sqs.SQS, m *sqs.Message, h Handler) error {
	var err error
	err = h.HandleMessage(m)
	if _, ok := err.(InvalidEventError); ok {
		config.Log.Error(err.Error())
	} else if err != nil {
		return err
	}

	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(config.QueueURL), // Required
		ReceiptHandle: m.ReceiptHandle,      // Required
	}
	_, err = svc.DeleteMessage(params)
	if err != nil {
		return err
	}
	config.Log.Debug(fmt.Sprintf("worker: deleted message from queue: %s", aws.StringValue(m.ReceiptHandle)))

	return nil
}
