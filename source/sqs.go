package source

import (
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const (
	defaultWaitTimeSeconds = 20
)

type SQS struct {
	service             *sqs.SQS
	config              *Config
	receiveMessageInput *sqs.ReceiveMessageInput
}

type SqsConfig struct {
	UseLocalSqs        bool   `json:"use_local_sqs"`
	Region             string `json:"region"`
	CredentialFilename string `json:"credential_filename"`
	CredentialProfile  string `json:"credential_profile"`

	// Receive
	MaxNumberOfMessages int64 `json:"max_number_of_messages"`
	VisibilityTimeout   int64 `json:"visibility_timeout"`
	WaitTimeSeconds     int64 `json:"wait_time_seconds"`
}

func newSQS(c *Config) *SQS {
	var endpoint string
	if c.Metadata.SQS.UseLocalSqs {
		// Remove the last slash
		endpoint = strings.TrimRight(c.Endpoint, "/")
	}
	session := newAwsSession(c.Metadata.SQS.Region, c.Metadata.SQS.CredentialFilename, c.Metadata.SQS.CredentialProfile, endpoint)

	// New ReceiveMessageInput
	recInput := &sqs.ReceiveMessageInput{}
	recInput.SetQueueUrl(c.Endpoint + c.Topic)
	if c.Metadata.SQS.MaxNumberOfMessages != 0 {
		recInput.SetMaxNumberOfMessages(c.Metadata.SQS.MaxNumberOfMessages)
	}
	if c.Metadata.SQS.VisibilityTimeout != 0 {
		recInput.SetVisibilityTimeout(c.Metadata.SQS.VisibilityTimeout)
	}
	if c.Metadata.SQS.WaitTimeSeconds == 0 {
		recInput.SetWaitTimeSeconds(defaultWaitTimeSeconds)
	} else {
		recInput.SetWaitTimeSeconds(c.Metadata.SQS.WaitTimeSeconds)
	}

	s := &SQS{
		service:             sqs.New(session),
		config:              c,
		receiveMessageInput: recInput,
	}
	return s
}

func (s *SQS) Send(msg []byte) error {
	// New SendMessageInput
	param := &sqs.SendMessageInput{
		QueueUrl:    aws.String(s.config.Endpoint + s.config.Topic),
		MessageBody: aws.String(string(msg)),
	}
	_, err := s.service.SendMessage(param)
	return err
}

func (s *SQS) Receive() (messages [][]byte, err error) {
	resp, err := s.service.ReceiveMessage(s.receiveMessageInput)
	if err != nil {
		return nil, errors.New("Failed to receive messages from SQS. " + err.Error())
	}
	for _, msg := range resp.Messages {
		messages = append(messages, []byte(*msg.Body))
		s.Delete(*msg.ReceiptHandle)
	}
	return
}

func (s *SQS) Delete(receiptHandle string) (*sqs.DeleteMessageOutput, error) {
	param := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.config.Endpoint + s.config.Topic),
		ReceiptHandle: aws.String(receiptHandle),
	}
	return s.service.DeleteMessage(param)
}
