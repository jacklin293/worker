package source

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const (
	defaultWaitTimeSeconds = 20
)

// Implementation
type sqsConfig struct {
	// e.g. https://sqs.us-west-1.amazonaws.com/4**********7/queue_name
	QueueUrl string `json:"queue_url"`

	UseLocalSqs        bool   `json:"use_local_sqs"`
	Region             string `json:"region"`
	CredentialFilename string `json:"credential_filename"`
	CredentialProfile  string `json:"credential_profile"`

	// Receive
	MaxNumberOfMessages int64 `json:"max_number_of_messages"`
	VisibilityTimeout   int64 `json:"visibility_timeout"`
	WaitTimeSeconds     int64 `json:"wait_time_seconds"`
}

// Config
type SQS struct {
	service             *sqs.SQS
	config              *sqsConfig
	receiveMessageInput *sqs.ReceiveMessageInput
}

func (c *sqsConfig) validate() error {
	return nil
}

func (c *sqsConfig) New() Sourcer {
	var endpoint string
	if c.UseLocalSqs {
		// Remove the last slash
		endpoint = c.QueueUrl
	}
	session := newAwsSession(c.Region, c.CredentialFilename, c.CredentialProfile, endpoint)

	// New ReceiveMessageInput
	recInput := &sqs.ReceiveMessageInput{}
	recInput.SetQueueUrl(c.QueueUrl)
	if c.MaxNumberOfMessages != 0 {
		recInput.SetMaxNumberOfMessages(c.MaxNumberOfMessages)
	}
	if c.VisibilityTimeout != 0 {
		recInput.SetVisibilityTimeout(c.VisibilityTimeout)
	}
	if c.WaitTimeSeconds == 0 {
		recInput.SetWaitTimeSeconds(defaultWaitTimeSeconds)
	} else {
		recInput.SetWaitTimeSeconds(c.WaitTimeSeconds)
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
		QueueUrl:    aws.String(s.config.QueueUrl),
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
		QueueUrl:      aws.String(s.config.QueueUrl),
		ReceiptHandle: aws.String(receiptHandle),
	}
	return s.service.DeleteMessage(param)
}
