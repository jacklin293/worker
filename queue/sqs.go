package queue

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const (
	defaultWaitTimeSeconds = 20
)

// Config
type sqsConfig struct {
	// e.g. https://sqs.us-east-1.amazonaws.com/4**********7/queue_name
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

type SQS struct {
	service             *sqs.SQS
	config              *sqsConfig
	receiveMessageInput *sqs.ReceiveMessageInput
}

func (c *sqsConfig) validate() error {
	if c.QueueUrl == "" {
		return errors.New("queue_url can't be empty")
	}
	if c.Region == "" {
		return errors.New("region can't be empty")
	}
	if c.MaxNumberOfMessages < 0 || c.MaxNumberOfMessages > 10 {
		return errors.New("max_number_of_messages can only be 1 to 10")
	}
	return nil
}

func (c *sqsConfig) New() (QueueContainer, error) {
	var endpoint string
	var err error
	if c.UseLocalSqs {
		// Remove the last slash
		endpoint, err = c.getEndpoint()
		if err != nil {
			return nil, err
		}
	}
	session, err := newAwsSession(c.Region, c.CredentialFilename, c.CredentialProfile, endpoint)
	if err != nil {
		return nil, err
	}

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

	q := &SQS{
		service:             sqs.New(session),
		config:              c,
		receiveMessageInput: recInput,
	}
	return q, err
}

func (c *sqsConfig) getEndpoint() (endpoint string, err error) {
	u, err := url.Parse(c.QueueUrl)
	if err != nil {
		return
	}
	endpoint = u.Scheme + "://" + u.Host
	return
}

func (s *SQS) Send(msgs interface{}) (interface{}, error) {
	entries := make([]*sqs.SendMessageBatchRequestEntry, len(msgs.([][]byte)))
	for i, msg := range msgs.([][]byte) {
		e := sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("Message-ID-%d", i)),
			MessageBody: aws.String(string(msg)),
		}
		entries[i] = &e
	}
	param := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(s.config.QueueUrl),
	}
	return s.service.SendMessageBatch(param)
}

func (s *SQS) Receive() (interface{}, error) {
	resp, err := s.service.ReceiveMessage(s.receiveMessageInput)
	if err != nil {
		return nil, errors.New("Failed to receive messages from SQS. " + err.Error())
	}
	messages := make([][]byte, 0, len(resp.Messages))
	receipts := make([]string, 0, len(resp.Messages))
	for _, msg := range resp.Messages {
		// TODO Create dedicated type for sqs message, a struct contains 2 field - message, receipt
		// TODO Return this type here
		// TODO fetcher.poll() should pass message and receipt to newMessage in dispatch()
		// TODO Add fields queueType and receipt to Message, assign them during newMessage
		// TODO msg <-h.doneMessageCh
		// TODO h.workers[msg.queueName].queue.Delete([]string{msg.receipt})
		messages = append(messages, []byte(*msg.Body))
		receipts = append(receipts, *msg.ReceiptHandle)
	}

	// TODO Put Delete() into done() in handler
	if len(receipts) > 0 {
		_, err = s.Delete(receipts)
	}
	return messages, err
}

// TODO Use DeleteMessage(
func (s *SQS) Delete(receipts []string) (*sqs.DeleteMessageBatchOutput, error) {
	entries := make([]*sqs.DeleteMessageBatchRequestEntry, len(receipts))
	for i, receipt := range receipts {
		e := sqs.DeleteMessageBatchRequestEntry{
			Id:            aws.String(fmt.Sprintf("Message-ID-%d", i)),
			ReceiptHandle: aws.String(receipt),
		}
		entries[i] = &e
	}
	param := &sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(s.config.QueueUrl),
	}
	return s.service.DeleteMessageBatch(param)
}

func newAwsSession(region string, filename string, profile string, endpoint string) (*session.Session, error) {
	s, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	config := aws.NewConfig()
	var ProviderList []credentials.Provider = []credentials.Provider{
		&credentials.EnvProvider{},
		&ec2rolecreds.EC2RoleProvider{
			Client: ec2metadata.New(s, config),
		},
		&credentials.SharedCredentialsProvider{
			Filename: filename,
			Profile:  profile,
		},
	}
	cred := credentials.NewChainCredentials(ProviderList)
	config.WithCredentials(cred)
	config.WithRegion(region)

	if endpoint != "" {
		config.WithEndpoint(endpoint)
	}
	return session.NewSession(config)
}
