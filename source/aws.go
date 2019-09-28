package source

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
)

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
