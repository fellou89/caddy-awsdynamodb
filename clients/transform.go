package awsdynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"regexp"
)

type Id struct {
	Id        string `json:"id"`
	TimeStamp string `json:"timestamp"`
}

var dpidPattern = regexp.MustCompile("dpid=(.*)")
var duuPattern = regexp.MustCompile("duu=(.*)")

func responseTransform(m map[string]*dynamodb.AttributeValue, sortKeyName string) (string, Id) {
	var ts, domain, id string
	for k, v := range m {
		switch k {
		case "timestamp":
			ts = *v.S
		case "value":
			p := duuPattern.FindStringSubmatch(*v.S)
			id = p[1]
		case sortKeyName:
			p := dpidPattern.FindStringSubmatch(*v.S)
			domain = p[1]
		}
	}
	return domain, Id{id, ts}
}

var spidPattern = regexp.MustCompile(".*,spid=(.*),.*")
var suuPattern = regexp.MustCompile(",.*suu=(.*)")
