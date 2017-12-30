package awsdynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
	"regexp"
)

const (
	maxReadItemsPerBatch = 100
	partitionKeyName     = "partition-key"
	sortKeyName          = "sort-key"
)

type Id struct {
	Id        string `json:"id"`
	TimeStamp string `json:"timestamp"`
}

func Fetch(db dynamodb.DynamoDB, cid, entitytype, domain, id string, targetDomains []string) (interface{}, error) {
	var tableName = "aqfer-idsync" // We have hard-coded this here, but it should be read from the config file.

	partitionKey := "cid=" + cid + ",spid=" + domain + ",suu=" + id

	response := map[string]Id{domain: {Id: id}}
	if len(targetDomains) != 0 {
		var sortKeys = make([]string, len(targetDomains))
		for i, t := range targetDomains {
			sortKeys[i] = t
		}
		keyAttributes := make([]map[string]*dynamodb.AttributeValue, len(sortKeys))
		for i, v := range sortKeys {
			sk := "dpid=" + v
			keyAttributes[i] = map[string]*dynamodb.AttributeValue{partitionKeyName: {S: &partitionKey},
				sortKeyName: {S: &sk}}
		}
		itemsSpec := dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{tableName: {Keys: keyAttributes}},
		}
		for len(keyAttributes) > 0 {
			n := maxReadItemsPerBatch
			if len(keyAttributes) < n {
				n = len(keyAttributes)
			}

			itemsSpec.RequestItems[tableName].Keys = keyAttributes[:n]
			keyAttributes = keyAttributes[n:]
			for {
				if result, err := db.BatchGetItem(&itemsSpec); err != nil {
					return nil, errors.Wrap(err, "error in BatchGetItem")

				} else {
					r := result.Responses[tableName]
					for _, e := range r {
						domain, id := transform(e)
						response[domain] = id
					}
					if result.UnprocessedKeys != nil {
						if unprocessed, ok := result.UnprocessedKeys[tableName]; ok {
							unprocessedKeys := unprocessed.Keys
							if len(unprocessedKeys) != 0 {
								itemsSpec.RequestItems[tableName].Keys = unprocessedKeys
								continue
							}
						}
					}
					break

				}
			}
		}

	} else {
		pk := partitionKeyName
		expr := "#P1 = :V1"
		qin := dynamodb.QueryInput{
			TableName:                 &tableName,
			ExpressionAttributeNames:  map[string]*string{"#P1": &pk},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":V1": {S: &partitionKey}},
			KeyConditionExpression:    &expr,
		}

		if out, err := db.Query(&qin); err != nil {
			return nil, errors.Wrap(err, "Error in Query")

		} else {
			for _, e := range out.Items {
				domain, id := transform(e)
				response[domain] = id
			}
		}
	}
	return response, nil
}

var dpidPattern = regexp.MustCompile("dpid=(.*)")
var duuPattern = regexp.MustCompile("duu=(.*)")

func transform(m map[string]*dynamodb.AttributeValue) (string, Id) {
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
