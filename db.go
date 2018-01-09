package awsdynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
	"regexp"
)

const (
	maxReadItemsPerBatch = 100
)

type Id struct {
	Id        string `json:"id"`
	TimeStamp string `json:"timestamp"`
}

func (h MyHandler) Fetch(cid, entitytype, domain, id string, targetDomains []string) (interface{}, error) {
	partitionKeyValue := "cid=" + cid + ",spid=" + domain + ",suu=" + id

	response := map[string]Id{domain: {Id: id}}
	if len(targetDomains) != 0 {
		var sortKeys = make([]string, len(targetDomains))
		for i, t := range targetDomains {
			sortKeys[i] = t
		}
		keyAttributes := make([]map[string]*dynamodb.AttributeValue, len(sortKeys))
		for i, v := range sortKeys {
			sk := "dpid=" + v
			keyAttributes[i] = map[string]*dynamodb.AttributeValue{h.PartitionKeyName: {S: &partitionKeyValue},
				h.SortKeyName: {S: &sk}}
		}
		itemsSpec := dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{h.Table: {Keys: keyAttributes}},
		}
		for len(keyAttributes) > 0 {
			n := maxReadItemsPerBatch
			if len(keyAttributes) < n {
				n = len(keyAttributes)
			}

			itemsSpec.RequestItems[h.Table].Keys = keyAttributes[:n]
			keyAttributes = keyAttributes[n:]
			for {
				if result, err := h.DynamoDB.BatchGetItem(&itemsSpec); err != nil {
					return nil, errors.Wrap(err, "error in BatchGetItem")

				} else {
					r := result.Responses[h.Table]
					for _, e := range r {
						domain, id := h.transform(e)
						response[domain] = id
					}
					if result.UnprocessedKeys != nil {
						if unprocessed, ok := result.UnprocessedKeys[h.Table]; ok {
							unprocessedKeys := unprocessed.Keys
							if len(unprocessedKeys) != 0 {
								itemsSpec.RequestItems[h.Table].Keys = unprocessedKeys
								continue
							}
						}
					}
					break

				}
			}
		}

	} else {
		pk := h.PartitionKeyName
		expr := "#P1 = :V1"
		qin := dynamodb.QueryInput{
			TableName:                 &h.Table,
			ExpressionAttributeNames:  map[string]*string{"#P1": &pk},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":V1": {S: &partitionKeyValue}},
			KeyConditionExpression:    &expr,
		}

		if out, err := h.DynamoDB.Query(&qin); err != nil {
			return nil, errors.Wrap(err, "Error in Query")

		} else {
			for _, e := range out.Items {
				domain, id := h.transform(e)
				response[domain] = id
			}
		}
	}
	return response, nil
}

var dpidPattern = regexp.MustCompile("dpid=(.*)")
var duuPattern = regexp.MustCompile("duu=(.*)")

func (h MyHandler) transform(m map[string]*dynamodb.AttributeValue) (string, Id) {
	var ts, domain, id string
	for k, v := range m {
		switch k {
		case "timestamp":
			ts = *v.S
		case "value":
			p := duuPattern.FindStringSubmatch(*v.S)
			id = p[1]
		case h.SortKeyName:
			p := dpidPattern.FindStringSubmatch(*v.S)
			domain = p[1]
		}
	}
	return domain, Id{id, ts}
}
