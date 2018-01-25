package awsdynamodb

import (
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoClient struct {
	Dynamo *dynamodb.DynamoDB
}

func (c DynamoClient) Query(h MyHandler, partitionKeys, sortKeys []string) (map[string]Id, error) {
	pkv := partitionKeys[0]
	domain := spidPattern.FindStringSubmatch(pkv)[1]
	id := suuPattern.FindStringSubmatch(pkv)[1]
	response := map[string]Id{domain: {Id: id}}

	expr := "#P1 = :V1"
	qin := dynamodb.QueryInput{
		TableName:                 &h.Table,
		ExpressionAttributeNames:  map[string]*string{"#P1": &h.PartitionKeyName},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":V1": {S: &pkv}},
		KeyConditionExpression:    &expr,
	}
	if out, err := c.Dynamo.Query(&qin); err != nil {
		return nil, errors.Wrap(err, "Error in Query")
	} else {
		for _, e := range out.Items {
			domain, id := h.responseTransform(e)
			response[domain] = id
		}
	}
	return response, nil
}

func (c DynamoClient) BatchGetItem(h MyHandler, partitionKeys, sortKeys []string) (map[string]Id, error) {
	pkv := partitionKeys[0]
	domain := spidPattern.FindStringSubmatch(pkv)[1]
	id := suuPattern.FindStringSubmatch(pkv)[1]
	response := map[string]Id{domain: {Id: id}}

	keyAttributes := make([]map[string]*dynamodb.AttributeValue, len(sortKeys))
	for i, sk := range sortKeys {
		keyAttributes[i] = map[string]*dynamodb.AttributeValue{
			h.PartitionKeyName: {S: &pkv},
			h.SortKeyName:      {S: &sk},
		}
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
			if result, err := c.Dynamo.BatchGetItem(&itemsSpec); err != nil {
				return nil, errors.Wrap(err, "error in BatchGetItem")

			} else {
				r := result.Responses[h.Table]
				for _, e := range r {
					domain, id := h.responseTransform(e)
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
	return response, nil
}
