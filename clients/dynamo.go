package awsdynamodb

import (
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	maxReadItemsPerBatch = 100
)

type DynamoClient struct {
	Dynamo *dynamodb.DynamoDB
}

func (c DynamoClient) Query(table, partitionKeyName, sortKeyName string, partitionKeys, sortKeys []string) (map[string]Id, error) {
	pkv := partitionKeys[0]
	domain := spidPattern.FindStringSubmatch(pkv)[1]
	id := suuPattern.FindStringSubmatch(pkv)[1]
	response := map[string]Id{domain: {Id: id}}

	expr := "#P1 = :V1"
	qin := dynamodb.QueryInput{
		TableName:                 &table,
		ExpressionAttributeNames:  map[string]*string{"#P1": &partitionKeyName},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":V1": {S: &pkv}},
		KeyConditionExpression:    &expr,
	}
	if out, err := c.Dynamo.Query(&qin); err != nil {
		return nil, errors.Wrap(err, "Error in Query")
	} else {
		for _, e := range out.Items {
			domain, id := responseTransform(e, sortKeyName)
			response[domain] = id
		}
	}
	return response, nil
}

func (c DynamoClient) BatchGetItem(table, partitionKeyName, sortKeyName string, partitionKeys, sortKeys []string) (map[string]Id, error) {
	pkv := partitionKeys[0]
	domain := spidPattern.FindStringSubmatch(pkv)[1]
	id := suuPattern.FindStringSubmatch(pkv)[1]
	response := map[string]Id{domain: {Id: id}}

	keyAttributes := make([]map[string]*dynamodb.AttributeValue, len(sortKeys))
	for i, sk := range sortKeys {
		keyAttributes[i] = map[string]*dynamodb.AttributeValue{
			partitionKeyName: {S: &pkv},
			sortKeyName:      {S: &sk},
		}
	}

	itemsSpec := dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{table: {Keys: keyAttributes}},
	}

	for len(keyAttributes) > 0 {
		n := maxReadItemsPerBatch
		if len(keyAttributes) < n {
			n = len(keyAttributes)
		}

		itemsSpec.RequestItems[table].Keys = keyAttributes[:n]
		keyAttributes = keyAttributes[n:]
		for {
			if result, err := c.Dynamo.BatchGetItem(&itemsSpec); err != nil {
				return nil, errors.Wrap(err, "error in BatchGetItem")

			} else {
				r := result.Responses[table]
				for _, e := range r {
					domain, id := responseTransform(e, sortKeyName)
					response[domain] = id
				}
				if result.UnprocessedKeys != nil {
					if unprocessed, ok := result.UnprocessedKeys[table]; ok {
						unprocessedKeys := unprocessed.Keys
						if len(unprocessedKeys) != 0 {
							itemsSpec.RequestItems[table].Keys = unprocessedKeys
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
