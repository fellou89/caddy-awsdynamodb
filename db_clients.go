package awsdynamodb

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	maxReadItemsPerBatch = 100
)

type Id struct {
	Id        string `json:"id"`
	TimeStamp string `json:"timestamp"`
}

type DBC interface {
	BatchGetItem(h MyHandler, domains []string, cid, domain, id string) (map[string]Id, error)
	Query(h MyHandler, domains []string, cid, domain, id string) (map[string]Id, error)
}

type DynamoClient struct {
	Dynamo *dynamodb.DynamoDB
}

func (c DynamoClient) Query(h MyHandler, domains []string, cid, domain, id string) (map[string]Id, error) {
	pkv := "cid=" + cid + ",spid=" + domain + ",suu=" + id
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
			domain, id := h.transform(e)
			response[domain] = id
		}
	}
	return response, nil
}

func (c DynamoClient) BatchGetItem(h MyHandler, targetDomains []string, cid, domain, id string) (map[string]Id, error) {
	pkv := "cid=" + cid + ",spid=" + domain + ",suu=" + id
	response := map[string]Id{domain: {Id: id}}

	var sortKeys = make([]string, len(targetDomains))
	for i, t := range targetDomains {
		sortKeys[i] = t
	}

	keyAttributes := make([]map[string]*dynamodb.AttributeValue, len(sortKeys))
	for i, v := range sortKeys {
		sk := "dpid=" + v
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
	return response, nil
}

type DaxClient struct {
	Endpoint string
}

func (c DaxClient) Query(h MyHandler, domains []string, cid, domain, id string) (map[string]Id, error) {
	if len(domains) == 0 {
		return nil, errors.New("DAX needs sort-key for request")
	}

	response := map[string]Id{domain: {Id: id}}

	pkv := "cid=" + cid + ",spid=" + domain + ",suu=" + id
	req := c.Endpoint + "?pkv=" + pkv + "&skv=dpid=" + domains[0]

	if out, err := http.Get(req); err != nil {
		return nil, err
	} else {

		if body, err := ioutil.ReadAll(out.Body); err != nil {
			return nil, err
		} else {
			var result map[string]map[string]map[string]string
			if err := json.Unmarshal(body, &result); err != nil {
				return nil, err
			} else {
				if len(result) > 0 {
					sk := result["Item"]["sort-key"]["S"]
					domain := dpidPattern.FindStringSubmatch(sk)[1]

					id := duuPattern.FindStringSubmatch(result["Item"]["value"]["S"])[1]
					ts := result["Item"]["timestamp"]["S"]

					response[domain] = Id{id, ts}
				}
			}
		}
	}
	return response, nil
}

func (c DaxClient) BatchGetItem(h MyHandler, domains []string, cid, domain, id string) (map[string]Id, error) {
	response := map[string]Id{domain: {Id: id}}

	pkv := "cid=" + cid + ",spid=" + domain + ",suu=" + id
	req := c.Endpoint + "?pkv=" + pkv + "&skv="
	for _, domain := range domains {
		req += "dpid=" + domain + ","
	}
	req = req[:len(req)-1]

	if out, err := http.Get(req); err != nil {
		return nil, err
	} else {

		if body, err := ioutil.ReadAll(out.Body); err != nil {
			return nil, err
		} else {
			var result map[string]map[string][]map[string]map[string]string
			if err := json.Unmarshal(body, &result); err != nil {
				return nil, err
			} else {
				tableResponses := result["Responses"][h.Table]
				if len(tableResponses) > 0 {

					for _, r := range tableResponses {
						domain := dpidPattern.FindStringSubmatch(r[h.SortKeyName]["S"])[1]
						id := duuPattern.FindStringSubmatch(r["value"]["S"])[1]
						ts := r["timestamp"]["S"]

						response[domain] = Id{id, ts}
					}
				}
			}
		}
	}
	return response, nil
}
