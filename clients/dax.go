package awsdynamodb

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"strings"
)

type DaxClient struct {
	Endpoint string
}

func (c DaxClient) Query(table, partitionKeyName, sortKeyName string, partitionKeys, sortKeys []string) (map[string]Id, error) {
	if len(sortKeys) == 0 {
		return nil, errors.New("DAX needs sort-key for request")
	}

	pkv := partitionKeys[0]
	domain := spidPattern.FindStringSubmatch(pkv)[1]
	id := suuPattern.FindStringSubmatch(pkv)[1]
	response := map[string]Id{domain: {Id: id}}

	req := c.Endpoint + "?pkv=" + pkv + "&skv=" + sortKeys[0]

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

func (c DaxClient) BatchGetItem(table, partitionKeyName, sortKeyName string, partitionKeys, sortKeys []string) (map[string]Id, error) {
	pkv := partitionKeys[0]
	domain := spidPattern.FindStringSubmatch(pkv)[1]
	id := suuPattern.FindStringSubmatch(pkv)[1]
	response := map[string]Id{domain: {Id: id}}

	req := c.Endpoint + "?pkv=" + pkv + "&skv="
	req += strings.Join(sortKeys, ",")

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
				tableResponses := result["Responses"][table]
				if len(tableResponses) > 0 {

					for _, r := range tableResponses {
						domain := dpidPattern.FindStringSubmatch(r[sortKeyName]["S"])[1]
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
