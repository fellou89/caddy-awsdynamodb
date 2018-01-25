package awsdynamodb

import (
	"encoding/json"
	"net/http"

	. "github.com/fellou89/caddy-awscloudwatch"
	"github.com/mholt/caddy/caddyhttp/httpserver"
	"github.com/pkg/errors"

	transform "github.com/fellou89/caddy-awsdynamodb/clients"
)

type DBC interface {
	BatchGetItem(table, partitionKeyName, sortKeyName string, partitionKeys, sortKeys []string) (map[string]transform.Id, error)
	Query(table, partitionKeyName, sortKeyName string, partitionKeys, sortKeys []string) (map[string]transform.Id, error)
}

type MyHandler struct {
	DBConnection     DBC
	RequestID        string
	Table            string
	PartitionKeyName string
	SortKeyName      string
	Next             httpserver.Handler
}

func (h MyHandler) GetIds(w http.ResponseWriter, r *http.Request) (int, error) {

	r.ParseForm()
	params := r.Form

	partitionKeys := params["partitionkeys"]
	sortKeys := params["sortkeys"]

	if resp, err := h.Fetch(partitionKeys, sortKeys); err != nil {
		return 500, errors.Wrap(err, "Error fetching records")

	} else {
		if bb, err := json.Marshal(resp); err != nil {
			return 500, errors.Wrap(err, "Error generating json")

		} else {
			LoggerInstance.Info(string(bb))
			w.Header()["Content-Type"] = []string{"application/json"}
			w.Write(bb)
		}
	}
	return 200, nil
}

func (h MyHandler) Fetch(partitionKeys, sortKeys []string) (interface{}, error) {
	var response map[string]transform.Id
	var err error

	if len(sortKeys) > 1 {
		if response, err = h.DBConnection.BatchGetItem(h.Table, h.PartitionKeyName, h.SortKeyName, partitionKeys, sortKeys); err != nil {
			return nil, err
		}

	} else {
		if response, err = h.DBConnection.Query(h.Table, h.PartitionKeyName, h.SortKeyName, partitionKeys, sortKeys); err != nil {
			return nil, err
		}
	}

	return response, nil
}
