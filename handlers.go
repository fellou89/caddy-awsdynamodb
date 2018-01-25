package awsdynamodb

import (
	"encoding/json"
	"net/http"
	"regexp"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	. "github.com/fellou89/caddy-awscloudwatch"
	"github.com/mholt/caddy/caddyhttp/httpserver"
	"github.com/pkg/errors"
)

const (
	maxReadItemsPerBatch = 100
)

type Id struct {
	Id        string `json:"id"`
	TimeStamp string `json:"timestamp"`
}

type DBC interface {
	BatchGetItem(h MyHandler, partitionKeys, sortKeys []string) (map[string]Id, error)
	Query(h MyHandler, partitionKeys, sortKeys []string) (map[string]Id, error)
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
	var response map[string]Id
	var err error

	if len(sortKeys) > 1 {
		if response, err = h.DBConnection.BatchGetItem(h, partitionKeys, sortKeys); err != nil {
			return nil, err
		}

	} else {
		if response, err = h.DBConnection.Query(h, partitionKeys, sortKeys); err != nil {
			return nil, err
		}
	}

	return response, nil
}

var dpidPattern = regexp.MustCompile("dpid=(.*)")
var duuPattern = regexp.MustCompile("duu=(.*)")

func (h MyHandler) responseTransform(m map[string]*dynamodb.AttributeValue) (string, Id) {
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

var spidPattern = regexp.MustCompile(".*,spid=(.*),.*")
var suuPattern = regexp.MustCompile(",.*suu=(.*)")
