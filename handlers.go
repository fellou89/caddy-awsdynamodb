package awsdynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"net/http"
	"regexp"
	"strings"
)

func GetIds(dynamoDB *dynamodb.DynamoDB, w http.ResponseWriter, r *http.Request) (int, error) {

	var routeExp = regexp.MustCompile(`ids/v1/domains/([0-9a-z]+)/ids/([0-9a-z]+)`)
	match := routeExp.FindStringSubmatch(r.RequestURI)

	result := make(map[string]string)
	for i, name := range routeExp.SubexpNames() {
		if i != 0 {
			result[name] = match[i]
		}
	}
	domain := match[1]
	id := match[2]

	// vars := mux.Vars(r)
	// domain := vars["domain"]
	// id := vars["id"]

	// cid is the client (i.e. tenant) id. It is hardcoded here, but will be extracted from the security context
	// the security context is the information about the current user, their role and scope of access for the
	// current session etc.
	cid := "c016"

	r.ParseForm()

	var targetDomains []string
	params := r.Form
	var paramErrors []string
	for k, v := range params {
		switch k {
		case "target":
			for _, p := range v {
				targetDomains = append(targetDomains, strings.Split(p, ",")...)
			}
		default:
			paramErrors = append(paramErrors, fmt.Sprintf("Unknown query parameter: %s\n", k))
		}
	}

	var responseError string
	if len(paramErrors) != 0 {
		w.Header()["Content-Type"] = []string{"text/ascii"}
		responseError = strings.Join(paramErrors, "")
		fmt.Fprintf(w, responseError)
		// this error isn't handled anywhere at the moment
		return 404, errors.New("Internal Server Error")
	}

	if resp, err := Fetch(*dynamoDB, cid, domain, id, targetDomains); err != nil {
		log.Printf("error fetching records: %s", err)
		return 500, errors.New("Internal Server Error")

	} else {
		if bb, err := json.Marshal(resp); err != nil {
			// We only write this kind of application-specific logs. The infrastructure should log
			// the incoming request, time taken to process the reqyest response size (ideally in a standard
			// format such as Apache web logging (https://httpd.apache.org/docs/1.3/logs.html).
			log.Printf("error generating json: %s", err)
			return 500, errors.New(responseError)
		} else {
			w.Header()["Content-Type"] = []string{"application/json"}
			w.Write(bb)
		}
	}
	return 200, nil
}
