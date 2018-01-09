package awsdynamodb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	. "github.com/fellou89/caddy-awscloudwatch"
	"github.com/pkg/errors"
)

func (h MyHandler) GetIds(w http.ResponseWriter, r *http.Request) (int, error) {
	var routeExp = regexp.MustCompile(`ids/v1/([0-9a-z]+)/([0-9a-z\-\_\.]+)/([0-9A-Za-z]+)`)
	match := routeExp.FindStringSubmatch(r.RequestURI)

	result := make(map[string]string)
	for i, name := range routeExp.SubexpNames() {
		if i != 0 {
			result[name] = match[i]
		}
	}
	entitytype := match[1]
	domain := match[2]
	id := match[3]

	// cid is the client (i.e. tenant) id. It is hardcoded here, but will be extracted from the security context
	// the security context is the information about the current user, their role and scope of access for the
	// current session etc.
	cid := "c016"

	r.ParseForm()
	params := r.Form

	var targetDomains []string
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
		return 404, errors.New("Parameter Errors")
	}

	if resp, err := h.Fetch(cid, entitytype, domain, id, targetDomains); err != nil {
		return 500, errors.Wrap(err, "Error fetching records")

	} else {
		if bb, err := json.Marshal(resp); err != nil {
			return 500, errors.Wrap(errors.New(responseError), "Error generating json")

		} else {
			LoggerInstance.Info(string(bb))
			w.Header()["Content-Type"] = []string{"application/json"}
			w.Write(bb)
		}
	}
	return 200, nil
}
