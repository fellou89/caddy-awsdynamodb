package awsdynamodb

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/mholt/caddy"
	"github.com/mholt/caddy/caddyhttp/httpserver"
	"github.com/pkg/errors"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	// "io/ioutil"
	// "os/signal"
	// "time"
)

func init() {
	caddy.RegisterPlugin("awsdynamodb", caddy.Plugin{
		ServerType: "http",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {
	if c.Next() {
		args := c.RemainingArgs()

		if len(args) < 3 {
			return errors.New("Too few arguments")
		}
		table := args[0]
		pkn := args[1]
		skn := args[2]

		sess, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})

		region := *sess.Config.Region

		var ddb DBC
		var dax *exec.Cmd

		if len(args) > 3 {
			if args[3] != "DAX" {
				return errors.New(fmt.Sprintf("Fourth argument can only be DAX indicator, got %s\n", args[3]))
			} else {
				if len(args) < 6 {
					return errors.New("Not enough arguments to run DAX")
				} else {
					daxPort := args[4]
					endpoint := args[5]

					if endpoint == "DAX_ENDPOINT" {
						return errors.New("DAX endpoint not set, this component should be removed from caddyfile")
					}

					if len(args) > 7 {
						return errors.New("Too many arguments")

					} else if len(args) > 6 {
						testingFlag := args[6]
						if testingFlag != "testing" {
							return errors.New("Different value than expected on last config item")

						} else {
							// Should return Testing API
						}
					}
					out, _ := exec.Command("sh", "-c", "echo $GOPATH/src/github.com/fellou89/caddy-awsdynamodb/dax.js").Output()
					path := string(out)

					dax = exec.Command("node", path[:len(path)-1], region, daxPort, endpoint, table, pkn, skn)
					var stderr bytes.Buffer
					dax.Stdout = os.Stdout
					dax.Stderr = &stderr
					go func() {
						err := dax.Run()
						if err != nil {
							fmt.Printf("%s: %s\n", err, stderr)
						}
					}()

					ddb = DaxClient{Endpoint: "http://0.0.0.0:" + daxPort}
				}

				// looks like all this isn't needed to kill the node server
				// interruptChan := make(chan os.Signal)
				// go func() {
				// 	defer close(interruptChan)
				// listen:
				// 	for {
				// 		select {
				// 		case <-interruptChan:
				// 			req, err := http.NewRequest("GET", "http://0.0.0.0:8086/shutdown", nil)
				// 			if err != nil {
				// 				fmt.Printf("Error making shutdown request: %s\n", err)
				// 			} else {
				// 				fmt.Println("Request made")
				// 			}
				// 			c := &http.Client{
				// 				Timeout: 10 * time.Second,
				// 			}
				// 			resp, err := c.Do(req)
				// 			defer resp.Body.Close()
				// 			if err != nil {
				// 				fmt.Printf("\nFailed to send DAX shutdown request: %s\n", err)
				// 			} else {
				// 				body, err := ioutil.ReadAll(resp.Body)
				// 				if err != nil {
				// 					fmt.Println(err)
				// 				}
				// 				fmt.Println("Shutdown response:")
				// 				fmt.Println(string(body))
				// 			}
				// 			if err := dax.Process.Kill(); err != nil {
				// 				fmt.Printf("\nFailed to kill DAX process: %s\n", err)
				// 			}
				// 			break listen
				// 		}
				// 	}
				// }()
				// signal.Notify(interruptChan, os.Interrupt)
			}
		} else {
			ddb = DynamoClient{Dynamo: dynamodb.New(sess)}
		}

		if err != nil {
			return errors.Wrap(err, "Error setting up AWS session")
		}

		cfg := httpserver.GetConfig(c)
		mid := func(next httpserver.Handler) httpserver.Handler {
			return MyHandler{
				DBConnection:     ddb,
				Table:            table,
				PartitionKeyName: pkn,
				SortKeyName:      skn,
			}
		}
		cfg.AddMiddleware(mid)
	}
	return nil
}

type MyHandler struct {
	DBConnection     DBC
	Table            string
	PartitionKeyName string
	SortKeyName      string
}

func (h MyHandler) Fetch(cid, entitytype, domain, id string, targetDomains []string) (interface{}, error) {
	var response map[string]Id
	var err error

	if len(targetDomains) > 1 {
		if response, err = h.DBConnection.BatchGetItem(h, targetDomains, cid, domain, id); err != nil {
			return nil, err
		}

	} else {
		if response, err = h.DBConnection.Query(h, targetDomains, cid, domain, id); err != nil {
			return nil, err
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

func (h MyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) (int, error) {
	return h.GetIds(w, r)
}
