package awsdynamodb

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"os/exec"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/mholt/caddy"
	"github.com/mholt/caddy/caddyhttp/httpserver"
	"github.com/pkg/errors"

	clients "github.com/fellou89/caddy-awsdynamodb/clients"
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

		var id string
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
						if err := dax.Run(); err != nil {
							fmt.Printf("%s: %s\n", err, stderr)
						}
					}()

					ddb = clients.DaxClient{Endpoint: "http://0.0.0.0:" + daxPort}
					id = "dax"
				}
			}
		} else {
			ddb = clients.DynamoClient{Dynamo: dynamodb.New(sess)}
			id = "dynamo"
		}

		if err != nil {
			return errors.Wrap(err, "Error setting up AWS session")
		}

		cfg := httpserver.GetConfig(c)
		mid := func(next httpserver.Handler) httpserver.Handler {
			return MyHandler{
				DBConnection:     ddb,
				RequestID:        id,
				Table:            table,
				PartitionKeyName: pkn,
				SortKeyName:      skn,
				Next:             next,
			}
		}
		cfg.AddMiddleware(mid)
	}
	return nil
}

func (h MyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) (int, error) {
	r.ParseForm()
	if len(r.Form["backend"]) > 0 {
		requestID := r.Form["backend"][0]
		if requestID == h.RequestID {
			return h.GetIds(w, r)
		}
	}
	return h.Next.ServeHTTP(w, r)
}
