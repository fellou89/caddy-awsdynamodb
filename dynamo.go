package awsdynamodb

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/mholt/caddy"
	"github.com/mholt/caddy/caddyhttp/httpserver"
	"net/http"
	"os"
	"os/exec"
	"os/signal"

	"github.com/pkg/errors"
)

func init() {
	caddy.RegisterPlugin("awsdynamodb", caddy.Plugin{
		ServerType: "http",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {
	if c.Next() {
		c.Next()
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

		if len(args) > 3 {
			if args[3] != "DAX" {
				return errors.New("Fourth argument can only be DAX indicator")
			} else {
				if len(args) < 5 {
					return errors.New("Not enough arguments to run DAX")
				} else {
					endpoint := args[4]
					if len(args) > 6 {
						return errors.New("Too many arguments")

					} else if len(args) > 5 {
						testingFlag := args[5]
						if testingFlag != "testing" {
							return errors.New("Different value than expected on last config item")

						} else {
							// Should return Testing API
						}
					}
					fmt.Println("node dax.js %s %s %s %s %s %s %s", region, endpoint, table, pkn, skn)
					exec.Command("node", "dax.js", region, endpoint, table, pkn, skn).Run()
				}
			}
		}

		if err != nil {
			return errors.Wrap(err, "Error setting up AWS session")
		}

		cfg := httpserver.GetConfig(c)
		mid := func(next httpserver.Handler) httpserver.Handler {
			return MyHandler{
				DynamoDB:         dynamodb.New(sess),
				Table:            table,
				PartitionKeyName: pkn,
				SortKeyName:      skn,
			}
		}
		cfg.AddMiddleware(mid)
	}

	interruptChan := make(chan os.Signal)
	go func() {
		defer close(interruptChan)
	listen:
		for {
			select {
			case <-interruptChan:
				// send shutdown request to node DAX
				break listen
			}
		}
	}()
	signal.Notify(interruptChan, os.Interrupt)
	return nil
}

type MyHandler struct {
	DynamoDB         *dynamodb.DynamoDB
	Table            string
	PartitionKeyName string
	SortKeyName      string
}

func (h MyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) (int, error) {
	return h.GetIds(w, r)
}
