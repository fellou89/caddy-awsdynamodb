package awsdynamodb

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/mholt/caddy"
	"github.com/mholt/caddy/caddyhttp/httpserver"
	"net/http"
)

func init() {
	caddy.RegisterPlugin("awsdynamodb", caddy.Plugin{
		ServerType: "http",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {
	for c.Next() {
		// Here, we read the credentials from AWS standard places (~/.aws/credentials or environment variables)
		// But, in production systems we should be able to get it from application-specific config files.
		// There should be at least two config files (1) for storing non-secret config, which can be read with
		// low privileged access (2) for storing secrets, which can be read only by root permissions.
		sess, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			fmt.Errorf("error setting up AWS session: %s", err)
			return err
		}

		cfg := httpserver.GetConfig(c)
		mid := func(next httpserver.Handler) httpserver.Handler {
			return MyHandler{
				Next:     next,
				DynamoDB: dynamodb.New(sess),
			}
		}
		cfg.AddMiddleware(mid)
	}

	return nil
}

type MyHandler struct {
	Next     httpserver.Handler
	DynamoDB *dynamodb.DynamoDB
}

func (h MyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) (int, error) {
	if status, err := GetIds(h.DynamoDB, w, r); err != nil {
		w.WriteHeader(status)
		return status, err
	} else {
		return h.Next.ServeHTTP(w, r)
	}
}
