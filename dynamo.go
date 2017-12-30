package awsdynamodb

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/mholt/caddy"
	"github.com/mholt/caddy/caddyhttp/httpserver"
	"net/http"

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
		sess, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			return errors.Wrap(err, "Error setting up AWS session")
		}

		cfg := httpserver.GetConfig(c)
		mid := func(next httpserver.Handler) httpserver.Handler {
			return MyHandler{
				DynamoDB: dynamodb.New(sess),
			}
		}
		cfg.AddMiddleware(mid)
	}
	return nil
}

type MyHandler struct {
	DynamoDB *dynamodb.DynamoDB
}

func (h MyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) (int, error) {
	return GetIds(h.DynamoDB, w, r)
}
