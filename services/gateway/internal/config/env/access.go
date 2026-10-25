package env

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	access "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/events/access"
	"github.com/caarlos0/env/v11"
)

type accessConfig struct {
	Enable    bool          `env:"ACCESS_EVENTS_ENABLED" envDefault:"false"`
	ID        string        `env:"GATEWAY_ID"`
	Bootstrap string        `env:"KAFKA_BOOTSTRAP"`
	Topic     string        `env:"KAFKA_ACCESS_TOPIC" envDefault:"opens3.access.v1"`
	Capacity  int           `env:"ACCESS_EVENTS_QUEUE_CAPACITY" envDefault:"1024"`
	Retry     time.Duration `env:"ACCESS_EVENTS_RETRY_INTERVAL" envDefault:"1s"`
	Send      time.Duration `env:"ACCESS_EVENTS_SEND_TIMEOUT" envDefault:"2s"`
	Shutdown  time.Duration `env:"ACCESS_EVENTS_SHUTDOWN_TIMEOUT" envDefault:"10s"`
}

func NewAccessConfig() (*accessConfig, error) {
	c := &accessConfig{}
	if err := env.Parse(c); err != nil {
		return nil, err
	}
	if !c.Enable {
		return c, nil
	}
	if !access.ValidStreamID(c.ID) {
		return nil, fmt.Errorf("GATEWAY_ID must match [A-Za-z0-9_.-]{1,128}")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`).MatchString(c.Topic) || c.Topic == "." || c.Topic == ".." {
		return nil, fmt.Errorf("invalid KAFKA_ACCESS_TOPIC")
	}
	for _, broker := range c.Brokers() {
		host, port, err := net.SplitHostPort(broker)
		number, portErr := strconv.Atoi(port)
		if err != nil || host == "" || portErr != nil || number < 1 || number > 65535 {
			return nil, fmt.Errorf("KAFKA_BOOTSTRAP must contain host:port addresses")
		}
	}
	if c.Capacity < 1 || c.Capacity > 65536 {
		return nil, fmt.Errorf("ACCESS_EVENTS_QUEUE_CAPACITY must be between 1 and 65536")
	}
	if c.Retry < time.Millisecond || c.Retry > time.Minute || c.Send < time.Millisecond || c.Send > 30*time.Second || c.Shutdown < time.Millisecond || c.Shutdown > time.Minute {
		return nil, fmt.Errorf("invalid access event retry, send or shutdown timeout")
	}
	return c, nil
}
func (c *accessConfig) Enabled() bool     { return c.Enable }
func (c *accessConfig) GatewayID() string { return c.ID }
func (c *accessConfig) Brokers() []string {
	b := strings.Split(c.Bootstrap, ",")
	for i := range b {
		b[i] = strings.TrimSpace(b[i])
	}
	return b
}
func (c *accessConfig) AccessTopic() string            { return c.Topic }
func (c *accessConfig) QueueCapacity() int             { return c.Capacity }
func (c *accessConfig) RetryInterval() time.Duration   { return c.Retry }
func (c *accessConfig) SendTimeout() time.Duration     { return c.Send }
func (c *accessConfig) ShutdownTimeout() time.Duration { return c.Shutdown }
