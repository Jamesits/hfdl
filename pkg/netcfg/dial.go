package netcfg

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"syscall"
	"time"
)

// ProxyFn is http.Transport.Proxy's callback shape; nil means direct.
type ProxyFn = func(*http.Request) (*url.URL, error)

// Dialer tuning shared by every hfdl connection.
const (
	dialTimeout   = 30 * time.Second
	dialKeepAlive = 30 * time.Second
)

// Transport tuning: generous pools, no global request timeout (transfer and
// hfapi enforce response timeouts per request instead).
const (
	maxIdleConns          = 128
	maxIdleConnsPerHost   = 32
	idleConnTimeout       = 90 * time.Second
	tlsHandshakeTimeout   = 10 * time.Second
	expectContinueTimeout = 1 * time.Second
)

// Dialer builds the tuned dual-stack dialer, tagging every socket with the
// given TOS/Traffic Class byte (TOSNone leaves sockets untagged). Tagging is
// advisory: a socket that refuses the option still connects, with one warning
// per dialer rather than one per connection.
func Dialer(tos int, log *slog.Logger) *net.Dialer {
	d := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: dialKeepAlive,
	}
	if tos == TOSNone {
		return d
	}
	var warnOnce sync.Once
	d.ControlContext = func(ctx context.Context, network, address string, c syscall.RawConn) error {
		var soErr error
		if err := c.Control(func(fd uintptr) { soErr = setTOS(fd, network, tos) }); err != nil {
			return err
		}
		if soErr != nil {
			warnOnce.Do(func() {
				log.LogAttrs(ctx, slog.LevelWarn, "setting IP QoS on socket failed; continuing untagged",
					slog.Int("tos", tos), slog.String("network", network), slog.Any("err", soErr))
			})
		}
		return nil
	}
	return d
}

// Transport builds the tuned *http.Transport all hfdl HTTP traffic uses.
// proxy comes from Proxy.ProxyFunc (nil = direct); tos tags the sockets of
// this transport's connection pool, including connections to a proxy.
func Transport(proxy ProxyFn, tos int, log *slog.Logger) *http.Transport {
	return &http.Transport{
		Proxy:                 proxy,
		DialContext:           Dialer(tos, log).DialContext,
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ExpectContinueTimeout: expectContinueTimeout,
	}
}

// GRPCDialer adapts the QoS dialer to grpc.WithContextDialer's signature for
// OTLP gRPC exporters. Proxying is not handled here: grpc-go applies its own
// *_PROXY environment handling around whatever dialer it is given.
func GRPCDialer(tos int, log *slog.Logger) func(context.Context, string) (net.Conn, error) {
	d := Dialer(tos, log)
	return func(ctx context.Context, addr string) (net.Conn, error) {
		return d.DialContext(ctx, "tcp", addr)
	}
}
