package stream

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Resolve the IP manually so we don't use an IPv6 address. Kafka's listener
// doesn't seem to like IPv6.
func resolveBrokerAddr(addr string) (net.Addr, error) {
	host, port, err := splitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ip, err := lookupIPv4(ctx, host)
	if err != nil {
		return nil, err
	}

	return &net.TCPAddr{
		IP:   ip,
		Port: port,
	}, nil
}

// splitHostPort splits a string on the first colon to pull a hostname and a
// port out separately from an address.
func splitHostPort(hostport string) (host string, port int, err error) {
	parts := strings.SplitN(hostport, ":", 2)
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid hostport: %v", hostport)
	}
	host = parts[0]
	port, err = strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid hostport: %v", hostport)
	}
	return host, port, nil
}

// lookupIPv4 returns the first IPv4 address returned by the system resolver for
// the given hostname. If the hostname does not resolve to any IPv4 addresses,
// an error is returned.
func lookupIPv4(ctx context.Context, hostname string) (addr net.IP, err error) {
	res := new(net.Resolver)
	addrs, err := res.LookupIPAddr(ctx, hostname)
	if err != nil {
		return nil, err
	}
	for _, a := range addrs {
		ipv4 := a.IP.To4()
		if ipv4 != nil {
			return ipv4, nil
		}
	}
	return nil, fmt.Errorf("no ipv4 addresses found for %s", hostname)
}
