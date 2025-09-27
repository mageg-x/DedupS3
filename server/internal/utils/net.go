package utils

import (
	"net"
	"net/http"
	"regexp"
	"strings"
)

func GetSourceIP(r *http.Request) string {
	const (
		xForwardedFor = "X-Forwarded-For"
		xRealIP       = "X-Real-IP"
		forwarded     = "Forwarded"
	)

	// Regular expression to parse the 'for=' field in the Forwarded header.
	// Matches both IPv4, IPv6 (quoted), and obfuscated formats.
	forRegex := regexp.MustCompile(`(?i)(?:(?:^|,)\s*for\s*=\s*(?:"([^"]+)"|[^,\s]+))`)

	var addr string

	// Step 1: Try to get the IP from X-Forwarded-For header.
	// Use only the first (client) address if multiple are present.
	if fwd := r.Header.Get(xForwardedFor); fwd != "" {
		s := strings.Index(fwd, ", ")
		if s == -1 {
			s = len(fwd)
		}
		addr = fwd[:s]
	}

	// Step 2: If not found, try X-Real-IP.
	if addr == "" {
		if fwd := r.Header.Get(xRealIP); fwd != "" {
			addr = fwd
		} else if fwd := r.Header.Get(forwarded); fwd != "" {
			// Parse the Forwarded header to extract the 'for' parameter.
			if match := forRegex.FindStringSubmatch(fwd); len(match) > 1 {
				// Remove surrounding quotes, especially important for IPv6 addresses.
				addr = strings.Trim(match[1], `"`)
			}
		}
	}

	// Step 3: Fallback to RemoteAddr if headers didn't provide an IP.
	if addr == "" {
		addr = r.RemoteAddr
	}

	// Step 4: Extract just the IP part from addr (which might be "host:port").
	raddr, _, err := net.SplitHostPort(addr)
	if err != nil || raddr == "" {
		raddr = addr // In case SplitHostPort fails, use the original.
	}

	// Step 5: Format IPv6 addresses with square brackets.
	if strings.ContainsRune(raddr, ':') {
		return "[" + raddr + "]"
	}
	return raddr
}
