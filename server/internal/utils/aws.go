/*
 * Copyright (C) 2025-2025 raochaoxun <raochaoxun@gmail.com>.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package utils

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	xhttp "github.com/mageg-x/boulder/internal/http"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"
)

// We support '.' with bucket names but we fallback to using path
// style requests instead for such buckets.
var (
	validBucketName       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9\.\-\_\:]{1,61}[A-Za-z0-9]$`)
	validBucketNameStrict = regexp.MustCompile(`^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$`)
	ipAddress             = regexp.MustCompile(`^(\d+\.){3}\d+$`)
	sentinelURL           = url.URL{}

	reservedObjectNames = regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")
	// amazonS3HostHyphen - regular expression used to determine if an arg is s3 host in hyphenated style.
	amazonS3HostHyphen = regexp.MustCompile(`^s3-(.*?).amazonaws.com$`)

	// amazonS3HostDualStack - regular expression used to determine if an arg is s3 host dualstack.
	amazonS3HostDualStack = regexp.MustCompile(`^s3.dualstack.(.*?).amazonaws.com$`)

	// amazonS3HostFIPS - regular expression used to determine if an arg is s3 FIPS host.
	amazonS3HostFIPS = regexp.MustCompile(`^s3-fips.(.*?).amazonaws.com$`)

	// amazonS3HostFIPSDualStack - regular expression used to determine if an arg is s3 FIPS host dualstack.
	amazonS3HostFIPSDualStack = regexp.MustCompile(`^s3-fips.dualstack.(.*?).amazonaws.com$`)

	// amazonS3HostDot - regular expression used to determine if an arg is s3 host in . style.
	amazonS3HostDot = regexp.MustCompile(`^s3.(.*?).amazonaws.com$`)

	// amazonS3ChinaHost - regular expression used to determine if the arg is s3 china host.
	amazonS3ChinaHost = regexp.MustCompile(`^s3.(cn.*?).amazonaws.com.cn$`)

	// amazonS3ChinaHostDualStack - regular expression used to determine if the arg is s3 china host dualstack.
	amazonS3ChinaHostDualStack = regexp.MustCompile(`^s3.dualstack.(cn.*?).amazonaws.com.cn$`)

	// Regular expression used to determine if the arg is elb host.
	elbAmazonRegex = regexp.MustCompile(`elb(.*?).amazonaws.com$`)

	// Regular expression used to determine if the arg is elb host in china.
	elbAmazonCnRegex = regexp.MustCompile(`elb(.*?).amazonaws.com.cn$`)

	// amazonS3HostPrivateLink - regular expression used to determine if an arg is s3 host in AWS PrivateLink interface endpoints style
	amazonS3HostPrivateLink = regexp.MustCompile(`^(?:bucket|accesspoint).vpce-.*?.s3.(.*?).vpce.amazonaws.com$`)
)

// IsValidDomain validates if input string is a valid domain name.
func IsValidDomain(host string) bool {
	// See RFC 1035, RFC 3696.
	host = strings.TrimSpace(host)
	if len(host) == 0 || len(host) > 255 {
		return false
	}
	// host cannot start or end with "-"
	if host[len(host)-1:] == "-" || host[:1] == "-" {
		return false
	}
	// host cannot start or end with "_"
	if host[len(host)-1:] == "_" || host[:1] == "_" {
		return false
	}
	// host cannot start with a "."
	if host[:1] == "." {
		return false
	}
	// All non alphanumeric characters are invalid.
	if strings.ContainsAny(host, "`~!@#$%^&*()+={}[]|\\\"';:><?/") {
		return false
	}
	// No need to regexp match, since the list is non-exhaustive.
	// We let it valid and fail later.
	return true
}

// IsValidIP parses input string for ip address validity.
func IsValidIP(ip string) bool {
	return net.ParseIP(ip) != nil
}

// IsVirtualHostSupported - verifies if bucketName can be part of
// virtual host. Currently only Amazon S3 and Google Cloud Storage
// would support this.
func IsVirtualHostSupported(endpointURL url.URL, bucketName string) bool {
	if endpointURL == sentinelURL {
		return false
	}
	// bucketName can be valid but '.' in the hostname will fail SSL
	// certificate validation. So do not use host-style for such buckets.
	if endpointURL.Scheme == "https" && strings.Contains(bucketName, ".") {
		return false
	}
	// Return true for all other cases
	return IsAmazonEndpoint(endpointURL) || IsGoogleEndpoint(endpointURL) || IsAliyunOSSEndpoint(endpointURL)
}

// GetRegionFromURL - returns a region from url host.
func GetRegionFromURL(endpointURL url.URL) string {
	if endpointURL == sentinelURL {
		return ""
	}
	if endpointURL.Hostname() == "s3-external-1.amazonaws.com" {
		return ""
	}

	// if elb's are used we cannot calculate which region it may be, just return empty.
	if elbAmazonRegex.MatchString(endpointURL.Hostname()) || elbAmazonCnRegex.MatchString(endpointURL.Hostname()) {
		return ""
	}

	// We check for FIPS dualstack matching first to avoid the non-greedy
	// regex for FIPS non-dualstack matching a dualstack URL
	parts := amazonS3HostFIPSDualStack.FindStringSubmatch(endpointURL.Hostname())
	if len(parts) > 1 {
		return parts[1]
	}

	parts = amazonS3HostFIPS.FindStringSubmatch(endpointURL.Hostname())
	if len(parts) > 1 {
		return parts[1]
	}

	parts = amazonS3HostDualStack.FindStringSubmatch(endpointURL.Hostname())
	if len(parts) > 1 {
		return parts[1]
	}

	parts = amazonS3HostHyphen.FindStringSubmatch(endpointURL.Hostname())
	if len(parts) > 1 {
		return parts[1]
	}

	parts = amazonS3ChinaHost.FindStringSubmatch(endpointURL.Hostname())
	if len(parts) > 1 {
		return parts[1]
	}

	parts = amazonS3ChinaHostDualStack.FindStringSubmatch(endpointURL.Hostname())
	if len(parts) > 1 {
		return parts[1]
	}

	parts = amazonS3HostDot.FindStringSubmatch(endpointURL.Hostname())
	if len(parts) > 1 {
		return parts[1]
	}

	parts = amazonS3HostPrivateLink.FindStringSubmatch(endpointURL.Hostname())
	if len(parts) > 1 {
		return parts[1]
	}

	return ""
}

// IsAliyunOSSEndpoint - Match if it is exactly Aliyun OSS endpoint.
func IsAliyunOSSEndpoint(endpointURL url.URL) bool {
	return strings.HasSuffix(endpointURL.Host, "aliyuncs.com")
}

// IsAmazonEndpoint - Match if it is exactly Amazon S3 endpoint.
func IsAmazonEndpoint(endpointURL url.URL) bool {
	if endpointURL.Host == "s3-external-1.amazonaws.com" || endpointURL.Host == "s3.amazonaws.com" {
		return true
	}
	return GetRegionFromURL(endpointURL) != ""
}

// IsAmazonGovCloudEndpoint - Match if it is exactly Amazon S3 GovCloud endpoint.
func IsAmazonGovCloudEndpoint(endpointURL url.URL) bool {
	if endpointURL == sentinelURL {
		return false
	}
	return (endpointURL.Host == "s3-us-gov-west-1.amazonaws.com" ||
		endpointURL.Host == "s3-us-gov-east-1.amazonaws.com" ||
		IsAmazonFIPSGovCloudEndpoint(endpointURL))
}

// IsAmazonFIPSGovCloudEndpoint - match if the endpoint is FIPS and GovCloud.
func IsAmazonFIPSGovCloudEndpoint(endpointURL url.URL) bool {
	if endpointURL == sentinelURL {
		return false
	}
	return IsAmazonFIPSEndpoint(endpointURL) && strings.Contains(endpointURL.Host, "us-gov-")
}

// IsAmazonFIPSEndpoint - Match if it is exactly Amazon S3 FIPS endpoint.
// See https://aws.amazon.com/compliance/fips.
func IsAmazonFIPSEndpoint(endpointURL url.URL) bool {
	if endpointURL == sentinelURL {
		return false
	}
	return strings.HasPrefix(endpointURL.Host, "s3-fips") && strings.HasSuffix(endpointURL.Host, ".amazonaws.com")
}

// IsAmazonPrivateLinkEndpoint - Match if it is exactly Amazon S3 PrivateLink interface endpoint
// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html.
func IsAmazonPrivateLinkEndpoint(endpointURL url.URL) bool {
	if endpointURL == sentinelURL {
		return false
	}
	return amazonS3HostPrivateLink.MatchString(endpointURL.Hostname())
}

// IsGoogleEndpoint - Match if it is exactly Google cloud storage endpoint.
func IsGoogleEndpoint(endpointURL url.URL) bool {
	if endpointURL == sentinelURL {
		return false
	}
	return endpointURL.Hostname() == "storage.googleapis.com"
}

// Expects ascii encoded strings - from output of urlEncodePath
func percentEncodeSlash(s string) string {
	return strings.ReplaceAll(s, "/", "%2F")
}

// QueryEncode - encodes query values in their URL encoded form. In
// addition to the percent encoding performed by urlEncodePath() used
// here, it also percent encodes '/' (forward slash)
func QueryEncode(v url.Values) string {
	if v == nil {
		return ""
	}
	var buf bytes.Buffer
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vs := v[k]
		prefix := percentEncodeSlash(EncodePath(k)) + "="
		for _, v := range vs {
			if buf.Len() > 0 {
				buf.WriteByte('&')
			}
			buf.WriteString(prefix)
			buf.WriteString(percentEncodeSlash(EncodePath(v)))
		}
	}
	return buf.String()
}

func EncodePath(pathName string) string {
	if reservedObjectNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname strings.Builder
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname.WriteRune(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname.WriteRune(s)
			continue
		default:
			l := utf8.RuneLen(s)
			if l < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, l)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedPathname.WriteString("%" + strings.ToUpper(hex))
			}
		}
	}
	return encodedPathname.String()
}

// Common checker for both stricter and basic validation.
func checkBucketNameCommon(bucketName string, strict bool) (err error) {
	if strings.TrimSpace(bucketName) == "" {
		return errors.New("Bucket name cannot be empty")
	}
	if len(bucketName) < 3 {
		return errors.New("Bucket name cannot be shorter than 3 characters")
	}
	if len(bucketName) > 63 {
		return errors.New("Bucket name cannot be longer than 63 characters")
	}
	if ipAddress.MatchString(bucketName) {
		return errors.New("Bucket name cannot be an ip address")
	}
	if strings.Contains(bucketName, "..") || strings.Contains(bucketName, ".-") || strings.Contains(bucketName, "-.") {
		return errors.New("Bucket name contains invalid characters")
	}
	if strict {
		if !validBucketNameStrict.MatchString(bucketName) {
			err = errors.New("Bucket name contains invalid characters")
		}
		return err
	}
	if !validBucketName.MatchString(bucketName) {
		err = errors.New("Bucket name contains invalid characters")
	}
	return err
}

// CheckValidBucketName - checks if we have a valid input bucket name.
func CheckValidBucketName(bucketName string) (err error) {
	return checkBucketNameCommon(bucketName, false)
}

// CheckValidBucketNameStrict - checks if we have a valid input bucket name.
// This is a stricter version.
// - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
func CheckValidBucketNameStrict(bucketName string) (err error) {
	return checkBucketNameCommon(bucketName, true)
}

// CheckValidObjectNamePrefix - checks if we have a valid input object name prefix.
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
func CheckValidObjectNamePrefix(objectName string) error {
	if len(objectName) > 1024 {
		return errors.New("Object name cannot be longer than 1024 characters")
	}
	if !utf8.ValidString(objectName) {
		return errors.New("Object name with non UTF-8 strings are not supported")
	}
	return nil
}

// CheckValidObjectName - checks if we have a valid input object name.
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
func CheckValidObjectName(objectName string) error {
	if strings.TrimSpace(objectName) == "" {
		return errors.New("Object name cannot be empty")
	}
	return CheckValidObjectNamePrefix(objectName)
}

func CheckValidStorageClass(storageClass string) (err error) {
	var oscs types.ObjectStorageClass
	for _, osc := range oscs.Values() {
		if osc == types.ObjectStorageClass(storageClass) {
			return nil
		}
	}
	return errors.New("Invalid storage class")
}

func ExtractMetadata(header http.Header) (map[string]string, error) {
	meta := make(map[string]string)
	totalSize := 0

	for key, values := range header {
		lowerKey := strings.ToLower(key)
		if !strings.HasPrefix(lowerKey, strings.ToLower(xhttp.AMZMetPrefix)) {
			continue
		}

		metaKey := key[len(xhttp.AMZMetPrefix):]

		// 检查 key 是否只包含合法字符（可选）
		if !IsValidMetaKey(metaKey) {
			return nil, fmt.Errorf("invalid meta key: %s", metaKey)
		}

		if len(values) == 0 {
			continue
		}
		metaValue := values[0]

		// 检查值是否为有效 UTF-8（可选）
		if !utf8.ValidString(metaValue) {
			return nil, fmt.Errorf("meta value not valid UTF-8: %s", metaKey)
		}

		size := len(metaKey) + len(metaValue)
		if totalSize+size > xhttp.AMZMaxMetaSize {
			return nil, fmt.Errorf("user metadata exceeds 2KB limit")
		}
		totalSize += size

		meta[metaKey] = metaValue
	}

	return meta, nil
}

func ExtractTags(header http.Header) (map[string]string, error) {
	tagHeader := header.Get(xhttp.AmzObjectTagging)
	tags := make(map[string]string)

	if tagHeader == "" {
		return tags, nil
	}

	// URL 解码（因为 key 和 value 都可能被编码）
	decoded, err := url.QueryUnescape(tagHeader)
	if err != nil {
		return nil, fmt.Errorf("invalid tagging: malformed URL encoding: %w", err)
	}

	// 按 & 分割
	pairs := strings.Split(decoded, "&")
	for _, pair := range pairs {
		if pair == "" {
			continue
		}

		// 按第一个 = 分割（value 中可能包含 =）
		eqIdx := strings.Index(pair, "=")
		if eqIdx == -1 {
			return nil, fmt.Errorf("invalid tag: missing '=' in '%s'", pair)
		}

		key := strings.TrimSpace(pair[:eqIdx])
		value := strings.TrimSpace(pair[eqIdx+1:])

		if key == "" {
			return nil, fmt.Errorf("invalid tag: empty key in '%s'", pair)
		}

		// S3 tag key 限制：
		// - 1-128 字符
		// - 不能以 `aws:` 开头（保留）
		if len(key) < 1 || len(key) > 128 {
			return nil, fmt.Errorf("tag key '%s' must be 1-128 characters long", key)
		}
		if strings.HasPrefix(strings.ToLower(key), "aws:") {
			return nil, fmt.Errorf("tag key '%s' is reserved by AWS", key)
		}

		// S3 tag value 限制：
		// - 0-256 字符
		if len(value) > 256 {
			return nil, fmt.Errorf("tag value for key '%s' exceeds 256 characters", key)
		}

		// S3 不允许重复 key
		if _, exists := tags[key]; exists {
			return nil, fmt.Errorf("duplicate tag key: '%s'", key)
		}

		tags[key] = value
	}

	// S3 最多支持 50 个标签
	if len(tags) > 50 {
		return nil, fmt.Errorf("too many tags: %d, maximum is 50", len(tags))
	}

	return tags, nil
}
func IsValidMetaKey(key string) bool {
	for _, r := range key {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' && r != '_' && r != '.' {
			return false
		}
	}
	return len(key) > 0
}

// IsNetworkOrHostDown - if there was a network error or if the host is down.
// expectTimeouts indicates that *context* timeouts are expected and does not
// indicate a downed host. Other timeouts still returns down.
func IsNetworkOrHostDown(err error, expectTimeouts bool) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return false
	}

	if expectTimeouts && errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	if expectTimeouts && errors.Is(err, os.ErrDeadlineExceeded) {
		return false
	}

	// We need to figure if the error either a timeout
	// or a non-temporary error.
	urlErr := &url.Error{}
	if errors.As(err, &urlErr) {
		switch urlErr.Err.(type) {
		case *net.DNSError, *net.OpError, net.UnknownNetworkError:
			return true
		}
	}

	var e net.Error
	if errors.As(err, &e) {
		if e.Timeout() {
			return true
		}
	}

	// If write to an closed connection, It will make this error
	opErr := &net.OpError{}
	if errors.As(err, &opErr) {
		if opErr.Op == "write" && opErr.Net == "tcp" {
			if es, ok := opErr.Err.(*os.SyscallError); ok && es.Syscall == "wsasend" {
				return true
			}
		}
	}

	// Fallback to other mechanisms.
	return strings.Contains(err.Error(), "Connection closed by foreign host") ||
		strings.Contains(err.Error(), "TLS handshake timeout") ||
		strings.Contains(err.Error(), "i/o timeout") ||
		strings.Contains(err.Error(), "connection timed out") ||
		strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(strings.ToLower(err.Error()), "503 service unavailable") ||
		strings.Contains(err.Error(), "use of closed network connection") ||
		strings.Contains(err.Error(), "An existing connection was forcibly closed by the remote host")
}

// IsConnResetErr - Checks for "connection reset" errors.
func IsConnResetErr(err error) bool {
	if strings.Contains(err.Error(), "connection reset by peer") {
		return true
	}
	// incase if error message is wrapped.
	return errors.Is(err, syscall.ECONNRESET)
}

// IsConnRefusedErr - Checks for "connection refused" errors.
func IsConnRefusedErr(err error) bool {
	if strings.Contains(err.Error(), "connection refused") {
		return true
	}
	// incase if error message is wrapped.
	return errors.Is(err, syscall.ECONNREFUSED)
}
