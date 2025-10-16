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
package aws

import (
	"net/http"
	"strings"

	xhttp "github.com/mageg-x/dedups3/internal/http"
)

const (
	accessKeyMinLen = 3
	accessKeyMaxLen = 20
	secretKeyMinLen = 8
	secretKeyMaxLen = 40

	// SlashSeparator - slash separator.
	SlashSeparator  = "/"
	signV4Algorithm = "AWS4-HMAC-SHA256"
)

func getReqAccessKeyV2(r *http.Request) (string, xhttp.APIErrorCode) {
	if accessKey := r.Form.Get(xhttp.AmzAccessKeyID); accessKey != "" {
		return accessKey, xhttp.ErrNone
	}

	// below is V2 Signed Auth header format, splitting on `space` (after the `AWS` string).
	// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature
	authFields := strings.Split(r.Header.Get(xhttp.Authorization), " ")
	if len(authFields) != 2 {
		return "", xhttp.ErrMissingFields
	}

	// Then will be splitting on ":", this will separate `AWSAccessKeyId` and `Signature` string.
	keySignFields := strings.Split(strings.TrimSpace(authFields[1]), ":")
	if len(keySignFields) != 2 {
		return "", xhttp.ErrMissingFields
	}

	return keySignFields[0], xhttp.ErrNone
}

// parse credentialHeader string into its structured form.
func parseCredentialHeader(credElement string) (string, xhttp.APIErrorCode) {
	creds := strings.SplitN(strings.TrimSpace(credElement), "=", 2)
	if len(creds) != 2 {
		return "", xhttp.ErrMissingFields
	}
	if creds[0] != "Credential" {
		return "", xhttp.ErrMissingCredTag
	}
	credElements := strings.Split(strings.TrimSpace(creds[1]), SlashSeparator)
	if len(credElements) < 5 {
		return "", xhttp.ErrCredMalformed
	}
	accessKey := strings.Join(credElements[:len(credElements)-4], SlashSeparator) // The access key may contain one or more `/`
	if len(accessKey) < accessKeyMinLen || len(accessKey) > accessKeyMaxLen {
		return "", xhttp.ErrInvalidAccessKeyID
	}
	return accessKey, xhttp.ErrNone
}

func getReqAccessKeyV4(r *http.Request) (string, xhttp.APIErrorCode) {
	accessKey, s3Err := parseCredentialHeader("Credential=" + r.Form.Get(xhttp.AmzCredential))
	if s3Err != xhttp.ErrNone {
		// Strip off the Algorithm prefix.
		v4Auth := strings.TrimPrefix(r.Header.Get("Authorization"), signV4Algorithm)
		authFields := strings.Split(strings.TrimSpace(v4Auth), ",")
		if len(authFields) != 3 {
			return "", xhttp.ErrMissingFields
		}
		accessKey, s3Err = parseCredentialHeader(authFields[0])
		if s3Err != xhttp.ErrNone {
			return "", s3Err
		}
	}
	return accessKey, s3Err
}

func GetReqAccess(r *http.Request) string {
	accessKey, _ := getReqAccessKeyV4(r)
	if accessKey == "" {
		accessKey, _ = getReqAccessKeyV2(r)
	}
	return accessKey
}
