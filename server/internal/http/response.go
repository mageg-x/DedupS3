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
package http

import (
	"net/http"
	"strconv"
)

type mimeType string

const (
	// Means no response type.
	mimeNone mimeType = ""
	// Means response type is JSON.
	mimeJSON mimeType = "application/json"
	// Means response type is XML.
	mimeXML mimeType = "application/xml"
)

func Response(w http.ResponseWriter, code int, data []byte, headers map[string]string, mType mimeType) {
	w.Header().Set(ServerInfo, "Boulder")
	w.Header().Set(AcceptRanges, "bytes")
	w.Header().Del(AmzServerSideEncryptionCustomerKey)
	w.Header().Del(AmzServerSideEncryptionCopyCustomerKey)
	w.Header().Del(AmzMetaUnencryptedContentLength)
	w.Header().Del(AmzMetaUnencryptedContentMD5)

	if mType != mimeNone {
		w.Header().Set(ContentType, string(mType))
	} else {
		w.Header().Set(ContentType, "text/plain; charset=utf-8")
	}
	if mType != mimeNone {
		w.Header().Set(ContentType, string(mType))
	}
	w.Header().Set(ContentLength, strconv.Itoa(len(data)))
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(code)
	if data != nil {
		w.Write(data)
	}
}
