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
package handler

import (
	"mime"
	"net/http"

	xhttp "github.com/mageg-x/boulder/internal/http"
)

// Verify if request has AWS Post policy Signature Version '4'.
func IsRequestPostPolicySignatureV4(r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get(xhttp.ContentType))
	if err != nil {
		return false
	}
	return mediaType == "multipart/form-data" && r.Method == http.MethodPost
}
