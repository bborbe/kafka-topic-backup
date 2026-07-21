// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import "strings"

func IsBrokenPipeError(err error) bool {
	return strings.HasSuffix(err.Error(), "broken pipe")
}
