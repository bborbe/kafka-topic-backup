// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

//go:generate mkdir -p ./avro
//go:generate go run -mod=mod github.com/actgardner/gogen-avro/v9/cmd/gogen-avro --containers ./avro ./avsc/backup-topic-record.avsc ./avsc/backup-topic-consumer-group-offset.avsc
