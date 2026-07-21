// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
)

const DateLayout = "2006-01-02"

func ParseDate(ctx context.Context, date string) (*libtime.Date, error) {
	parse, err := libtime.ParseDate(ctx, date)
	if err != nil {
		return nil, errors.Wrap(ctx, err, "parse failed")
	}
	return parse, err
}

func ParseDates(ctx context.Context, dates []string) (Dates, error) {
	var result Dates
	for _, date := range dates {
		d, err := ParseDate(ctx, date)
		if err != nil {
			return nil, errors.Wrap(ctx, err, "parse date failed")
		}
		result = append(result, *d)
	}
	return result, nil
}

type Dates []libtime.Date

func (d Dates) Len() int { return len(d) }

func (d Dates) Less(i, j int) bool { return d[i].Time().Before(d[j].Time()) }

func (d Dates) Swap(i, j int) { d[i], d[j] = d[j], d[i] }

func (d Dates) Unique() Dates {
	dates := map[libtime.Date]struct{}{}
	for _, date := range d {
		dates[date] = struct{}{}
	}
	result := make(Dates, 0, len(d))
	for date := range dates {
		result = append(result, date)
	}
	return result
}
