// Package common contains constants
package common

import "time"

const (
	DefaultHeartbeat = 3
)

func ToUTC(t *time.Time) *time.Time {
    if t == nil {
        return nil
    }
    utc := t.UTC()
    return &utc
}
