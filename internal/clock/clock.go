package clock

import "time"

type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

type FixedClock struct {
	T time.Time
}

func (c FixedClock) Now() time.Time {
	return c.T
}
