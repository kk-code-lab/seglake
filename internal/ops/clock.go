package ops

import (
	"time"

	"github.com/kk-code-lab/seglake/internal/clock"
)

func now() time.Time {
	return clock.RealClock{}.Now()
}
