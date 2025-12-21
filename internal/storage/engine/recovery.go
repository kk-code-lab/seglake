package engine

import "errors"

// RecoveryReport summarizes scan results.
type RecoveryReport struct {
	CorruptedSegments []string
	MissingChunks     int
}

// ScanSegments verifies segment structure and returns a report.
func (e *Engine) ScanSegments() (*RecoveryReport, error) {
	return nil, errors.New("not implemented")
}
