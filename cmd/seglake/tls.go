package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

type certReloader struct {
	certPath string
	keyPath  string

	mu        sync.Mutex
	cert      *tls.Certificate
	certMtime time.Time
	keyMtime  time.Time
}

func newTLSConfig(certPath, keyPath string) (*tls.Config, error) {
	if certPath == "" || keyPath == "" {
		return nil, errors.New("tls cert and key required")
	}
	loader := &certReloader{certPath: certPath, keyPath: keyPath}
	if err := loader.load(); err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: loader.getCertificate,
	}, nil
}

func (r *certReloader) getCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.needsReload() {
		if err := r.loadLocked(); err != nil {
			if r.cert != nil {
				fmt.Fprintf(os.Stderr, "tls reload failed, using previous cert: %v\n", err)
				return r.cert, nil
			}
			return nil, err
		}
	}
	if r.cert == nil {
		return nil, errors.New("tls certificate not loaded")
	}
	return r.cert, nil
}

func (r *certReloader) needsReload() bool {
	certInfo, err := os.Stat(r.certPath)
	if err != nil {
		return false
	}
	keyInfo, err := os.Stat(r.keyPath)
	if err != nil {
		return false
	}
	if certInfo.ModTime().After(r.certMtime) || keyInfo.ModTime().After(r.keyMtime) {
		return true
	}
	return false
}

func (r *certReloader) load() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.loadLocked()
}

func (r *certReloader) loadLocked() error {
	certInfo, err := os.Stat(r.certPath)
	if err != nil {
		return err
	}
	keyInfo, err := os.Stat(r.keyPath)
	if err != nil {
		return err
	}
	pair, err := tls.LoadX509KeyPair(r.certPath, r.keyPath)
	if err != nil {
		return err
	}
	r.cert = &pair
	r.certMtime = certInfo.ModTime()
	r.keyMtime = keyInfo.ModTime()
	return nil
}
