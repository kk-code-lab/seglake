package sse

import (
	"context"
	"fmt"
)

type wrapAlgorithmProvider interface {
	WrapAlgorithm() string
}

type dataKeyWrapper interface {
	WrapDataKey(ctx context.Context, req WrapDataKeyRequest) (WrapDataKeyResult, error)
}

// RoutingProvider dispatches reads by manifest wrap algorithm while sending new
// writes and target rewraps to the configured active provider.
type RoutingProvider struct {
	active  KeyProvider
	readers map[string]KeyProvider
}

func NewRoutingProvider(active KeyProvider, providers ...KeyProvider) (*RoutingProvider, error) {
	if active == nil {
		return nil, fmt.Errorf("%w: active provider required", ErrProviderUnavailable)
	}
	out := &RoutingProvider{
		active:  active,
		readers: make(map[string]KeyProvider),
	}
	for _, provider := range append([]KeyProvider{active}, providers...) {
		if provider == nil {
			continue
		}
		algorithm := providerWrapAlgorithm(provider)
		if _, ok := out.readers[algorithm]; ok {
			return nil, fmt.Errorf("%w: duplicate provider for wrap algorithm %q", ErrBadKeySpec, algorithm)
		}
		out.readers[algorithm] = provider
	}
	return out, nil
}

func (p *RoutingProvider) GenerateDataKey(ctx context.Context, req GenerateDataKeyRequest) (GenerateDataKeyResult, error) {
	if p == nil || p.active == nil {
		return GenerateDataKeyResult{}, ErrProviderUnavailable
	}
	return p.active.GenerateDataKey(ctx, req)
}

func (p *RoutingProvider) DefaultKeyID() string {
	if p == nil || p.active == nil {
		return ""
	}
	return DefaultKeyID(p.active)
}

func (p *RoutingProvider) DecryptDataKey(ctx context.Context, req DecryptDataKeyRequest) (DecryptDataKeyResult, error) {
	provider, err := p.providerFor(req.KeyEntry.WrapAlgorithm)
	if err != nil {
		return DecryptDataKeyResult{}, err
	}
	return provider.DecryptDataKey(ctx, req)
}

func (p *RoutingProvider) WrapDataKey(ctx context.Context, req WrapDataKeyRequest) (WrapDataKeyResult, error) {
	if p == nil || p.active == nil {
		return WrapDataKeyResult{}, ErrProviderUnavailable
	}
	wrapper, ok := p.active.(dataKeyWrapper)
	if !ok {
		return WrapDataKeyResult{}, fmt.Errorf("%w: active provider cannot wrap data keys", ErrProviderUnavailable)
	}
	return wrapper.WrapDataKey(ctx, req)
}

func (p *RoutingProvider) RewrapDataKey(ctx context.Context, req RewrapDataKeyRequest) (RewrapDataKeyResult, error) {
	decrypted, err := p.DecryptDataKey(ctx, DecryptDataKeyRequest{KeyEntry: req.KeyEntry})
	if err != nil {
		return RewrapDataKeyResult{}, err
	}
	wrapped, err := p.WrapDataKey(ctx, WrapDataKeyRequest{
		PlaintextDEK: decrypted.PlaintextDEK,
		KeyEntry:     req.KeyEntry,
		TargetKeyID:  req.TargetKeyID,
	})
	if err != nil {
		return RewrapDataKeyResult{}, err
	}
	return RewrapDataKeyResult(wrapped), nil
}

func (p *RoutingProvider) DescribeKey(ctx context.Context, keyID string) (KeyDescription, error) {
	if p == nil || p.active == nil {
		return KeyDescription{}, ErrProviderUnavailable
	}
	return p.active.DescribeKey(ctx, keyID)
}

func (p *RoutingProvider) providerFor(algorithm string) (KeyProvider, error) {
	if p == nil {
		return nil, ErrProviderUnavailable
	}
	provider, ok := p.readers[NormalizeWrapAlgorithm(algorithm)]
	if !ok {
		return nil, fmt.Errorf("%w: provider for wrap algorithm %q", ErrMissingKey, NormalizeWrapAlgorithm(algorithm))
	}
	return provider, nil
}

func providerWrapAlgorithm(provider KeyProvider) string {
	if provider, ok := provider.(wrapAlgorithmProvider); ok {
		return NormalizeWrapAlgorithm(provider.WrapAlgorithm())
	}
	return WrapAES256GCM
}

type defaultKeyIDProvider interface {
	DefaultKeyID() string
}

func DefaultKeyID(provider KeyProvider) string {
	if provider, ok := provider.(defaultKeyIDProvider); ok {
		return provider.DefaultKeyID()
	}
	return ""
}
