package engine

import (
	ssecrypto "github.com/kk-code-lab/seglake/internal/sse"
	"github.com/kk-code-lab/seglake/internal/storage/manifest"
)

func manifestKeyEntryFromSSE(entry ssecrypto.KeyEntry) manifest.KeyEntry {
	return manifest.KeyEntry{
		KeyRef:          entry.KeyRef,
		KeyID:           entry.KeyID,
		EncryptedDEK:    entry.EncryptedDEK,
		WrapNonce:       entry.WrapNonce,
		NoncePrefix:     entry.NoncePrefix,
		NonceScheme:     entry.NonceScheme,
		EDEKFingerprint: entry.EDEKFingerprint,
	}
}

func sseKeyEntryFromManifestWithWrap(wrapAlgorithm string, entry manifest.KeyEntry) ssecrypto.KeyEntry {
	return ssecrypto.KeyEntry{
		WrapAlgorithm:   ssecrypto.NormalizeWrapAlgorithm(wrapAlgorithm),
		KeyRef:          entry.KeyRef,
		KeyID:           entry.KeyID,
		EncryptedDEK:    entry.EncryptedDEK,
		WrapNonce:       entry.WrapNonce,
		NoncePrefix:     entry.NoncePrefix,
		NonceScheme:     entry.NonceScheme,
		EDEKFingerprint: entry.EDEKFingerprint,
	}
}
