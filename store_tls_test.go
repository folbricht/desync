package desync

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestCA returns a self-signed CA certificate and its key.
func newTestCA(t *testing.T, cn string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert, key
}

// TestTLSClientCertSentRegardlessOfAcceptableCAs checks that the configured
// client certificate is sent even when the server's CertificateRequest lists
// CAs that did not issue it. Some TLS terminators (for example managed
// ingresses that forward the client certificate to the backend for
// verification) advertise a fixed list of public CAs, and Go's default
// selection would then send no certificate at all.
func TestTLSClientCertSentRegardlessOfAcceptableCAs(t *testing.T) {
	clientCA, clientCAKey := newTestCA(t, "private client CA")
	unrelatedCA, _ := newTestCA(t, "unrelated public CA")

	// Client certificate issued by the private CA.
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	clientTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, clientCA, &clientKey.PublicKey, clientCAKey)
	require.NoError(t, err)

	dir := t.TempDir()
	certFile := filepath.Join(dir, "client.crt")
	keyFile := filepath.Join(dir, "client.key")
	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))

	// The server advertises only the unrelated CA in its CertificateRequest
	// and requires a certificate without verifying it, like an ingress that
	// leaves verification to the backend.
	advertised := x509.NewCertPool()
	advertised.AddCert(unrelatedCA)
	var gotCN string
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.PeerCertificates) > 0 {
			gotCN = r.TLS.PeerCertificates[0].Subject.CommonName
		}
	}))
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert, ClientCAs: advertised}
	srv.StartTLS()
	defer srv.Close()

	opt := StoreOptions{ClientCert: certFile, ClientKey: keyFile, TrustInsecure: true}
	tlsConfig, err := opt.tlsClientConfig()
	require.NoError(t, err)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
	resp, err := client.Get(srv.URL)
	require.NoError(t, err, "client certificate was likely not sent")
	resp.Body.Close()
	require.Equal(t, "client", gotCN)
}
