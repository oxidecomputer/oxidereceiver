package oxidereceiver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMakeOxideConfig(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	for _, tc := range []struct {
		name               string
		insecureSkipVerify bool
		expectHTTPClient   bool
		canConnectToTLS    bool
	}{
		{
			name:               "skip verify",
			insecureSkipVerify: true,
			expectHTTPClient:   true,
			canConnectToTLS:    true,
		},
		{
			name:               "verify",
			insecureSkipVerify: false,
			expectHTTPClient:   false,
			canConnectToTLS:    false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				Host:               ts.URL,
				InsecureSkipVerify: tc.insecureSkipVerify,
			}

			oxideConfig := makeOxideConfig(cfg)

			require.Equal(t, ts.URL, oxideConfig.Host)

			if tc.expectHTTPClient {
				require.NotNil(t, oxideConfig.HTTPClient, "HTTPClient should be set when InsecureSkipVerify is true")

				// Verify the client can connect to the test server with self-signed cert.
				resp, err := oxideConfig.HTTPClient.Get(ts.URL)
				require.NoError(t, err, "Should be able to connect with InsecureSkipVerify=true")
				require.NotNil(t, resp)
				resp.Body.Close()
			} else {
				require.Nil(t, oxideConfig.HTTPClient, "HTTPClient should be nil when InsecureSkipVerify is false")

				// Verify a default client cannot connect to the test server with self-signed cert.
				defaultClient := &http.Client{}
				_, err := defaultClient.Get(ts.URL)
				require.Error(t, err, "Should not be able to connect with default client to self-signed cert")
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	require.Equal(t, []string{".*"}, cfg.MetricPatterns)
	require.Equal(t, 16, cfg.ScrapeConcurrency)
	require.Equal(t, "5m", cfg.QueryLookback)
	require.False(t, cfg.InsecureSkipVerify, "InsecureSkipVerify should default to false")
}
