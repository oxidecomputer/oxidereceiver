package oxidereceiver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "valid",
			cfg: Config{
				Host:  "${env:OXIDE_HOST}",
				Token: "${env:OXIDE_TOKEN}",
			},
		},
		{
			name: "invalid regex",
			cfg: Config{
				Host:  "http://localhost:12220",
				Token: "${env:OXIDE_TOKEN}",
				MetricPatterns: []string{
					"?",
				},
			},
			wantErr: "invalid metric pattern",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
