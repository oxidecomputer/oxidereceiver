package oxidereceiver

import (
	"context"
	"crypto/tls"
	"net/http"

	"github.com/oxidecomputer/oxide.go/oxide"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

// NewFactory creates a new factory for the oxide metrics receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType("oxide"),
		createDefaultConfig,
		receiver.WithMetrics(makeMetricsReceiver, component.StabilityLevelDevelopment),
	)
}

// createDefaultConfig creates the default configuration for the receiver.
func createDefaultConfig() component.Config {
	return &Config{
		MetricPatterns:    []string{".*"},
		ScrapeConcurrency: 16,
		QueryLookback:     "5m",
	}
}

// makeOxideConfig creates an oxide.Config from the receiver Config.
func makeOxideConfig(cfg *Config) *oxide.Config {
	oxideConfig := &oxide.Config{
		Host:  cfg.Host,
		Token: cfg.Token,
	}

	// Configure custom HTTP client if InsecureSkipVerify is enabled.
	if cfg.InsecureSkipVerify {
		oxideConfig.HTTPClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}

	return oxideConfig
}

// makeMetricsReceiver creates a metrics receiver based on the provided config.
func makeMetricsReceiver(
	ctx context.Context,
	settings receiver.Settings,
	baseCfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	rCfg := baseCfg.(*Config)

	oxideConfig := makeOxideConfig(rCfg)

	client, err := oxide.NewClient(oxideConfig)
	if err != nil {
		return nil, err
	}

	r := newOxideScraper(rCfg, settings.TelemetrySettings, client)
	s, err := scraper.NewMetrics(r.Scrape, scraper.WithStart(r.Start), scraper.WithShutdown(r.Shutdown))
	if err != nil {
		return nil, err
	}

	return scraperhelper.NewMetricsController(
		&rCfg.ControllerConfig,
		settings,
		consumer,
		scraperhelper.AddScraper(component.MustNewType("oxide"), s),
	)
}
