package oxidereceiver

import (
	"context"

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
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelDevelopment),
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

// createMetricsReceiver creates a metrics receiver based on the provided config.
func createMetricsReceiver(
	ctx context.Context,
	settings receiver.Settings,
	baseCfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	rCfg := baseCfg.(*Config)

	client, err := oxide.NewClient(&oxide.Config{
		Host:  rCfg.Host,
		Token: rCfg.Token,
	})
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
