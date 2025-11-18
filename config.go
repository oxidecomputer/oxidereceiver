package oxidereceiver

import (
	"fmt"
	"regexp"

	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`

	Host              string   `mapstructure:"host"`
	Token             string   `mapstructure:"token"`
	MetricPatterns    []string `mapstructure:"metric_patterns"`
	ScrapeConcurrency int      `mapstructure:"scrape_concurrency"`

	// AddLabels configures the receiver to add human-readable labels to metrics using the Oxide API.
	AddLabels bool `mapstructure:"add_labels"`
}

func (cfg *Config) Validate() error {
	for _, pattern := range cfg.MetricPatterns {
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("invalid metric pattern %s: %w", pattern, err)
		}
	}
	return nil
}
