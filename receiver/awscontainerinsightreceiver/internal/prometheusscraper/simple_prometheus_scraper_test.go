package prometheusscraper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"
)

type mockHostInfoProvider struct {
}

func (m mockHostInfoProvider) GetClusterName() string {
	return "cluster-name"
}

func (m mockHostInfoProvider) GetInstanceID() string {
	return "i-000000000"
}

func TestSimplePrometheusScraperBadInputs(t *testing.T) {
	settings := componenttest.NewNopTelemetrySettings()
	settings.Logger, _ = zap.NewDevelopment()

	tests := []SimplePromethuesScraperOpts{
		{
			Ctx:               context.TODO(),
			TelemetrySettings: settings,
			Consumer:          nil,
			Host:              componenttest.NewNopHost(),
			HostInfoProvider:  mockHostInfoProvider{},
		},
		{
			Ctx:               context.TODO(),
			TelemetrySettings: settings,
			Consumer:          MockConsumer{},
			Host:              nil,
			HostInfoProvider:  mockHostInfoProvider{},
		},
		{
			Ctx:               context.TODO(),
			TelemetrySettings: settings,
			Consumer:          MockConsumer{},
			Host:              componenttest.NewNopHost(),
			HostInfoProvider:  nil,
		},
	}

	for _, tt := range tests {
		scraper, err := NewSimplePromethuesScraper(tt)

		assert.Error(t, err)
		assert.Nil(t, scraper)
	}
}
