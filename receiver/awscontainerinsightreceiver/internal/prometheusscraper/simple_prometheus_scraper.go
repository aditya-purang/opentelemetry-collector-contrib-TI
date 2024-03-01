package prometheusscraper

import (
	"context"
	"errors"
	"fmt"
	ci "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/containerinsight"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
	"github.com/prometheus/prometheus/config"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type SimplePromethuesScraper struct {
	ctx                context.Context
	settings           component.TelemetrySettings
	host               component.Host
	hostInfoProvider   hostInfoProvider
	prometheusReceiver receiver.Metrics
	running            bool
}

type SimplePromethuesScraperOpts struct {
	Ctx               context.Context
	TelemetrySettings component.TelemetrySettings
	Consumer          consumer.Metrics
	Host              component.Host
	HostInfoProvider  hostInfoProvider
	K8sDecorator      Decorator
	Logger            *zap.Logger
}

type hostInfoProvider interface {
	GetClusterName() string
	GetInstanceID() string
}

func NewSimplePromethuesScraper(opts SimplePromethuesScraperOpts, scraperConfig *config.ScrapeConfig) (*SimplePromethuesScraper, error) {
	if opts.Consumer == nil {
		return nil, errors.New("consumer cannot be nil")
	}
	if opts.Host == nil {
		return nil, errors.New("host cannot be nil")
	}
	if opts.HostInfoProvider == nil {
		return nil, errors.New("cluster name provider cannot be nil")
	}

	promConfig := prometheusreceiver.Config{
		PrometheusConfig: &config.Config{
			ScrapeConfigs: []*config.ScrapeConfig{scraperConfig},
		},
	}

	params := receiver.CreateSettings{
		TelemetrySettings: opts.TelemetrySettings,
	}

	decoConsumer := decorateConsumer{
		containerOrchestrator: ci.EKS,
		nextConsumer:          opts.Consumer,
		k8sDecorator:          opts.K8sDecorator,
		logger:                opts.Logger,
	}

	promFactory := prometheusreceiver.NewFactory()
	promReceiver, err := promFactory.CreateMetricsReceiver(opts.Ctx, params, &promConfig, &decoConsumer)
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus receiver: %w", err)
	}

	return &SimplePromethuesScraper{
		ctx:                opts.Ctx,
		settings:           opts.TelemetrySettings,
		host:               opts.Host,
		hostInfoProvider:   opts.HostInfoProvider,
		prometheusReceiver: promReceiver,
	}, nil
}

func (ds *SimplePromethuesScraper) GetMetrics() []pmetric.Metrics {
	// This method will never return metrics because the metrics are collected by the scraper.
	// This method will ensure the scraper is running

	// this thing works, now just fixing the podresourcestore
	//ds.settings.Logger.Info("static_pod_resources staring scrapping")
	//stores.StartScraping(ds.settings.Logger)

	podresourcesstore := stores.NewPodResourcesStore(ds.settings.Logger)
	ds.settings.Logger.Info("Adding resources to PodResources")
	podresourcesstore.AddResourceName("aws.amazon.com/neuroncore")
	podresourcesstore.AddResourceName("aws.amazon.com/neuron")
	podresourcesstore.AddResourceName("aws.amazon.com/neurondevice")
	podresourcesstore.GetResourcesInfo("123", "123", "123")

	if !ds.running {
		ds.settings.Logger.Info("The scraper is not running, starting up the scraper")
		err := ds.prometheusReceiver.Start(ds.ctx, ds.host)
		if err != nil {
			ds.settings.Logger.Error("Unable to start PrometheusReceiver", zap.Error(err))
		}
		ds.running = err == nil
	}
	return nil
}

func (ds *SimplePromethuesScraper) Shutdown() {
	if ds.running {
		err := ds.prometheusReceiver.Shutdown(ds.ctx)
		if err != nil {
			ds.settings.Logger.Error("Unable to shutdown PrometheusReceiver", zap.Error(err))
		}
		ds.running = false
	}
}
