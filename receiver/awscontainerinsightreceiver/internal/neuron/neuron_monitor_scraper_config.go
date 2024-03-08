// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package neuron

import (
	"os"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/prometheusscraper"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/kubernetes"
	"github.com/prometheus/prometheus/model/relabel"
)

const (
	caFile             = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	collectionInterval = 60 * time.Second
	jobName            = "containerInsightsNeuronMonitorScraper"
)

func GetNueronScrapeConfig(hostinfo prometheusscraper.HostInfoProvider) *config.ScrapeConfig {

	return &config.ScrapeConfig{
		ScrapeInterval: model.Duration(collectionInterval),
		ScrapeTimeout:  model.Duration(collectionInterval),
		JobName:        jobName,
		Scheme:         "http",
		MetricsPath:    "/metrics",
		ServiceDiscoveryConfigs: discovery.Configs{
			&kubernetes.SDConfig{
				Role: kubernetes.RoleService,
				NamespaceDiscovery: kubernetes.NamespaceDiscovery{
					IncludeOwnNamespace: true,
				},
				Selectors: []kubernetes.SelectorConfig{
					{
						Role:  kubernetes.RoleService,
						Label: "k8s-app=neuron-monitor-service",
					},
				},
			},
		},
		MetricRelabelConfigs: GetNueronMetricRelabelConfigs(hostinfo),
	}
}

func GetNueronMetricRelabelConfigs(hostinfo prometheusscraper.HostInfoProvider) []*relabel.Config {

	return []*relabel.Config{
		{
			SourceLabels: model.LabelNames{"__name__"},
			Regex:        relabel.MustNewRegexp("neuron.*|system_.*|execution_.*"),
			Action:       relabel.Keep,
		},
		{
			SourceLabels: model.LabelNames{"instance_name"},
			TargetLabel:  "NodeName",
			Regex:        relabel.MustNewRegexp("(.*)"),
			Replacement:  "${1}",
			Action:       relabel.Replace,
		},
		{
			SourceLabels: model.LabelNames{"instance_id"},
			TargetLabel:  "InstanceId",
			Regex:        relabel.MustNewRegexp("(.*)"),
			Replacement:  "${1}",
			Action:       relabel.Replace,
		},
		{
			SourceLabels: model.LabelNames{"instance_type"},
			TargetLabel:  "InstanceType",
			Regex:        relabel.MustNewRegexp("(.*)"),
			Replacement:  "${1}",
			Action:       relabel.Replace,
		},
		{
			SourceLabels: model.LabelNames{"neuroncore"},
			TargetLabel:  "NeuronCore",
			Regex:        relabel.MustNewRegexp("(.*)"),
			Replacement:  "${1}",
			Action:       relabel.Replace,
		},
		// hacky way to inject static values (clusterName) to label set without additional processor
		// relabel looks up an existing label then creates another label with given key (TargetLabel) and value (static)
		{
			SourceLabels: model.LabelNames{"instance_id"},
			TargetLabel:  "ClusterName",
			Regex:        relabel.MustNewRegexp("(.*)"),
			Replacement:  hostinfo.GetClusterName(),
			Action:       relabel.Replace,
		},
		{
			SourceLabels: model.LabelNames{"instance_id"},
			TargetLabel:  "NodeName",
			Regex:        relabel.MustNewRegexp("(.*)"),
			Replacement:  os.Getenv("NODE_NAME"),
			Action:       relabel.Replace,
		},
	}
}
