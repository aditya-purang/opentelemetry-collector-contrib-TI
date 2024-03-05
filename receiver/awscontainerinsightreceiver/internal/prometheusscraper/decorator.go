// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusscraper

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	ci "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/containerinsight"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores"
)

const (
	gpuUtil                = "DCGM_FI_DEV_GPU_UTIL"
	gpuMemUtil             = "DCGM_FI_DEV_FB_USED_PERCENT"
	gpuMemUsed             = "DCGM_FI_DEV_FB_USED"
	gpuMemTotal            = "DCGM_FI_DEV_FB_TOTAL"
	gpuTemperature         = "DCGM_FI_DEV_GPU_TEMP"
	gpuPowerDraw           = "DCGM_FI_DEV_POWER_USAGE"
	neuronCorePerDeviceKey = "neuroncore_per_device_count"
	neuronHardwareInfoKey  = "neuron_hardware"
)

var metricToUnit = map[string]string{
	gpuUtil:        "Percent",
	gpuMemUtil:     "Percent",
	gpuMemUsed:     "Bytes",
	gpuMemTotal:    "Bytes",
	gpuTemperature: "None",
	gpuPowerDraw:   "None",
}

// GPU decorator acts as an interceptor of metrics before the scraper sends them to the next designated consumer
type decorateConsumer struct {
	containerOrchestrator string
	nextConsumer          consumer.Metrics
	k8sDecorator          Decorator
	metricModifier        MetricModifier
	logger                *zap.Logger
}

func (dc *decorateConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: true,
	}
}

func (dc *decorateConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	resourceTags := make(map[string]string)
	md, _ = podCorrelationProcess(md, &dc.metricModifier)
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		// get resource attributes
		ras := rms.At(i).Resource().Attributes()
		ras.Range(func(k string, v pcommon.Value) bool {
			resourceTags[k] = v.AsString()
			return true
		})
		ilms := rms.At(i).ScopeMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ms := ilms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				converted := ci.ConvertToFieldsAndTags(m, dc.logger)
				var rcis []*stores.RawContainerInsightsMetric
				for _, pair := range converted {
					rcis = append(rcis, stores.NewRawContainerInsightsMetricWithData(ci.TypeGpuContainer, pair.Fields, pair.Tags, dc.logger))
				}

				decorated := dc.decorateMetrics(rcis)
				dc.updateAttributes(m, decorated)
				if unit, ok := metricToUnit[m.Name()]; ok {
					m.SetUnit(unit)
				}
			}
		}
	}
	dc.logMd(md, "Scrapper_md")

	md, _ = neuronMetricsProcess(md, &dc.metricModifier)
	dc.logMd(md, "Neuron_md")
	return dc.nextConsumer.ConsumeMetrics(ctx, md)
}

type Decorator interface {
	Decorate(stores.CIMetric) stores.CIMetric
	Shutdown() error
}

func (dc *decorateConsumer) decorateMetrics(rcis []*stores.RawContainerInsightsMetric) []*stores.RawContainerInsightsMetric {
	var result []*stores.RawContainerInsightsMetric
	if dc.containerOrchestrator != ci.EKS {
		return result
	}
	for _, rci := range rcis {
		// add tags for EKS
		out := dc.k8sDecorator.Decorate(rci)
		if out != nil {
			result = append(result, out.(*stores.RawContainerInsightsMetric))
		}
	}
	return result
}

func (dc *decorateConsumer) updateAttributes(m pmetric.Metric, rcis []*stores.RawContainerInsightsMetric) {
	if len(rcis) == 0 {
		return
	}
	var dps pmetric.NumberDataPointSlice
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps = m.Gauge().DataPoints()
	case pmetric.MetricTypeSum:
		dps = m.Sum().DataPoints()
	default:
		dc.logger.Warn("Unsupported metric type", zap.String("metric", m.Name()), zap.String("type", m.Type().String()))
	}
	if dps.Len() == 0 {
		return
	}
	for i := 0; i < dps.Len(); i++ {
		if i >= len(rcis) {
			// this shouldn't be the case, but it helps to avoid panic
			continue
		}
		attrs := dps.At(i).Attributes()
		tags := rcis[i].Tags
		for tk, tv := range tags {
			// type gets set with metrictransformer while duplicating metrics at different resource levels
			if tk == ci.MetricType {
				continue
			}
			attrs.PutStr(tk, tv)
		}
	}
}

func (dc *decorateConsumer) Shutdown() error {
	if dc.k8sDecorator != nil {
		return dc.k8sDecorator.Shutdown()
	}
	return nil
}
func (dc *decorateConsumer) logMd(md pmetric.Metrics, mdName string) {
	var logMessage strings.Builder

	logMessage.WriteString("\"METRICS_MD\" : {\n")
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rs := rms.At(i)
		ilms := rs.ScopeMetrics()
		logMessage.WriteString(fmt.Sprintf("\t\"ResourceMetric_%d\": {\n", i))
		for j := 0; j < ilms.Len(); j++ {
			ils := ilms.At(j)
			metrics := ils.Metrics()
			logMessage.WriteString(fmt.Sprintf("\t\t\"ScopeMetric_%d\": {\n", j))
			logMessage.WriteString(fmt.Sprintf("\t\t\"Metrics_%d\": [\n", j))

			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				logMessage.WriteString(fmt.Sprintf("\t\t\t\"Metric_%d\": {\n", k))
				logMessage.WriteString(fmt.Sprintf("\t\t\t\t\"name\": \"%s\",\n", m.Name()))

				var datapoints pmetric.NumberDataPointSlice
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					datapoints = m.Gauge().DataPoints()
				case pmetric.MetricTypeSum:
					datapoints = m.Sum().DataPoints()
				default:
					datapoints = pmetric.NewNumberDataPointSlice()
				}

				logMessage.WriteString("\t\t\t\t\"datapoints\": [\n")
				for yu := 0; yu < datapoints.Len(); yu++ {
					logMessage.WriteString("\t\t\t\t\t{\n")
					logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"attributes\": \"%v\",\n", datapoints.At(yu).Attributes().AsRaw()))
					logMessage.WriteString(fmt.Sprintf("\t\t\t\t\t\t\"value\": %v,\n", datapoints.At(yu).DoubleValue()))
					logMessage.WriteString("\t\t\t\t\t},\n")
				}
				logMessage.WriteString("\t\t\t\t],\n")
				logMessage.WriteString("\t\t\t},\n")
			}
			logMessage.WriteString("\t\t],\n")
			logMessage.WriteString("\t\t},\n")
		}
		logMessage.WriteString("\t},\n")
	}
	logMessage.WriteString("},\n")

	dc.logger.Info(logMessage.String())
}

func neuronMetricsProcess(md pmetric.Metrics, modifier *MetricModifier) (pmetric.Metrics, error) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rs := rms.At(i)
		ilms := rs.ScopeMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ils := ilms.At(j)
			metrics := ils.Metrics()

			newMetrics := pmetric.NewMetricSlice()
			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				modifier.ModifyMetric(m).MoveAndAppendTo(newMetrics)
			}
			newMetrics.CopyTo(metrics)
		}
	}
	return md, nil
}

func podCorrelationProcess(md pmetric.Metrics, modifier *MetricModifier) (pmetric.Metrics, error) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rs := rms.At(i)
		ilms := rs.ScopeMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ils := ilms.At(j)
			metrics := ils.Metrics()

			neuronHardwareInfo := pmetric.Metric{}
			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				if m.Name() == neuronHardwareInfoKey {
					neuronHardwareInfo = m
					break
				}
			}

			neuronCoresPerDeviceValue, _ := neuronHardwareInfo.Sum().DataPoints().At(0).Attributes().Get(neuronCorePerDeviceKey)
			neuronCoresPerDevice := neuronCoresPerDeviceValue.Int()

			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				modifier.AddPodCorrelationAttributes(GetMetricDatapoints(m), neuronCoresPerDevice)
			}
		}
	}
	return md, nil
}
