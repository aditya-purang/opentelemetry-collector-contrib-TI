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

// Decorator acts as an interceptor of metrics before the scraper sends them to the next designated consumer
type DecorateConsumer struct {
	ContainerOrchestrator string
	NextConsumer          consumer.Metrics
	K8sDecorator          Decorator
	MetricToUnitMap       map[string]string
	Logger                *zap.Logger
}

func (dc *DecorateConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: true,
	}
}

func (dc *DecorateConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	resourceTags := make(map[string]string)
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
				converted := ci.ConvertToFieldsAndTags(m, dc.Logger)
				var rcis []*stores.RawContainerInsightsMetric
				for _, pair := range converted {
					rcis = append(rcis, stores.NewRawContainerInsightsMetricWithData(ci.TypeGpuContainer, pair.Fields, pair.Tags, dc.Logger))
				}

				decorated := dc.decorateMetrics(rcis)
				dc.updateAttributes(m, decorated)
				if unit, ok := dc.MetricToUnitMap[m.Name()]; ok {
					m.SetUnit(unit)
				}
			}
		}
	}
	// dc.logMd(md)
	return dc.NextConsumer.ConsumeMetrics(ctx, md)
}

type Decorator interface {
	Decorate(stores.CIMetric) stores.CIMetric
	Shutdown() error
}

func (dc *DecorateConsumer) decorateMetrics(rcis []*stores.RawContainerInsightsMetric) []*stores.RawContainerInsightsMetric {
	var result []*stores.RawContainerInsightsMetric
	if dc.ContainerOrchestrator != ci.EKS {
		return result
	}
	for _, rci := range rcis {
		// add tags for EKS
		out := dc.K8sDecorator.Decorate(rci)
		if out != nil {
			result = append(result, out.(*stores.RawContainerInsightsMetric))
		}
	}
	return result
}

func (dc *DecorateConsumer) updateAttributes(m pmetric.Metric, rcis []*stores.RawContainerInsightsMetric) {
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
		dc.Logger.Warn("Unsupported metric type", zap.String("metric", m.Name()), zap.String("type", m.Type().String()))
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

func (dc *DecorateConsumer) Shutdown() error {
	if dc.K8sDecorator != nil {
		return dc.K8sDecorator.Shutdown()
	}
	return nil
}
func (dc *DecorateConsumer) logMd(md pmetric.Metrics) {
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

	dc.Logger.Info(logMessage.String())
}
