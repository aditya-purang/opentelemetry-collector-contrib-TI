// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusscraper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	ci "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/containerinsight"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"strings"
)

const (
	gpuUtil        = "DCGM_FI_DEV_GPU_UTIL"
	gpuMemUtil     = "DCGM_FI_DEV_FB_USED_PERCENT"
	gpuMemUsed     = "DCGM_FI_DEV_FB_USED"
	gpuMemTotal    = "DCGM_FI_DEV_FB_TOTAL"
	gpuTemperature = "DCGM_FI_DEV_GPU_TEMP"
	gpuPowerDraw   = "DCGM_FI_DEV_POWER_USAGE"
)

var _ stores.CIMetric = (*gpuMetric)(nil)

var metricToUnit = map[string]string{
	gpuUtil:        "Percent",
	gpuMemUtil:     "Percent",
	gpuMemUsed:     "Bytes",
	gpuMemTotal:    "Bytes",
	gpuTemperature: "None",
	gpuPowerDraw:   "None",
}

type gpuMetric struct {
	// key/value pairs that are typed and contain the metric (numerical) data
	fields map[string]any
	// key/value string pairs that are used to identify the metrics
	tags map[string]string
}

func (gr *gpuMetric) GetTags() map[string]string {
	return gr.tags
}

func (gr *gpuMetric) GetFields() map[string]any {
	return gr.fields
}

func (gr *gpuMetric) GetMetricType() string {
	return gr.tags[ci.MetricType]
}

func (gr *gpuMetric) AddTags(tags map[string]string) {
	for k, v := range tags {
		gr.tags[k] = v
	}
}

func (gr *gpuMetric) HasField(key string) bool {
	return gr.fields[key] != nil
}

func (gr *gpuMetric) AddField(key string, val any) {
	gr.fields[key] = val
}

func (gr *gpuMetric) GetField(key string) any {
	return gr.fields[key]
}

func (gr *gpuMetric) HasTag(key string) bool {
	return gr.tags[key] != ""
}

func (gr *gpuMetric) AddTag(key, val string) {
	gr.tags[key] = val
}

func (gr *gpuMetric) GetTag(key string) string {
	return gr.tags[key]
}

func (gr *gpuMetric) RemoveTag(key string) {
	delete(gr.tags, key)
}

// GPU decorator acts as an interceptor of metrics before the scraper sends them to the next designated consumer
type decorateConsumer struct {
	containerOrchestrator string
	nextConsumer          consumer.Metrics
	k8sDecorator          Decorator
	logger                *zap.Logger
}

func (dc *decorateConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: true,
	}
}

func prettyPrint(i interface{}) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}

func (dc *decorateConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	dc.logger.Info("Metrics Object" + prettyPrint(md))
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
				fields, tags := ci.ConvertToFieldsAndTags(m, dc.logger)
				maps.Copy(tags, resourceTags)
				rm := gpuMetric{
					fields: fields,
					tags:   tags,
				}
				if !rm.HasTag(ci.MetricType) {
					// force type to be Container to decorate with container level labels
					rm.AddTag(ci.MetricType, ci.TypeNeuron)
				}
				dc.decorateMetrics([]*gpuMetric{&rm})
				dc.updateAttributes(m, rm)
				if unit, ok := metricToUnit[m.Name()]; ok {
					m.SetUnit(unit)
				}
			}
		}
	}

	dc.logMd(md)
	return dc.nextConsumer.ConsumeMetrics(ctx, md)
}

type Decorator interface {
	Decorate(stores.CIMetric) stores.CIMetric
	Shutdown() error
}

func (dc *decorateConsumer) decorateMetrics(metrics []*gpuMetric) []*gpuMetric {
	var result []*gpuMetric
	for _, m := range metrics {
		// add tags for EKS
		if dc.containerOrchestrator == ci.EKS {
			out := dc.k8sDecorator.Decorate(m)
			if out != nil {
				result = append(result, out.(*gpuMetric))
			}
		}
	}
	return result
}

func (dc *decorateConsumer) updateAttributes(m pmetric.Metric, gm gpuMetric) {
	if len(gm.tags) < 1 {
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

	dc.logger.Info("Metric name is " + m.Name())

	if dps.Len() < 1 {
		return
	}
	attrs := dps.At(0).Attributes()
	for tk, tv := range gm.tags {
		// type gets set with metrictransformer while duplicating metrics at different resource levels
		if tk == ci.MetricType {
			continue
		}
		dc.logger.Info("Atttributes are -" + tk + ":" + tv)
		attrs.PutStr(tk, tv)
	}
}

func (dc *decorateConsumer) Shutdown() error {
	var errs error

	if dc.k8sDecorator != nil {
		errs = errors.Join(errs, dc.k8sDecorator.Shutdown())
	}
	return errs
}

func (dc *decorateConsumer) logMd(md pmetric.Metrics) {
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
