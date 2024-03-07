// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsemfexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter"

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/amazon-contributing/opentelemetry-collector-contrib/extension/awsmiddleware"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsemfexporter/internal/appsignals"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/awsutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/cwlogs"
)

const (
	// OutputDestination Options
	outputDestinationCloudWatch = "cloudwatch"
	outputDestinationStdout     = "stdout"

	// AppSignals EMF config
	appSignalsMetricNamespace    = "AppSignals"
	appSignalsLogGroupNamePrefix = "/aws/appsignals/"
)

type emfExporter struct {
	pusherMap        map[cwlogs.StreamKey]cwlogs.Pusher
	svcStructuredLog *cwlogs.Client
	config           *Config

	metricTranslator metricTranslator

	pusherMapLock sync.Mutex
	retryCnt      int
	collectorID   string

	processResourceLabels func(map[string]string)
}

// newEmfExporter creates a new exporter using exporterhelper
func newEmfExporter(config *Config, set exporter.CreateSettings) (*emfExporter, error) {
	if config == nil {
		return nil, errors.New("emf exporter config is nil")
	}

	config.logger = set.Logger

	// create AWS session
	awsConfig, session, err := awsutil.GetAWSConfigSession(set.Logger, &awsutil.Conn{}, &config.AWSSessionSettings)
	if err != nil {
		return nil, err
	}

	// create CWLogs client with aws session config
	svcStructuredLog := cwlogs.NewClient(set.Logger,
		awsConfig,
		set.BuildInfo,
		config.LogGroupName,
		config.LogRetention,
		config.Tags,
		session,
		cwlogs.WithEnabledContainerInsights(config.IsEnhancedContainerInsights()),
		cwlogs.WithEnabledAppSignals(config.IsAppSignalsEnabled()),
	)

	collectorIdentifier, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	emfExporter := &emfExporter{
		svcStructuredLog:      svcStructuredLog,
		config:                config,
		metricTranslator:      newMetricTranslator(*config),
		retryCnt:              *awsConfig.MaxRetries,
		collectorID:           collectorIdentifier.String(),
		pusherMap:             map[cwlogs.StreamKey]cwlogs.Pusher{},
		processResourceLabels: func(map[string]string) {},
	}

	if config.IsAppSignalsEnabled() {
		userAgent := appsignals.NewUserAgent()
		svcStructuredLog.Handlers().Build.PushBackNamed(userAgent.Handler())
		emfExporter.processResourceLabels = userAgent.Process
	}

	config.logger.Warn("the default value for DimensionRollupOption will be changing to NoDimensionRollup" +
		"in a future release. See https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/23997 for more" +
		"information")

	return emfExporter, nil
}

func (emf *emfExporter) pushMetricsData(_ context.Context, md pmetric.Metrics) error {
	emf.logMd(md, "Before_emf_conversion")
	rms := md.ResourceMetrics()
	labels := map[string]string{}
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		am := rm.Resource().Attributes()
		if am.Len() > 0 {
			am.Range(func(k string, v pcommon.Value) bool {
				labels[k] = v.Str()
				return true
			})
		}
	}
	emf.config.logger.Info("Start processing resource metrics", zap.Any("labels", labels))
	emf.processResourceLabels(labels)

	groupedMetrics := make(map[any]*groupedMetric)
	defaultLogStream := fmt.Sprintf("otel-stream-%s", emf.collectorID)
	outputDestination := emf.config.OutputDestination

	for i := 0; i < rms.Len(); i++ {
		err := emf.metricTranslator.translateOTelToGroupedMetric(rms.At(i), groupedMetrics, emf.config)
		if err != nil {
			return err
		}
	}

	emf.logGroupMetrics(groupedMetrics, "GroupedMetrics_before_emf_conversion")

	for _, groupedMetric := range groupedMetrics {
		putLogEvent, err := translateGroupedMetricToEmf(groupedMetric, emf.config, defaultLogStream)
		if err != nil {
			if errors.Is(err, errMissingMetricsForEnhancedContainerInsights) {
				emf.config.logger.Debug("Dropping empty putLogEvents for enhanced container insights", zap.Error(err))
				continue
			}
			return err
		}

		// Currently we only support two options for "OutputDestination".
		if strings.EqualFold(outputDestination, outputDestinationStdout) {
			if putLogEvent != nil &&
				putLogEvent.InputLogEvent != nil &&
				putLogEvent.InputLogEvent.Message != nil {
				emf.config.logger.Info("Emf_log_after_processing" + *putLogEvent.InputLogEvent.Message)
				fmt.Println(*putLogEvent.InputLogEvent.Message)
			}
		} else if strings.EqualFold(outputDestination, outputDestinationCloudWatch) {
			emfPusher := emf.getPusher(putLogEvent.StreamKey)
			if emfPusher != nil {
				returnError := emfPusher.AddLogEntry(putLogEvent)
				if returnError != nil {
					return wrapErrorIfBadRequest(returnError)
				}
			}
		}
	}

	if strings.EqualFold(outputDestination, outputDestinationCloudWatch) {
		for _, emfPusher := range emf.listPushers() {
			returnError := emfPusher.ForceFlush()
			if returnError != nil {
				// TODO now we only have one logPusher, so it's ok to return after first error occurred
				err := wrapErrorIfBadRequest(returnError)
				if err != nil {
					emf.config.logger.Error("Error force flushing logs. Skipping to next logPusher.", zap.Error(err))
				}
				return err
			}
		}
	}

	emf.config.logger.Info("Finish processing resource metrics", zap.Any("labels", labels))

	return nil
}

func (emf *emfExporter) getPusher(key cwlogs.StreamKey) cwlogs.Pusher {
	var ok bool
	if _, ok = emf.pusherMap[key]; !ok {
		emf.pusherMap[key] = cwlogs.NewPusher(key, emf.retryCnt, *emf.svcStructuredLog, emf.config.logger)
	}
	return emf.pusherMap[key]
}

func (emf *emfExporter) listPushers() []cwlogs.Pusher {
	emf.pusherMapLock.Lock()
	defer emf.pusherMapLock.Unlock()

	var pushers []cwlogs.Pusher
	for _, pusher := range emf.pusherMap {
		pushers = append(pushers, pusher)
	}
	return pushers
}

func (emf *emfExporter) start(_ context.Context, host component.Host) error {
	if emf.config.MiddlewareID != nil {
		awsmiddleware.TryConfigure(emf.config.logger, host, *emf.config.MiddlewareID, awsmiddleware.SDKv1(emf.svcStructuredLog.Handlers()))
	}
	return nil
}

// shutdown stops the exporter and is invoked during shutdown.
func (emf *emfExporter) shutdown(_ context.Context) error {
	for _, emfPusher := range emf.listPushers() {
		returnError := emfPusher.ForceFlush()
		if returnError != nil {
			err := wrapErrorIfBadRequest(returnError)
			if err != nil {
				emf.config.logger.Error("Error when gracefully shutting down emf_exporter. Skipping to next logPusher.", zap.Error(err))
			}
		}
	}

	return emf.metricTranslator.Shutdown()
}

func wrapErrorIfBadRequest(err error) error {
	var rfErr awserr.RequestFailure
	if errors.As(err, &rfErr) && rfErr.StatusCode() < 500 {
		return consumererror.NewPermanent(err)
	}
	return err
}

func (d *emfExporter) logMd(md pmetric.Metrics, mdName string) {
	var logMessage strings.Builder

	logMessage.WriteString(fmt.Sprintf("\"%s\" : {\n", mdName))
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

	d.config.logger.Info(logMessage.String())
}

func (d *emfExporter) logGroupMetrics(groupedMetrics map[any]*groupedMetric, groupedMetricsName string) {
	var logMessage strings.Builder

	logMessage.WriteString(fmt.Sprintf("\"%s\" : {\n", groupedMetricsName))
	for key, metric := range groupedMetrics {
		logMessage.WriteString(fmt.Sprintf("Key: %v, Value: %v, Metadata:%v", key, metric.metrics, metric.labels))
	}

	d.config.logger.Info(logMessage.String())
}
