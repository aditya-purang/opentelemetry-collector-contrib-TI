package prometheusscraper

import (
	"fmt"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

const (
	awsNeuronMetric                             = "neuron_"
	neuronCoreAttributeKey                      = "neuroncore"
	neuronDeviceAttributeKey                    = "neuron_device_index"
	neuronCoreResourceName                      = "aws.amazon.com/neuroncore"
	neuronDeviceResourceName                    = "aws.amazon.com/neurondevice"
	neuronDeviceResourceNameAlt                 = "aws.amazon.com/neuron"
	aggregated_metric_suffix                    = "_aggregated"
	logTypeSuffix                               = "AwsNeuron"
	NeuronCoreUtilization                       = "neuroncore_utilization"
	NeuronCoreMemoryUtilizationConstants        = "neuroncore_memory_usage_constants"
	NeuronCoreMemoryUtilizationModelCode        = "neuroncore_memory_usage_model_code"
	NeuronCoreMemoryUtilizationSharedScratchpad = "neuroncore_memory_usage_model_shared_scratchpad"
	NeuronCoreMemoryUtilizationRuntimeMemory    = "neuroncore_memory_usage_runtime_memory"
	NeuronCoreMemoryUtilizationTensors          = "neuroncore_memory_usage_tensors"
	NeuronDeviceHardwareEccEvents               = "neurondevice_hw_ecc_events"
	NeuronExecutionStatus                       = "neuron_execution_status"
	NeuronExecutionErrors                       = "neuron_execution_errors"
	NeuronRuntimeMemoryUsage                    = "neurondevice_runtime_memory_used_bytes"
	NeuronInstanceInfo                          = "instance_info"
	NeuronHardwareInfo                          = "neuron_hardware_info"
	NeuronExecutionLatency                      = "neuron_execution_latency_seconds"
	TypeNode                                    = "Node"
	TypePod                                     = "Pod"
	TypeContainer                               = "Container"
	MetricType                                  = "Type"
)

var metricNameToSubtypeAttributeKey = map[string]string{
	NeuronDeviceHardwareEccEvents: "event_type",
	NeuronExecutionErrors:         "error_type",
	NeuronExecutionStatus:         "status_type",
}

var metricsToBeDuplicated = map[string][]string{
	NeuronExecutionErrors:                       {TypeNode},
	NeuronExecutionStatus:                       {TypeNode},
	NeuronRuntimeMemoryUsage:                    {TypeNode},
	NeuronCoreMemoryUtilizationConstants:        {TypeContainer, TypePod, TypeNode},
	NeuronCoreMemoryUtilizationModelCode:        {TypeContainer, TypePod, TypeNode},
	NeuronCoreMemoryUtilizationSharedScratchpad: {TypeContainer, TypePod, TypeNode},
	NeuronCoreMemoryUtilizationRuntimeMemory:    {TypeContainer, TypePod, TypeNode},
	NeuronCoreMemoryUtilizationTensors:          {TypeContainer, TypePod, TypeNode},
	NeuronCoreUtilization:                       {TypeContainer, TypePod, TypeNode},
	NeuronInstanceInfo:                          {},
	NeuronHardwareInfo:                          {},
	// container and pod only if correlated
	NeuronDeviceHardwareEccEvents: {TypeContainer, TypePod, TypeNode},
	NeuronExecutionLatency:        {TypeNode},
}

type MetricModifier struct {
	logger            *zap.Logger
	podResourcesStore *stores.PodResourcesStore // replace with podResourcesApi
}

func NewMetricModifier(logger *zap.Logger, podResourcesStore *stores.PodResourcesStore) *MetricModifier {
	d := &MetricModifier{
		logger:            logger,
		podResourcesStore: podResourcesStore,
	}
	return d
}

func (d *MetricModifier) ModifyMetric(originalMetric pmetric.Metric) pmetric.MetricSlice {
	// only decorate GPU metrics
	// another option is to separate GPU of its own pipeline to minimize extra processing of metrics

	newMetricSlice := pmetric.NewMetricSlice()
	//if _, isNeuronMetric := metricsToBeDuplicated[originalMetric.Name()]; !isNeuronMetric {
	//	return newMetricSlice
	//}

	/*
		1. add pod correlation : done, should work
		2. add aggregated metric for the metrics which need to be aggregated : done, needs fixing
		4. modify metric name based on labels : done, needs fixing
		3. duplicate metric for node pod container based on their criteria : done
	*/

	originalMetricName := originalMetric.Name()
	d.logger.Info("MetricModifier metric name : " + originalMetricName)
	metricDatapoints := GetMetricDatapoints(originalMetric)
	slice := createAggregatatedSumMetrics(originalMetric, metricDatapoints)
	slice.MoveAndAppendTo(newMetricSlice)
	d.logSlice(slice, "intermediate slice")
	finalSlice := duplicateMetrics(newMetricSlice, originalMetricName, metricDatapoints)
	d.logSlice(&finalSlice, "final slice")

	return finalSlice
}

func createAggregatatedSumMetrics(originalMetric pmetric.Metric, metricDatapoints pmetric.NumberDataPointSlice) *pmetric.MetricSlice {
	slice := pmetric.NewMetricSlice()
	if subtypeKey, exists := metricNameToSubtypeAttributeKey[originalMetric.Name()]; exists && originalMetric.Type() == pmetric.MetricTypeSum {
		aggregatedMetric := pmetric.NewMetric()

		aggregatedMetric.SetName(originalMetric.Name() + aggregated_metric_suffix)
		sum := aggregatedMetric.SetEmptySum()

		aggregatedDatapoint := sum.DataPoints().AppendEmpty()
		metricDatapoints.At(0).CopyTo(aggregatedDatapoint)
		aggregatedDatapoint.Attributes().Remove(subtypeKey)
		aggregatedValue := 0.0
		for i := 0; i < metricDatapoints.Len(); i++ {
			originalDatapoint := metricDatapoints.At(i)
			aggregatedValue += originalDatapoint.DoubleValue()

			newNameMetric := pmetric.NewMetric()
			newNameSum := newNameMetric.SetEmptySum()
			newNameDatapoint := newNameSum.DataPoints().AppendEmpty()
			originalDatapoint.CopyTo(newNameDatapoint)
			subtypeValue, _ := newNameDatapoint.Attributes().Get(subtypeKey)
			newNameMetric.SetName(originalMetric.Name() + "_" + subtypeValue.Str())
			newNameMetric.CopyTo(slice.AppendEmpty())
		}
		aggregatedDatapoint.SetDoubleValue(aggregatedValue)

		aggregatedMetric.CopyTo(slice.AppendEmpty())
	} else {
		originalMetric.CopyTo(slice.AppendEmpty())
	}

	return &slice
}

func duplicateMetrics(metricsSlice pmetric.MetricSlice, originalMetricName string, originalMetricDatapoints pmetric.NumberDataPointSlice) pmetric.MetricSlice {
	newMetricsSlice := pmetric.NewMetricSlice()
	duplicateTypePrefix := metricsToBeDuplicated[originalMetricName]

	duplicateForNodeOnly := false
	if originalMetricName == NeuronDeviceHardwareEccEvents {
		podname, exists := originalMetricDatapoints.At(0).Attributes().Get("PodName")
		if !exists || len(podname.Str()) == 0 {
			duplicateForNodeOnly = true
		}
	}

	for i := 0; i < metricsSlice.Len(); i++ {
		metric := metricsSlice.At(i)
		if duplicateForNodeOnly {
			duplicateMetricForType(metric, TypeNode).CopyTo(newMetricsSlice.AppendEmpty())
		} else {
			for _, prefix := range duplicateTypePrefix {
				duplicateMetricForType(metric, prefix).CopyTo(newMetricsSlice.AppendEmpty())
			}
		}
	}

	return newMetricsSlice
}

func duplicateMetricForType(metric pmetric.Metric, duplicateType string) *pmetric.Metric {
	metricCopy := pmetric.NewMetric()
	metric.CopyTo(metricCopy)
	metricCopy.SetName(TypeNode + "_" + metricCopy.Name())

	datapoints := GetMetricDatapoints(metricCopy)
	for i := 0; i < datapoints.Len(); i++ {
		datapoints.At(i).Attributes().PutStr(MetricType, duplicateType+logTypeSuffix)
	}

	return &metricCopy
}

func GetMetricDatapoints(m pmetric.Metric) pmetric.NumberDataPointSlice {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints()
	default:
		return pmetric.NewNumberDataPointSlice()
	}
}

func (d *MetricModifier) AddPodCorrelationAttributes(metricDatapoints pmetric.NumberDataPointSlice, neuronCoresPerDevice int) {
	for i := 0; i < metricDatapoints.Len(); i++ {
		attributes := metricDatapoints.At(i).Attributes()
		if neuronCoreIndex, neuronCoreIndexPresent := attributes.Get(neuronCoreAttributeKey); neuronCoreIndexPresent {
			d.logger.Info("neuronCoreIndex string= " + neuronCoreIndex.AsString())
			neuronCoreIndexIntVal, _ := strconv.Atoi(neuronCoreIndex.AsString())
			neuronDeviceIndex := neuronCoreIndexIntVal / neuronCoresPerDevice

			neuronDeviceIndexString := strconv.Itoa(neuronDeviceIndex)
			neuronCoreIndexString := strconv.Itoa(neuronCoreIndexIntVal)

			containerInfo := d.podResourcesStore.GetContainerInfo(neuronCoreIndexString, neuronCoreResourceName)
			if containerInfo == nil {
				containerInfo = d.podResourcesStore.GetContainerInfo(neuronDeviceIndexString, neuronDeviceResourceName)
				if containerInfo == nil {
					// Alt resource name is to support backward compatibility in neuron monitor : https://awsdocs-neuron.readthedocs-hosted.com/en/latest/containers/tutorials/k8s-setup.html
					containerInfo = d.podResourcesStore.GetContainerInfo(neuronDeviceIndexString, neuronDeviceResourceNameAlt)
				}
			}
			attributes.PutStr(neuronDeviceAttributeKey, strconv.Itoa(neuronCoreIndexIntVal))

			if containerInfo != nil {
				attributes.PutStr("ContainerName", containerInfo.ContainerName)
				attributes.PutStr("PodName", containerInfo.PodName)
				attributes.PutStr("Namespace", containerInfo.Namespace)
				attributes.PutStr("FullPodname", containerInfo.PodName+"."+containerInfo.Namespace)
			}
		}

		if neuronDeviceIndex, neuronDeviceIndexPresent := attributes.Get(neuronDeviceAttributeKey); neuronDeviceIndexPresent {
			neuronDeviceIndexString := neuronDeviceIndex.AsString()
			if neuronDeviceIndexPresent {
				containerInfo := d.podResourcesStore.GetContainerInfo(neuronDeviceIndexString, neuronDeviceResourceName)
				if containerInfo == nil {
					// Alt resource name is to support backward compatibility in neuron monitor : https://awsdocs-neuron.readthedocs-hosted.com/en/latest/containers/tutorials/k8s-setup.html
					containerInfo = d.podResourcesStore.GetContainerInfo(neuronDeviceIndexString, neuronDeviceResourceNameAlt)
				}

				if containerInfo != nil {
					attributes.PutStr("ContainerName", containerInfo.ContainerName)
					attributes.PutStr("PodName", containerInfo.PodName)
					attributes.PutStr("Namespace", containerInfo.Namespace)
					attributes.PutStr("FullPodname", containerInfo.PodName+"."+containerInfo.Namespace)
				}
			}
		}
	}
}

func (d *MetricModifier) logSlice(slice *pmetric.MetricSlice, name string) {
	var logMessage strings.Builder

	logMessage.WriteString(fmt.Sprintf("printing Slice %s: {", name))
	for i := 0; i < slice.Len(); i++ {
		metric := slice.At(i)
		dps := GetMetricDatapoints(metric)

		logMessage.WriteString("{")
		logMessage.WriteString("name= " + metric.Name())
		logMessage.WriteString(", unit= " + metric.Type().String())
		logMessage.WriteString(fmt.Sprintf(", datapoints= %v", dps))
		logMessage.WriteString("}, ")
	}
	logMessage.WriteString(" }")
	d.logger.Info(logMessage.String())
}
