package neuron

import (
	"context"
	"strconv"

	ci "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/containerinsight"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const (
	neuronHardwareInfoKey       = "neuron_hardware"
	neuronCorePerDeviceKey      = "neuroncore_per_device_count"
	neuronCoreAttributeKey      = "neuroncore"
	neuronDeviceAttributeKey    = "neuron_device_index"
	neuronCoreResourceName      = "aws.amazon.com/neuroncore"
	neuronDeviceResourceName    = "aws.amazon.com/neurondevice"
	neuronDeviceResourceNameAlt = "aws.amazon.com/neuron"
	neuronCoreDimension         = "NeuronCore"
	neuronDeviceDimension       = "NeuronDevice"
)

type PodAttributesDecoratorConsumer struct {
	NextConsumer      consumer.Metrics
	PodResourcesStore *stores.PodResourcesStore // replace with podResourcesApi
	Logger            *zap.Logger
}

func (pdc *PodAttributesDecoratorConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: true,
	}
}

func (pdc *PodAttributesDecoratorConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	md = pdc.neuronMetricsProcess(md)
	return pdc.NextConsumer.ConsumeMetrics(ctx, md)
}

func (pdc *PodAttributesDecoratorConsumer) neuronMetricsProcess(md pmetric.Metrics) pmetric.Metrics {
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
			neuronCoresPerDevice, _ := strconv.Atoi(neuronCoresPerDeviceValue.AsString())

			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				pdc.AddPodCorrelationAttributes(getMetricDatapoints(m), neuronCoresPerDevice)
			}
		}
	}
	return md
}

func getMetricDatapoints(m pmetric.Metric) pmetric.NumberDataPointSlice {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints()
	default:
		return pmetric.NewNumberDataPointSlice()
	}
}

func (pdc *PodAttributesDecoratorConsumer) AddPodCorrelationAttributes(metricDatapoints pmetric.NumberDataPointSlice, neuronCoresPerDevice int) {
	for i := 0; i < metricDatapoints.Len(); i++ {
		attributes := metricDatapoints.At(i).Attributes()
		var containerInfo *stores.ContainerInfo

		if neuronDeviceIndex, neuronDeviceIndexPresent := attributes.Get(neuronDeviceAttributeKey); neuronDeviceIndexPresent {
			// get container info from neuronDeviceIndex
			neuronDeviceIndex := neuronDeviceIndex.AsString()
			if neuronDeviceIndexPresent {
				containerInfo = pdc.getContainerInfoForNueronDeviceIndex(neuronDeviceIndex)
				attributes.PutStr(neuronDeviceDimension, neuronDeviceIndex)
			}
		} else if neuronCoreIndex, neuronCoreIndexPresent := attributes.Get(neuronCoreAttributeKey); neuronCoreIndexPresent {
			// get container info from neuronCore
			containerInfo = pdc.PodResourcesStore.GetContainerInfo(neuronCoreIndex.AsString(), neuronCoreResourceName)
			neuronDeviceIndex := getNeuronDeviceIndexFromCoreAttribute(neuronCoreIndex, neuronCoresPerDevice)
			if containerInfo == nil {
				// else get container info from calculated neuronDeviceIndex
				containerInfo = pdc.getContainerInfoForNueronDeviceIndex(neuronDeviceIndex)
			}
			attributes.PutStr(neuronDeviceAttributeKey, neuronDeviceIndex)
			attributes.PutStr(neuronCoreDimension, neuronCoreIndex.AsString())
			attributes.PutStr(neuronDeviceDimension, neuronDeviceIndex)
		}
		populateAttributes(&attributes, containerInfo)
	}
}

func (pdc *PodAttributesDecoratorConsumer) getContainerInfoForNueronDeviceIndex(neuronDeviceIndex string) *stores.ContainerInfo {
	containerInfo := pdc.PodResourcesStore.GetContainerInfo(neuronDeviceIndex, neuronDeviceResourceName)
	if containerInfo == nil {
		// Alt resource name is to support backward compatibility in neuron monitor : https://awsdocs-neuron.readthedocs-hosted.com/en/latest/containers/tutorials/k8s-setup.html
		containerInfo = pdc.PodResourcesStore.GetContainerInfo(neuronDeviceIndex, neuronDeviceResourceNameAlt)
	}
	return containerInfo
}

func populateAttributes(attributes *pcommon.Map, containerInfo *stores.ContainerInfo) {
	if containerInfo != nil {
		attributes.PutStr(ci.AttributeContainerName, containerInfo.ContainerName)
		attributes.PutStr(ci.AttributeK8sPodName, containerInfo.PodName)
		attributes.PutStr(ci.AttributePodName, containerInfo.PodName)
		attributes.PutStr(ci.AttributeK8sNamespace, containerInfo.Namespace)
		attributes.PutStr(ci.AttributeFullPodName, containerInfo.PodName+"."+containerInfo.Namespace)
	}
}

func getNeuronDeviceIndexFromCoreAttribute(neuronCoreIndex pcommon.Value, neuronCoresPerDevice int) string {
	neuronCoreIndexIntVal, _ := strconv.Atoi(neuronCoreIndex.AsString())
	return strconv.Itoa(neuronCoreIndexIntVal / neuronCoresPerDevice)
}
