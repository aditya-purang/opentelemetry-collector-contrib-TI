package gpu

const (
	gpuUtil        = "DCGM_FI_DEV_GPU_UTIL"
	gpuMemUtil     = "DCGM_FI_DEV_FB_USED_PERCENT"
	gpuMemUsed     = "DCGM_FI_DEV_FB_USED"
	gpuMemTotal    = "DCGM_FI_DEV_FB_TOTAL"
	gpuTemperature = "DCGM_FI_DEV_GPU_TEMP"
	gpuPowerDraw   = "DCGM_FI_DEV_POWER_USAGE"
)

var metricToUnit = map[string]string{
	gpuUtil:        "Percent",
	gpuMemUtil:     "Percent",
	gpuMemUsed:     "Bytes",
	gpuMemTotal:    "Bytes",
	gpuTemperature: "None",
	gpuPowerDraw:   "None",
}
