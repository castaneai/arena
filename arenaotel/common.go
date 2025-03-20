package arenaotel

const (
	scopeName = "github.com/castaneai/arena"
)

var (
	latencyHistogramBuckets = []float64{
		.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10,
	}
)
