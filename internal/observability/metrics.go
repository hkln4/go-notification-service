package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every metric the service exposes. Each binary updates the
// ones it owns (API: HTTP and created; worker: delivery; API sampler: queue
// depth and outbox backlog); Prometheus scrapes them all.
//
// All methods are safe on a nil *Metrics, so tests and tools can pass nil.
type Metrics struct {
	reg *prometheus.Registry

	httpDuration     *prometheus.HistogramVec
	created          *prometheus.CounterVec
	delivered        *prometheus.CounterVec
	deliveryDuration *prometheus.HistogramVec
	endToEnd         *prometheus.HistogramVec
	queueDepth       *prometheus.GaugeVec
	outboxBacklog    prometheus.Gauge
	breakerState     *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by route pattern and status code.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route", "status"}),
		created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Notifications accepted by the API (replays not counted).",
		}, []string{"channel"}),
		delivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_delivered_total",
			Help: "Delivery attempts by outcome: sent, retry, dead_letter, permanent_fail.",
		}, []string{"channel", "result"}),
		deliveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_delivery_duration_seconds",
			Help:    "Provider call latency.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"channel"}),
		endToEnd: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_end_to_end_seconds",
			Help:    "Time from creation (or scheduledAt) to successful delivery, retries included.",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900},
		}, []string{"channel"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notification_queue_depth",
			Help: "Messages ready in each RabbitMQ queue.",
		}, []string{"queue"}),
		outboxBacklog: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "notification_outbox_backlog",
			Help: "Outbox rows due but not yet published.",
		}),
		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Per channel: 0 closed, 1 half-open, 2 open.",
		}, []string{"channel"}),
	}
	m.reg.MustRegister(
		m.httpDuration, m.created, m.delivered, m.deliveryDuration, m.endToEnd,
		m.queueDepth, m.outboxBacklog, m.breakerState,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveHTTP(method, route, status string, d time.Duration) {
	if m != nil {
		m.httpDuration.WithLabelValues(method, route, status).Observe(d.Seconds())
	}
}

func (m *Metrics) Created(channel string) {
	if m != nil {
		m.created.WithLabelValues(channel).Inc()
	}
}

func (m *Metrics) Delivered(channel, result string, latency time.Duration) {
	if m != nil {
		m.delivered.WithLabelValues(channel, result).Inc()
		m.deliveryDuration.WithLabelValues(channel).Observe(latency.Seconds())
	}
}

func (m *Metrics) EndToEnd(channel string, d time.Duration) {
	if m != nil {
		m.endToEnd.WithLabelValues(channel).Observe(d.Seconds())
	}
}

func (m *Metrics) QueueDepth(queue string, n int) {
	if m != nil {
		m.queueDepth.WithLabelValues(queue).Set(float64(n))
	}
}

func (m *Metrics) OutboxBacklog(n int) {
	if m != nil {
		m.outboxBacklog.Set(float64(n))
	}
}

func (m *Metrics) BreakerState(channel string, state int) {
	if m != nil {
		m.breakerState.WithLabelValues(channel).Set(float64(state))
	}
}
