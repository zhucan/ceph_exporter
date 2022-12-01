package ceph

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// BucketUsageCollector displays statistics about each bucket in the Ceph cluster.
type BucketUsageCollector struct {
	config  string
	user    string
	logger  *logrus.Logger
	version *Version

	// Number of bytes sent by the RADOS Gateway.
	BytesSent *prometheus.Desc

	// Number of bytes received by the RADOS Gateway.
	BytesReceived *prometheus.Desc

	// Number of operations.
	Ops *prometheus.Desc

	// Number of successful operations.
	SuccessfulOps *prometheus.Desc

	listBucketStats func(string, string) ([]byte, error)

	showBucketUsage func(string, string, string, string) ([]byte, error)
}

func NewBucketUsageCollector(exporter *Exporter) *BucketUsageCollector {
	var (
		subSystem   = "bucket_usage"
		bucketLabel = []string{"bucket", "user", "category"}
	)

	// prometheus.NewDesc(fmt.Sprintf("%s_%s_category", cephNamespace, subSystem), "", bucketLabel, labels),
	labels := make(prometheus.Labels)
	labels["cluster"] = exporter.Cluster

	return &BucketUsageCollector{
		config:          exporter.Config,
		user:            exporter.User,
		logger:          exporter.Logger,
		version:         exporter.Version,
		listBucketStats: listBucketStats,
		showBucketUsage: showBucketUsage,

		BytesSent:     prometheus.NewDesc(fmt.Sprintf("%s_%s_bytes_sent", cephNamespace, subSystem), "Number of bytes sent by the RADOS Gateway.", bucketLabel, labels),
		BytesReceived: prometheus.NewDesc(fmt.Sprintf("%s_%s_bytes_recived", cephNamespace, subSystem), "Number of bytes received by the RADOS Gateway.", bucketLabel, labels),
		Ops:           prometheus.NewDesc(fmt.Sprintf("%s_%s_ops", cephNamespace, subSystem), "Number of operations.", bucketLabel, labels),
		SuccessfulOps: prometheus.NewDesc(fmt.Sprintf("%s_%s_successful_ops", cephNamespace, subSystem), "Number of successful operations.", bucketLabel, labels),
	}
}

func (b *BucketUsageCollector) collect(ch chan<- prometheus.Metric) error {
	buf, err := listBucketStats(b.config, b.user)
	if err != nil {
		b.logger.WithError(err).WithField("config", b.config).WithField("user", b.user).Error("error list bucket stats")
		return err
	}
	stats := &BucketStats{}
	if err := json.Unmarshal(buf, &stats.Buckets); err != nil {
		return err
	}

	var (
		wg        sync.WaitGroup
		latestErr error
	)
	for _, bt := range stats.Buckets {
		wg.Add(1)
		bucket := bt
		go func() {
			defer wg.Done()
			buf, err = b.showBucketUsage(b.config, b.user, bucket.Bucket, bucket.Owner)
			if err != nil {
				b.logger.WithError(err).WithField("bucket", bucket.Bucket).Error("error getting bucket'usage")
				latestErr = err
			}
			usage := &BucketUsage{}
			if err := json.Unmarshal(buf, usage); err != nil {
				b.logger.WithError(err).WithField("bucket", bucket.Bucket).Error("error unmarhal bucket'usage")
				latestErr = err
			}
			for _, summary := range usage.Summary {
				for _, category := range summary.Categories {
					ch <- prometheus.MustNewConstMetric(b.BytesSent, prometheus.GaugeValue, float64(category.BytesSent), bucket.Bucket, summary.User, category.Category)
					ch <- prometheus.MustNewConstMetric(b.BytesReceived, prometheus.GaugeValue, float64(category.BytesReceived), bucket.Bucket, summary.User, category.Category)
					ch <- prometheus.MustNewConstMetric(b.Ops, prometheus.GaugeValue, float64(category.Ops), bucket.Bucket, summary.User, category.Category)
					ch <- prometheus.MustNewConstMetric(b.SuccessfulOps, prometheus.GaugeValue, float64(category.SuccessfulOps), bucket.Bucket, summary.User, category.Category)
				}
			}
		}()
	}
	wg.Wait()
	return latestErr
}

// Describe fulfills the prometheus.Collector's interface and sends the descriptors
// of bucket's metrics to the given channel.
func (b *BucketUsageCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- b.BytesSent
	ch <- b.BytesReceived
	ch <- b.Ops
	ch <- b.SuccessfulOps
}

// Collect extracts the current values of all the metrics and sends them to the
// prometheus channel.
func (b *BucketUsageCollector) Collect(ch chan<- prometheus.Metric) {
	b.logger.Debug("collecting bucket usage metrics")
	if err := b.collect(ch); err != nil {
		b.logger.WithError(err).Error("error collecting bucket usage metrics")
		return
	}
}

