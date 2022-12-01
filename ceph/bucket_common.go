package ceph

import "os/exec"

type BucketStats struct {
	Buckets []struct {
		Bucket       string `json:"bucket"`
		NumShards    int64  `json:"num_shards"`
		ID           string `json:"id"`
		Owner        string `json:"owner"`
		Mtime        string `json:"mtime"`
		CreationTime string `json:"creation_time"`
		// TODO add other key/value
		Usage struct {
		} `json:"usage"`
		BucketQuota struct {
		} `json:"bucket_quota"`
	} `json:"buckets"`
}

type BucketUsage struct {
	Summary []struct {
		User       string `json:"user"`
		Categories []struct {
			Category      string `json:"category"`
			BytesSent     int64  `json:"bytes_sent"`
			BytesReceived int64  `json:"bytes_received"`
			Ops           int64  `json:"ops"`
			SuccessfulOps int64  `json:"successful_ops"`
		} `json:"categories"`
	} `json:"summary"`
}

// listBUcketStats list all bucket's stats in ceph cluster
func listBucketStats(config string, user string) ([]byte, error) {
	var (
		out []byte
		err error
	)

	if out, err = exec.Command(radosgwAdminPath, "-c", config, "--user", user, "bucket", "stats", "--format", "json").Output(); err != nil {
		return nil, err
	}

	return out, nil
}

// showBucketUsage show the specified bucket's usage with uid
func showBucketUsage(config string, user string, bucket string, uid string) ([]byte, error) {
	var (
		out []byte
		err error
	)

	if out, err = exec.Command(radosgwAdminPath, "-c", config, "--user", user, "usage", "show", "--bucket",
		bucket, "--categories", "put_obj,get_obj", "--show-log-entries", "false", "--uid", uid, "--format", "json").Output(); err != nil {
		return nil, err
	}

	return out, nil
}

