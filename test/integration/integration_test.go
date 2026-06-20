package integration

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/megakuul/lakedb"
)

type Request struct {
	Timestamp lakedb.Int `parquet:"timestamp,asc"`
	Latency   lakedb.Int `parquet:"latency"`
	Endpoint  string     `parquet:"endpoint"`
}

func TestOperations(t *testing.T) {
	// prepare
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	server := httptest.NewServer(faker.Server())
	defer server.Close()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("ACCESS_KEY", "SECRET_KEY", "")),
		config.WithHTTPClient(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})
	_, err = client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String("test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := lakedb.NewFromClient(t.Context(), client, "test")
	if err != nil {
		t.Fatal(err)
	}
	ingestor := lakedb.NewIngestor[Request](bucket)
	for i := range int64(100000) {
		err = ingestor.Insert(t.Context(), Request{
			Timestamp: lakedb.IntValue(time.Now().Unix()),
			Latency:   lakedb.IntValue(i),
			Endpoint:  "Another Enedpoint",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = ingestor.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	err = lakedb.Query(t.Context(), bucket, Request{})
	if err != nil {
		t.Fatal(err)
	}
}
