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
	"github.com/parquet-go/parquet-go"
)

type Request struct {
	Timestamp lakedb.Int    `parquet:"timestamp,asc"`
	Latency   lakedb.Int    `parquet:"latency"`
	Endpoint  lakedb.String `parquet:"endpoint"`
}

func (r Request) Name() string {
	return "request"
}

func (r Request) Sorting() parquet.SortingOption {
	return nil
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

	start := time.Now()
	ingestor := lakedb.NewIngestor(bucket, Request{})
	for i := range int64(2) {
		err = ingestor.Insert(t.Context(), Request{
			Timestamp: lakedb.NewInt(69),
			Latency:   lakedb.NewInt(i),
			Endpoint:  lakedb.NewString("Another Enedpoint"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = ingestor.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	ingestor = lakedb.NewIngestor(bucket, Request{})
	for i := range int64(5000000) {
		err = ingestor.Insert(t.Context(), Request{
			Timestamp: lakedb.NewInt(187),
			Latency:   lakedb.NewInt(i + 500000),
			Endpoint:  lakedb.NewString("Another Enedpoint"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = ingestor.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	println("insert ", time.Since(start).String())

	start = time.Now()
	rows, err := lakedb.Query[Request]().
		Where(Request{
			Timestamp: lakedb.NewIntOp().Lte(time.Now().Unix()).End(),
			Latency:   lakedb.NewIntOp().Lte(500000).End(),
			Endpoint:  lakedb.NewStringFilter().Contains("Another Enedpoint").End(),
		}).
		Limit(2).
		Scan(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}

	windows := []Request{
		{Latency: lakedb.NewIntOp().Avg().End(), Timestamp: lakedb.NewIntOp().Max().End()},
	}
	err = lakedb.Query[Request]().Aggregate(t.Context(), bucket, windows)
	if err != nil {
		t.Fatal(err)
	}

	println("average ", windows[0].Latency.Data)
	println("max ", windows[0].Timestamp.Data)

	println("query ", time.Since(start).String())
	println("total:")
	println(len(rows))
	t.Fail()
}
