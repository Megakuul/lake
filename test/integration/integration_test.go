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
	lake "github.com/megakuul/lakedb"
	"github.com/parquet-go/parquet-go"
)

type Request struct {
	Timestamp lake.Int    `parquet:"timestamp"`
	Latency   lake.Float  `parquet:"latency"`
	Endpoint  lake.String `parquet:"endpoint"`
}

func (r Request) Name() string {
	return "request"
}

func (r Request) Sorting() parquet.SortingOption {
	return parquet.SortingColumns(
		parquet.Ascending("latency"),
	)
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
	bucket, err := lake.NewFromClient(t.Context(), client, "test")
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	ingestor := lake.NewIngestor[Request](bucket)
	for i := range int64(2) {
		err = ingestor.Insert(t.Context(), Request{
			Timestamp: lake.NewInt(69),
			Latency:   lake.NewFloat(float64(i)),
			Endpoint:  lake.NewString("Another Enedpoint"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = ingestor.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	ingestor = lake.NewIngestor[Request](bucket)
	for i := range int64(5000000) {
		err = ingestor.Insert(t.Context(), Request{
			Timestamp: lake.NewInt(187),
			Latency:   lake.NewFloat(float64(i + 500000)),
			Endpoint:  lake.NewString("Another Enedpoint"),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = ingestor.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	println("insert ", time.Since(start).String())

	rows, err := lake.Query[Request]().
		Where(Request{
			Timestamp: lake.FilterInt(lake.Before(time.Now())),
			Endpoint:  lake.FilterString(lake.In("Another Enedpoint")),
		}).
		Limit(2).
		Scan(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}

	start = time.Now()
	groups, err := lake.Query[Request]().Aggregate(t.Context(), bucket, Request{
		Timestamp: lake.AggrInt(lake.Count),
		// Latency:   lake.AggrFloat(lake.Sum),
	})
	if err != nil {
		t.Fatal(err)
	}
	println("aggregate ", time.Since(start).String())

	println("average ", groups[0].Latency.Data)
	println("max ", groups[0].Timestamp.Data)

	println("total:")
	println(len(rows))
	t.Fail()
}
