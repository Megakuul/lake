package integration

import (
	"fmt"
	"testing"
	"time"

	lake "github.com/megakuul/lakedb"
	"github.com/parquet-go/parquet-go"
)

type Request struct {
	Timestamp   lake.Int
	Latency     lake.Int    `parquet:"latency"`
	Endpoint    lake.String `parquet:"endpoint,ignoreme123"`
	RequestorIQ lake.Float  `parquet:"req_iq"`

	ignore lake.Float `parquet:"ignore"`
	Static float64    `parquet:"static"`
}

func (r Request) Name() string {
	return "request"
}

func (r Request) Sorting() parquet.SortingOption {
	return parquet.SortingColumns(
		parquet.Ascending("timestamp"),
	)
}

func testAggregation(t *testing.T, bucket *lake.Bucket) {
	elephantLatencies := [3]int64{100, -20, 200000}
	ironcladLatencies := [3]int64{201, 0, 50}
	requestorIq := [6]float64{199.99, 87.9, 74.5, 123.5, 160.0, 106.4}

	now := time.Now()
	// prepare
	ingestorA, ingestorB := lake.NewIngestor[Request](bucket), lake.NewIngestor[Request](bucket)
	ingestorA.Insert(t.Context(), Request{
		Timestamp:   lake.NewInt(now.Unix()),
		Latency:     lake.NewInt(elephantLatencies[0]),
		Endpoint:    lake.NewString("elephant"),
		RequestorIQ: lake.NewFloat(requestorIq[0]),
		ignore:      lake.NewFloat(187.0),
		Static:      420,
	})
	ingestorB.Insert(t.Context(), Request{
		Timestamp:   lake.NewInt(now.Unix() + 1),
		Latency:     lake.NewInt(elephantLatencies[1]),
		Endpoint:    lake.NewString("elephant"),
		RequestorIQ: lake.NewFloat(requestorIq[1]),
	})
	ingestorB.Insert(t.Context(), Request{
		Timestamp:   lake.NewInt(now.Unix() + 2),
		Latency:     lake.NewInt(ironcladLatencies[0]),
		Endpoint:    lake.NewString("ironclad"),
		RequestorIQ: lake.NewFloat(requestorIq[2]),
		ignore:      lake.NewFloat(187.0),
		Static:      420,
	})
	ingestorA.Insert(t.Context(), Request{
		Timestamp:   lake.NewInt(now.Unix() + 2),
		Latency:     lake.NewInt(ironcladLatencies[1]),
		Endpoint:    lake.NewString("ironclad"),
		RequestorIQ: lake.NewFloat(requestorIq[3]),
	})
	generatedIroncladLatency, generatedRequestorIq := []int64{}, []float64{}
	for i := range int64(1000000) {
		generatedIroncladLatency = append(generatedIroncladLatency, i)
		generatedRequestorIq = append(generatedRequestorIq, float64(i)*0.3)
		if i%2 == 0 {
			ingestorB.Insert(t.Context(), Request{
				Timestamp:   lake.NewInt(now.Add(time.Hour).Unix() + i),
				Latency:     lake.NewInt(generatedIroncladLatency[i]),
				Endpoint:    lake.NewString("ironclad"),
				RequestorIQ: lake.NewFloat(generatedRequestorIq[i]),
			})
		} else {
			ingestorA.Insert(t.Context(), Request{
				Timestamp:   lake.NewInt(now.Add(time.Hour).Unix() + i),
				Latency:     lake.NewInt(generatedIroncladLatency[i]),
				Endpoint:    lake.NewString("ironclad"),
				RequestorIQ: lake.NewFloat(generatedRequestorIq[i]),
			})
		}
	}
	ingestorB.Insert(t.Context(), Request{
		Timestamp:   lake.NewInt(now.Unix() + 2),
		Latency:     lake.NewInt(elephantLatencies[2]),
		Endpoint:    lake.NewString("elephant"),
		RequestorIQ: lake.NewFloat(requestorIq[4]),
	})
	ingestorA.Insert(t.Context(), Request{
		Timestamp:   lake.NewInt(now.Unix() + 7),
		Latency:     lake.NewInt(ironcladLatencies[2]),
		Endpoint:    lake.NewString("ironclad"),
		RequestorIQ: lake.NewFloat(requestorIq[5]),
	})
	if err := ingestorA.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := ingestorB.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	// execute
	orderedByEndpoint, err := lake.Query[Request]().
		Where(Request{
			Timestamp: lake.FilterInt(lake.Before(now.Add(time.Hour * 2))),
			Endpoint:  lake.FilterString(lake.Eq("ironclad")),
			Static:    800,                                 // should not do anything
			ignore:    lake.FilterFloat(lake.Eq(1337.420)), // should not do anything
		}).
		GroupBy(Request{
			Endpoint: lake.GroupString(lake.Exact),
		}).
		Aggregate(Request{
			Latency: lake.AggrInt(lake.Avg),
		}).
		Scan(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}
	println("aggregate time: ", fmt.Sprint(time.Since(start)))

	println(len(orderedByEndpoint))
	for _, result := range orderedByEndpoint {
		println(fmt.Sprint(result.Endpoint.Data))
		println(fmt.Sprint(result.Latency.Data))
	}
	// assert
	// if orderedByService[0].Latency.Data != ironcladLatencies {
	// 	t.Fatalf("filter operation did not work properly; expected '4' got '%d'", len(logsBeforeIncident))
	// }
	t.Fail()
}
