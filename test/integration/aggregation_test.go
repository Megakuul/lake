package integration

import (
	"testing"
	"time"

	lake "github.com/megakuul/lake"
)

type Request struct {
	lake.Table  `name:"request" sort:"latency:desc,Timestamp:asc"`
	Timestamp   lake.Int
	Latency     lake.Int    `parquet:"latency"`
	Endpoint    lake.String `parquet:"endpoint,ignoreme123"`
	RequestorIQ lake.Float  `parquet:"req_iq"`

	ignore lake.Float `parquet:"ignore"`
	Static float64    `parquet:"static"`
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

	// act
	orderedByEndpoint, err := lake.Query[Request]().
		Where(Request{
			Timestamp: lake.FilterInt(lake.After(now.Add(-time.Second))),
			Static:    800,                                 // should not do anything
			ignore:    lake.FilterFloat(lake.Eq(1337.420)), // should not do anything
		}).
		GroupBy(Request{
			Endpoint:  lake.GroupString(lake.Exact()),
			Timestamp: lake.GroupInt(lake.Date(lake.DateYear)), // should do nothing since everything happened in the same day
		}).
		Aggregate(Request{
			Latency:     lake.AggrInt(lake.Avg),
			RequestorIQ: lake.AggrFloat(lake.Sum),
		}).
		Scan(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}

	// assert
	if len(orderedByEndpoint) != 2 {
		t.Fatalf("incorrect grouping operation expected '2' result buckets got '%d'", len(orderedByEndpoint))
	}
	for _, result := range orderedByEndpoint {
		// group by datetrunc year should set this to start of the year -> now - 1 leap year should be before this.
		if now.Add(-time.Hour * 8784).After(time.Unix(result.Timestamp.Data-1, 0)) {
			t.Fatalf("grouping did not correctly set timestamp group")
		}
		switch result.Endpoint.Data {
		case "ironclad":
			expected, combined := 0.0, append(ironcladLatencies[:], generatedIroncladLatency...)
			for _, latency := range combined {
				expected += float64(latency)
			}
			expected /= float64(len(combined))
			if result.Latency.Data != int64(expected) {
				t.Fatalf("ironclad latency average is off (expected '%d' got '%d')", result.Latency.Data, int(expected))
			}
		case "elephant":
			expected, combined := 0.0, elephantLatencies[:]
			for _, latency := range combined {
				expected += float64(latency)
			}
			expected /= float64(len(combined))
			if result.Latency.Data != int64(expected) {
				t.Fatalf("elephant latency average is off (expected '%d' got '%d')", result.Latency.Data, int(expected))
			}
		default:
			t.Fatalf("unexpected endpoint in returned row result")
		}
	}
}
