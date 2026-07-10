package integration

import (
	"testing"

	"github.com/google/uuid"
	lake "github.com/megakuul/lakedb"
)

type RehabilitationStatistic struct {
	lake.Table `name:"rehabilitation_statistic" sort:"performance:desc,points:asc,player:asc"`

	RehabilitationID lake.String `parquet:"rehabilitation_id"`
	Player           lake.String `parquet:"player"`
	Performance      lake.Float  `parquet:"performance"`
	Points           lake.Int    `parquet:"points"` // lower == better

	ignore lake.Float `parquet:"ignore"`
	Static float64    `parquet:"static"`
}

func testSorting(t *testing.T, bucket *lake.Bucket) {
	// prepare
	bestRehabilitation, weakestRehabilitation := uuid.NewString(), uuid.NewString()
	ingestorA, ingestorB := lake.NewIngestor[RehabilitationStatistic](bucket), lake.NewIngestor[RehabilitationStatistic](bucket)
	ingestorB.Insert(RehabilitationStatistic{
		RehabilitationID: lake.NewString(weakestRehabilitation),
		Player:           lake.NewString("Tylenol Jones"),
		Performance:      lake.NewFloat(1.54),
		Points:           lake.NewInt(380),
	})
	ingestorA.Insert(RehabilitationStatistic{
		RehabilitationID: lake.NewString(uuid.New().String()),
		Player:           lake.NewString("Beef Supreme"),
		Performance:      lake.NewFloat(1.87),
		Points:           lake.NewInt(420),
	})
	ingestorA.Insert(RehabilitationStatistic{
		RehabilitationID: lake.NewString(bestRehabilitation),
		Player:           lake.NewString("Tylenol Jones"),
		Performance:      lake.NewFloat(1.87),
		Points:           lake.NewInt(400),
	})
	ingestorB.Insert(RehabilitationStatistic{
		RehabilitationID: lake.NewString(uuid.New().String()),
		Player:           lake.NewString("Beef Supreme"),
		Performance:      lake.NewFloat(1.54),
		Points:           lake.NewInt(380),
	})

	if err := ingestorB.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := ingestorA.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	// act
	rehabilitations, err := lake.Query[RehabilitationStatistic]().Scan(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}

	// assert
	if rehabilitations[0].RehabilitationID.Data != bestRehabilitation {
		t.Fatalf("invalid sorting in lake ingestor")
	}
	if rehabilitations[len(rehabilitations)-1].RehabilitationID.Data != weakestRehabilitation {
		t.Fatalf("invalid sorting in lake ingestor")
	}
}
