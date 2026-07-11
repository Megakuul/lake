package integration

import (
	"testing"
	"time"

	lake "github.com/megakuul/lake"
	"github.com/parquet-go/parquet-go"
)

type Log struct {
	Timestamp  lake.Int `parquet:"timestamp"`
	Service    lake.String
	Message    lake.String `parquet:"message"`
	Importance lake.Float  `parquet:"importance"`

	ignore lake.Float `parquet:"ignore"`
	Static float64    `parquet:"static"`
}

func (l Log) Name() string {
	return "log"
}

func (l Log) Sorting() parquet.SortingOption {
	return parquet.SortingColumns(
		parquet.Ascending("timestamp"),
	)
}

func testFilter(t *testing.T, bucket *lake.Bucket) {
	now := time.Now()
	// prepare
	ingestorA, ingestorB := lake.NewIngestor[Log](bucket), lake.NewIngestor[Log](bucket)
	ingestorA.Insert(t.Context(), Log{
		Timestamp:  lake.NewInt(now.Unix()),
		Service:    lake.NewString("elephant"),
		Importance: lake.NewFloat(99.9),
		Message:    lake.NewString("an elephant broke out of the cage; stop him..."),
		ignore:     lake.NewFloat(187.0),
		Static:     420,
	})
	ingestorB.Insert(t.Context(), Log{
		Timestamp:  lake.NewInt(now.Unix() + 1),
		Service:    lake.NewString("elephant"),
		Importance: lake.NewFloat(20.0),
		Message:    lake.NewString("we caught him, however, now he is eating the control panel"),
	})
	// dump some garbage data inbetween
	for i := range int64(50000) {
		ingestorB.Insert(t.Context(), Log{
			Timestamp:  lake.NewInt(50 + i),
			Service:    lake.NewString("elephantor"),
			Importance: lake.NewFloat(0.0),
			Message:    lake.NewString("GARBAGGEEGGEGEG"),
		})
	}
	ingestorA.Insert(t.Context(), Log{
		Timestamp:  lake.NewInt(now.Unix() + 2),
		Service:    lake.NewString("camera"),
		Importance: lake.NewFloat(50.0),
		Message:    lake.NewString("I detected an elephant in the room"),
	})
	// dump some garbage data inbetween
	for i := range int64(50000) {
		ingestorA.Insert(t.Context(), Log{
			Timestamp:  lake.NewInt(50 + i),
			Service:    lake.NewString("elephantor"),
			Importance: lake.NewFloat(0.0),
			Message:    lake.NewString("GARBAGGEEGGEGEG"),
		})
	}
	ingestorB.Insert(t.Context(), Log{
		Timestamp:  lake.NewInt(now.Unix() + 3),
		Importance: lake.NewFloat(1.0),
		Message:    lake.NewString("wait guys this is my first day... what should I do here?"),
	})
	ingestorA.Insert(t.Context(), Log{
		Timestamp:  lake.NewInt(now.Add(time.Hour).Unix()),
		Service:    lake.NewString("camera"),
		Importance: lake.NewFloat(13.37),
		Message:    lake.NewString("the elephant in the room seems to be gone"),
		Static:     50,
		ignore:     lake.NewFloat(1337.420),
	})
	if err := ingestorB.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := ingestorA.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	// act
	logsBeforeIncident, err := lake.Query[Log]().
		Where(Log{
			Timestamp: lake.FilterInt(lake.After(now.Add(-time.Second)), lake.Before(now.Add(time.Minute))),
			Static:    800,
			ignore:    lake.FilterFloat(lake.Eq(1337.420)), // should not do anything
		}).
		Scan(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}
	logsAfterIncident, err := lake.Query[Log]().
		Where(Log{
			Timestamp: lake.FilterInt(lake.After(now.Add(time.Minute))),
			Static:    -200,                                // should not do anything
			ignore:    lake.FilterFloat(lake.Eq(1337.420)), // should not do anything
		}).
		Scan(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}
	logsFromCamera, err := lake.Query[Log]().
		Where(Log{
			Service:    lake.FilterString(lake.Eq("camera")),
			Importance: lake.FilterFloat(lake.Gte(13.37), lake.Lt(50.1)),
			Static:     9999,                                // should not do anything
			ignore:     lake.FilterFloat(lake.Eq(1337.420)), // should not do anything
		}).
		Scan(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}
	logsFromOthers, err := lake.Query[Log]().
		Where(Log{
			Service: lake.FilterString(lake.In("elephant", "")),
			Static:  50,                                  // should not do anything
			ignore:  lake.FilterFloat(lake.Eq(1337.420)), // should not do anything
		}).
		Scan(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}

	// assert
	if len(logsBeforeIncident) != 4 {
		t.Fatalf("filter operation did not work properly; expected '4' got '%d'", len(logsBeforeIncident))
	}
	if len(logsAfterIncident) != 1 {
		t.Fatalf("filter operation did not work properly; expected '1' got '%d'", len(logsAfterIncident))
	}
	if len(logsFromCamera) != 2 {
		t.Fatalf("filter operation did not work properly; expected '2' got '%d'", len(logsFromCamera))
	}
	if len(logsFromOthers) != 3 {
		t.Fatalf("filter operation did not work properly; expected '3' got '%d'", len(logsFromOthers))
	}
}
