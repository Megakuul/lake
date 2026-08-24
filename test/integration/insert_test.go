package integration

import (
	"sync"
	"testing"

	lake "github.com/megakuul/lake"
)

type ConcurrentStatistic struct {
	lake.Table `name:"concurrent_statistic" sort:"points:desc"`

	Player lake.String `parquet:"player"`
	Points lake.Int    `parquet:"points"`
}

func TestConcurrentInsert(t *testing.T) {
	ingestor := lake.NewIngestor[ConcurrentStatistic](nil)

	wg := sync.WaitGroup{}
	for range 16 {
		wg.Go(func() {
			for points := range 1000 {
				if err := ingestor.Insert(t.Context(), ConcurrentStatistic{
					Player: lake.NewString("Beef Supreme"),
					Points: lake.NewInt(points),
				}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}
