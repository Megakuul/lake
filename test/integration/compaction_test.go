package integration

import (
	"fmt"
	"math"
	"testing"

	"github.com/google/uuid"
	lake "github.com/megakuul/lakedb"
)

type GameStatistic struct {
	lake.Table `name:"game_statistic" sort:"performance:desc,accuracy:desc,hits:desc"`

	GameID      lake.String `parquet:"game_id"`
	Player      lake.String `parquet:"player"`
	Performance lake.Float  `parquet:"performance"`
	Hits        lake.Int    `parquet:"hits"`
	Accuracy    lake.Float  `parquet:"accuracy"`

	ignore lake.Float `parquet:"ignore"`
	Static float64    `parquet:"static"`
}

func testCompaction(t *testing.T, bucket *lake.Bucket) {
	// prepare
	bestGame, weakestGame := uuid.NewString(), uuid.NewString()
	ingestors := []*lake.Ingestor[GameStatistic]{
		lake.NewIngestor[GameStatistic](bucket),
		lake.NewIngestor[GameStatistic](bucket),
		lake.NewIngestor[GameStatistic](bucket),
		lake.NewIngestor[GameStatistic](bucket),
	}
	ingestors[1].Insert(GameStatistic{
		GameID:      lake.NewString(weakestGame),
		Player:      lake.NewString("The Rizzler"),
		Performance: lake.NewFloat(-67),
		Hits:        lake.NewInt(-67),
		Accuracy:    lake.NewFloat(-0.69),
	})
	ingestors[3].Insert(GameStatistic{
		GameID:      lake.NewString(bestGame),
		Player:      lake.NewString("John fucking Cena"),
		Performance: lake.NewFloat(200),
		Hits:        lake.NewInt(math.MaxInt),
		Accuracy:    lake.NewFloat(999999.9999),
	})
	for ingestorIdx, ingestor := range ingestors {
		for i := range 2 {
			ingestor.Insert(GameStatistic{
				GameID:      lake.NewString(uuid.NewString()),
				Player:      lake.NewString(fmt.Sprintf("Big Baba %d %d", ingestorIdx, i)),
				Performance: lake.NewFloat(float64(i)),
				Hits:        lake.NewInt(int64(i % 100)),
				Accuracy:    lake.NewFloat(float64(i%100) * 0.1),
			})
		}
		if err := ingestor.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	// act
	compactor := lake.NewCompactor[GameStatistic](bucket, 128_000_000)
	if err := compactor.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}

	games, err := lake.Query[GameStatistic]().Scan(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}

	println(games[0].Performance.Data)
	println(games[len(games)-1].Performance.Data)
	for row, game := range games {
		if game.GameID.Data == bestGame {
			println("game on row", row)
		}
	}
	// assert
	if len(games) != 4*100000+2 {
		t.Fatalf("invalid compaction (entries lost or duplicated in process)")
	}
	if games[0].GameID.Data != bestGame {
		t.Fatalf("invalid sorting after compaction")
	}
	if games[len(games)-1].GameID.Data != weakestGame {
		t.Fatalf("invalid sorting after compaction")
	}
}
