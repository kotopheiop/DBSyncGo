package main

import (
	"log"
	"sync"
	"time"

	"DBSyncGo/config"
	"DBSyncGo/database"
)

func main() {
	cfg := config.LoadConfig("config.json")

	database.CheckLocalDatabaseConnection(cfg.LocalDB)
	database.CheckRemoteDatabaseConnection(cfg)

	var wg sync.WaitGroup
	sem := make(chan bool, cfg.MaxRoutines)

	timeFormat := time.RFC3339

	for _, table := range cfg.Tables {
		wg.Add(1)
		sem <- true
		go func(table string) {
			defer wg.Done()
			defer func() { <-sem }()

			err := database.DumpAndLoadTable(cfg, table, timeFormat)
			if err != nil {
				log.Println(err)
			}
		}(table)
	}

	wg.Wait()
}
