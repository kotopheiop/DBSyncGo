package main

import (
	"DBSyncGo/config"
	"DBSyncGo/database"
	"sync"
	"time"
)

func main() {
	cfg := config.LoadConfig("config.json")

	database.CheckDatabaseConnection(cfg.LocalDB, cfg.SSHKeyPath, false)
	database.CheckDatabaseConnection(cfg.RemoteDB, cfg.SSHKeyPath, true)

	var wg sync.WaitGroup
	sem := make(chan bool, cfg.MaxRoutines)

	timeFormat := time.RFC3339

	for _, table := range cfg.Tables {
		wg.Add(1)
		sem <- true
		go func(table string) {
			defer wg.Done()
			defer func() { <-sem }()

			database.DumpAndLoadTable(cfg, table, timeFormat)
		}(table)
	}

	wg.Wait()
}
