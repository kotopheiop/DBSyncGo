package main

import (
	"DBSyncGo/config"
	"DBSyncGo/database"
	sshConnection "DBSyncGo/ssh"
	"io"
	"log"
	"sync"
	"time"
)

func main() {
	start := time.Now()
	cfg := config.LoadConfig("config.json")

	database.CheckLocalDatabaseConnection(cfg.LocalDB)
	database.CheckRemoteDatabaseConnection(cfg)

	// Создаем SSH клиент и туннель один раз
	client, err := sshConnection.CreateSSHClient(cfg)
	if err != nil {
		log.Fatalf("⛔ Не удалось создать SSH клиент: %v", err)
	}
	defer func() {
		err := client.Close()
		if err != nil {
			log.Fatalf("⛔ Не удалось закрыть SSH клиент: %v", err)
		}
	}()

	tunnel, err := sshConnection.CreateSSHTunnel(cfg)
	if err != nil {
		log.Fatalf("⛔ Не удалось создать SSH туннель: %v", err)
	}
	defer func() {
		err := tunnel.Close()
		if err != nil {
			log.Fatalf("⛔ Не удалось закрыть SSH туннель: %v", err)
		}
	}()

	var wg sync.WaitGroup
	sem := make(chan bool, cfg.MaxRoutines)

	for _, table := range cfg.Tables {
		wg.Add(1)
		sem <- true
		go func(table string) {
			defer wg.Done()
			defer func() { <-sem }()

			session, err := client.NewSession()
			if err != nil {
				log.Println(err)
				return
			}
			defer func() {
				err := session.Close()
				if err != nil && err != io.EOF {
					log.Printf("⛔ Не удалось закрыть SSH сессию: %v", err)
				}
			}()

			err = database.DumpAndLoadTable(cfg, table, session)
			if err != nil {
				log.Println(err)
			}
		}(table)
	}

	wg.Wait()

	elapsed := time.Since(start)
	log.Printf("🎉 All done! Время выполнения: %s\n", elapsed)
}
