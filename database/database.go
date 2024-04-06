package database

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"os/exec"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"DBSyncGo/config"
)

func CheckDatabaseConnection(db config.Database, sshKeyPath string, isRemote bool) {
	var err error

	if isRemote {
		checkDBCmd := exec.Command("ssh", "-i", sshKeyPath, fmt.Sprintf("%s@%s", db.User, db.Address), "mysql", "-u", db.User, "-p"+db.Password, "-e", fmt.Sprintf("'USE %s'", db.Name))
		err = checkDBCmd.Run()
		if err != nil {
			log.Fatalf("Не удалось подключиться к базе данных %s: %v", db.Name, err)
		}
	} else {
		var database *sql.DB
		database, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", db.User, db.Password, db.Address, db.Port, db.Name))
		if err != nil {
			log.Fatalf("Не удалось подключиться к базе данных %s: %v", db.Name, err)
		}
		defer database.Close()

		err = database.Ping()
		if err != nil {
			log.Fatalf("Не удалось подключиться к базе данных %s: %v", db.Name, err)
		}
	}
}

func DumpAndLoadTable(cfg config.Config, table string, timeFormat string) {
	log.Printf("%s: Начинаю дамп таблицы %s\n", time.Now().Format(timeFormat), table)

	dumpCmd := exec.Command("mysqldump", "--single-transaction", "-u", cfg.LocalDB.User, "-p"+cfg.LocalDB.Password, "-h", cfg.LocalDB.Address, cfg.LocalDB.Name, table)
	dumpOut, err := dumpCmd.Output()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%s: Завершил дамп таблицы %s, начинаю загрузку на удаленный сервер\n", time.Now().Format(timeFormat), table)

	loadCmd := exec.Command("ssh", "-i", cfg.SSHKeyPath, fmt.Sprintf("%s@%s", cfg.RemoteDB.User, cfg.RemoteServer), "mysql", "-u", cfg.RemoteDB.User, "-p"+cfg.RemoteDB.Password, cfg.RemoteDB.Name)
	loadCmd.Stdin = bytes.NewReader(dumpOut)
	err = loadCmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%s: Завершил загрузку таблицы %s на удаленный сервер\n", time.Now().Format(timeFormat), table)
}
