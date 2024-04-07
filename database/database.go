package database

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/jfcote87/sshdb/mysql"
	"golang.org/x/crypto/ssh"
	"log"
	"os/exec"
	"time"

	"DBSyncGo/config"
	sshConnection "DBSyncGo/ssh"
)

func CheckLocalDatabaseConnection(cfg config.Database) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", cfg.User, cfg.Password, cfg.Address, cfg.Port, cfg.Name)
	dbLocalConnection, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("⛔ Не удалось подключиться к базе данных: %v", err)
	}

	err = dbLocalConnection.Ping()
	if err != nil {
		log.Fatalf("⛔ Не удалось подключиться к базе данных: %v", err)
	}
}

func CheckRemoteDatabaseConnection(cfg config.Config) {
	var err error
	tunnel, err := sshConnection.CreateSSHTunnel(cfg)
	if err != nil {
		log.Fatalf("⛔ Не удалось создать SSH туннель: %v", err)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", cfg.RemoteDB.User, cfg.RemoteDB.Password, cfg.RemoteDB.Address, cfg.RemoteDB.Port, cfg.RemoteDB.Name)

	connector, err := tunnel.OpenConnector(mysql.TunnelDriver, dsn)
	if err != nil {
		log.Fatalf("⛔ Не удалось открыть коннектор %s - %v", dsn, err)
	}

	dbRemoteConnection := sql.OpenDB(connector)

	err = dbRemoteConnection.Ping()
	if err != nil {
		log.Fatalf("⛔ Не удалось подключиться к базе данных: %v", err)
	} else {
		log.Println("✅ Удалось подключиться к базе данных")
	}
}

func DumpAndLoadTable(cfg config.Config, table string, timeFormat string) error {
	log.Printf("%s: Начинаю дамп таблицы %s\n", time.Now().Format(timeFormat), table)

	dumpCmd := exec.Command("mysqldump",
		"--skip-lock-tables --set-gtid-purged=OFF --no-tablespaces",
		"-u",
		cfg.LocalDB.User,
		"-p"+cfg.LocalDB.Password,
		"-h",
		cfg.LocalDB.Address,
		cfg.LocalDB.Name,
		table,
	)
	dumpOut, err := dumpCmd.Output()
	if err != nil {
		return err
	}

	log.Printf("%s: Завершил дамп таблицы %s, начинаю загрузку на удаленный сервер\n", time.Now().Format(timeFormat), table)

	client, err := sshConnection.CreateSSHClient(cfg)
	if err != nil {
		return err
	}
	defer func(client *ssh.Client) {
		err := client.Close()
		if err != nil {
			log.Fatalf("⛔ Не удалось закрыть SSH клиент: %v", err)
		}
	}(client)

	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer func(session *ssh.Session) {
		err := session.Close()
		if err != nil {
			log.Fatalf("⛔ Не удалось закрыть SSH сессию: %v", err)
		}
	}(session)

	session.Stdin = bytes.NewReader(dumpOut)
	err = session.Run("mysql -u " + cfg.RemoteDB.User + " -p" + cfg.RemoteDB.Password + " " + cfg.RemoteDB.Name)
	if err != nil {
		return err
	}

	log.Printf("%s: Завершил загрузку таблицы %s на удаленный сервер\n", time.Now().Format(timeFormat), table)
	return nil
}
