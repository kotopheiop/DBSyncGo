package database

import (
	"DBSyncGo/config"
	sshConnection "DBSyncGo/ssh"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/jfcote87/sshdb/mysql"
	"golang.org/x/crypto/ssh"
)

func CheckLocalDatabaseConnection(cfg config.Database) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", cfg.User, cfg.Password, cfg.Address, cfg.Port, cfg.Name)
	dbLocalConnection, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("⛔ Не удалось подключиться к базе данных: %v", err)
	}
	defer dbLocalConnection.Close()

	err = dbLocalConnection.Ping()
	if err != nil {
		log.Fatalf("⛔ Не удалось подключиться к базе данных: %v", err)
	} else {
		log.Println("✅ Подключение к локальной базе данных установлено")
	}
}

func CheckRemoteDatabaseConnection(cfg config.Config) {
	tunnel, err := sshConnection.CreateSSHTunnel(cfg)
	if err != nil {
		log.Fatalf("⛔ Не удалось создать SSH туннель: %v", err)
	} else {
		log.Println("✅ SSH туннель создан")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", cfg.RemoteDB.User, cfg.RemoteDB.Password, cfg.RemoteDB.Address, cfg.RemoteDB.Port, cfg.RemoteDB.Name)
	connector, err := tunnel.OpenConnector(mysql.TunnelDriver, dsn)
	if err != nil {
		log.Fatalf("⛔ Не удалось открыть коннектор %s - %v", dsn, err)
	} else {
		log.Println("✅ Коннектор открыт")
	}

	dbRemoteConnection := sql.OpenDB(connector)
	defer dbRemoteConnection.Close()

	err = dbRemoteConnection.Ping()
	if err != nil {
		if errors.Is(err, sql.ErrConnDone) {
			log.Fatalf("⛔ Соединение с базой данных было закрыто: %v", err)
		} else if errors.Is(err, sql.ErrNoRows) {
			log.Fatalf("⛔ Не найдено строк в базе данных: %v", err)
		} else if errors.Is(err, sql.ErrTxDone) {
			log.Fatalf("⛔ Транзакция уже завершена: %v", err)
		} else {
			log.Fatalf("⛔ Неизвестная ошибка при подключении к базе данных: %v", err)
		}
	} else {
		log.Println("✅ Удалось подключиться к удаленной базе данных")
	}
}

func DumpAndLoadTable(cfg config.Config, table string, session *ssh.Session) error {
	log.Printf("⏳ Начинаю дамп таблицы %s\n", table)

	dumpCmd := exec.Command("mysqldump",
		"--skip-lock-tables", "--set-gtid-purged=OFF", "--no-tablespaces",
		"-u", cfg.LocalDB.User, "-p"+cfg.LocalDB.Password,
		"-h", cfg.LocalDB.Address,
		cfg.LocalDB.Name, table,
	)
	dumpCmd.Env = append(os.Environ(), "MYSQL_PWD="+cfg.LocalDB.Password)
	dumpOut, err := dumpCmd.Output()
	if err != nil {
		return err
	}

	log.Printf("✅ Завершил дамп таблицы %s, начинаю загрузку на удаленный сервер\n", table)

	session.Stdin = bytes.NewReader(dumpOut)
	session.Setenv("MYSQL_PWD", cfg.RemoteDB.Password)
	err = session.Run("mysql -u " + cfg.RemoteDB.User + " -p" + cfg.RemoteDB.Password + " " + cfg.RemoteDB.Name)
	if err != nil {
		return err
	}

	log.Printf("✅ Завершил загрузку таблицы %s на удаленный сервер\n", table)
	return nil
}
