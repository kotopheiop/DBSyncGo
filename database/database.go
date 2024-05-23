package database

import (
	"DBSyncGo/config"
	sshConnection "DBSyncGo/ssh"
	"bytes"
	"compress/gzip"
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

	dumpOut, err := dumpTable(cfg, table)
	if err != nil {
		return err
	}

	var dataToSend []byte
	if cfg.CompressDump {
		dataToSend, err = compressData(dumpOut)
		if err != nil {
			return err
		}
		log.Printf("✅ Завершил сжатие дампа таблицы %s (размер: %s), начинаю загрузку на удаленный сервер\n", table, formatSize(len(dataToSend)))
	} else {
		dataToSend = dumpOut
		log.Printf("✅ Завершил дамп таблицы %s без сжатия (размер: %s), начинаю загрузку на удаленный сервер\n", table, formatSize(len(dataToSend)))
	}

	err = loadToRemote(cfg, dataToSend, session, cfg.CompressDump)
	if err != nil {
		return err
	}

	log.Printf("✅ Завершил загрузку таблицы %s на удаленный сервер\n", table)
	return nil
}

func dumpTable(cfg config.Config, table string) ([]byte, error) {
	dumpCmd := exec.Command("mysqldump",
		"--skip-lock-tables", "--set-gtid-purged=OFF", "--no-tablespaces",
		"-u", cfg.LocalDB.User, "-p"+cfg.LocalDB.Password,
		"-h", cfg.LocalDB.Address,
		cfg.LocalDB.Name, table,
	)
	dumpCmd.Env = append(os.Environ(), "MYSQL_PWD="+cfg.LocalDB.Password)
	return dumpCmd.Output()
}

func compressData(data []byte) ([]byte, error) {
	var compressedData bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedData)
	_, err := gzipWriter.Write(data)
	if err != nil {
		return nil, err
	}
	err = gzipWriter.Close()
	if err != nil {
		return nil, err
	}
	return compressedData.Bytes(), nil
}

func loadToRemote(cfg config.Config, data []byte, session *ssh.Session, isCompressed bool) error {
	session.Stdin = bytes.NewReader(data)
	session.Setenv("MYSQL_PWD", cfg.RemoteDB.Password)

	var runCmd string
	if isCompressed {
		runCmd = "gzip -d | mysql -u " + cfg.RemoteDB.User + " -p" + cfg.RemoteDB.Password + " " + cfg.RemoteDB.Name
	} else {
		runCmd = "mysql -u " + cfg.RemoteDB.User + " -p" + cfg.RemoteDB.Password + " " + cfg.RemoteDB.Name
	}
	return session.Run(runCmd)
}

func formatSize(size int) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case size >= GB:
		return fmt.Sprintf("%.2f GB", float64(size)/GB)
	case size >= MB:
		return fmt.Sprintf("%.2f MB", float64(size)/MB)
	case size >= KB:
		return fmt.Sprintf("%.2f KB", float64(size)/KB)
	default:
		return fmt.Sprintf("%d B", size)
	}
}
