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
	"os/exec"
	"time"

	mysqlConfig "github.com/go-sql-driver/mysql"
	"github.com/jfcote87/sshdb/mysql"
	"golang.org/x/crypto/ssh"
)

func CheckLocalDatabaseConnection(cfg config.Config) {
	// Создание строки подключения

	config := mysqlConfig.Config{
		User:                 cfg.LocalDB.User,
		Passwd:               cfg.LocalDB.Password,
		Net:                  "tcp",
		Addr:                 cfg.LocalDB.Address,
		DBName:               cfg.LocalDB.Name,
		AllowNativePasswords: true,
		ParseTime:            true,
		Loc:                  time.Local,
	}

	if cfg.Debug {
		log.Printf("ℹ️ Строка подключения: %s\n", config.FormatDSN())
	}

	dbLocalConnection, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		log.Fatalf("⛔ Не удалось подключиться к локальной базе данных: %v", err)
	}
	defer func(dbLocalConnection *sql.DB) {
		err := dbLocalConnection.Close()
		if err != nil {

		}
	}(dbLocalConnection)

	err = dbLocalConnection.Ping()
	if err != nil {
		log.Fatalf("⛔ Не удалось подключиться к локальной базе данных: %v", err)
	} else {
		log.Println("✅ Соединение с локальной базой данных установлено")
	}
}

func CheckRemoteDatabaseConnection(cfg config.Config) {
	tunnel, err := sshConnection.CreateSSHTunnel(cfg)
	if err != nil {
		log.Fatalf("⛔ Не удалось создать SSH туннель: %v", err)
	} else {
		log.Println("✅ SSH туннель создан")
	}

	dsn := ""
	if cfg.RemoteDB.Password == "" {
		dsn = fmt.Sprintf("%s@tcp(%s:%s)/%s?parseTime=true", cfg.RemoteDB.User, cfg.RemoteDB.Address, cfg.RemoteDB.Port, cfg.RemoteDB.Name)
	} else {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", cfg.RemoteDB.User, cfg.RemoteDB.Password, cfg.RemoteDB.Address, cfg.RemoteDB.Port, cfg.RemoteDB.Name)
	}
	connector, err := tunnel.OpenConnector(mysql.TunnelDriver, dsn)
	if err != nil {
		log.Fatalf("⛔ Не удалось открыть коннектор %s - %v", dsn, err)
	} else {
		log.Println("✅ Коннектор открыт")
	}

	dbRemoteConnection := sql.OpenDB(connector)
	defer func(dbRemoteConnection *sql.DB) {
		err := dbRemoteConnection.Close()
		if err != nil {

		}
	}(dbRemoteConnection)

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
		log.Println("✅ Соединение с удалённой базой данных установлено")
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
	startTimeLoadToRemote := time.Now()
	err = loadToRemote(cfg, dataToSend, session, cfg.CompressDump)
	if err != nil {
		return err
	}

	log.Printf("✅ Завершил загрузку таблицы %s на удаленный сервер (%s)\n", table, time.Since(startTimeLoadToRemote))
	return nil
}

func dumpTable(cfg config.Config, table string) ([]byte, error) {
	var arg []string

	if cfg.LocalDB.Password != "" {
		arg = append(arg, "-p"+cfg.LocalDB.Password)
	}

	arg = append(arg, "--skip-lock-tables",
		"-u", cfg.LocalDB.User,
		"-h", cfg.LocalDB.Address,
		"--set-gtid-purged=OFF",
		"--no-tablespaces",
		"--add-drop-table",
		"--compact",
		cfg.LocalDB.Name,
		table)
	dumpCmd := exec.Command("mysqldump", arg...)

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
	var err error

	clearAndLoadCmd := fmt.Sprintf(
		"mysql -u %s %s",
		cfg.RemoteDB.User, cfg.RemoteDB.Name,
	)

	if isCompressed {
		clearAndLoadCmd = fmt.Sprintf(
			"gzip -d | mysql -u %s %s",
			cfg.RemoteDB.User, cfg.RemoteDB.Name,
		)
	}
	if cfg.Debug {
		log.Println("ℹ️ Запущена команда:", clearAndLoadCmd)
	}

	session.Stdin = bytes.NewReader(data)

	// Захват вывода ошибок
	var stdoutBuf, stderrBuf bytes.Buffer
	session.Stdout = &stdoutBuf
	session.Stderr = &stderrBuf

	err = session.Run(clearAndLoadCmd)
	if err != nil {
		log.Printf("⛔ Произошла ошибка: %v", err)
		log.Printf("⛔ Stdout: %s", stdoutBuf.String())
		log.Printf("⛔ Stderr: %s", stderrBuf.String())
		return err
	}

	if cfg.Debug && stdoutBuf.String() != "" {
		log.Println("ℹ️ Вывод команды:", stdoutBuf.String())
	}
	return nil
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
