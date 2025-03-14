package config

import (
	"encoding/json"
	"fmt"
	"github.com/go-ini/ini"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type DatabaseConfig struct {
	Host     string
	Port     string
	DBName   string
	User     string
	Password string
}

type Database struct {
	Name     string `json:"name"`     // имя базы данных
	User     string `json:"user"`     // имя пользователя
	Password string `json:"password"` // пароль
	Address  string `json:"address"`  // адрес сервера базы данных
	Port     string `json:"port"`     // порт сервера базы данных
}

type Config struct {
	Tables       []string `json:"tables"`        // список таблиц для дампа
	SSHUser      string   `json:"ssh_user"`      // пользователь для SSH-соединения
	RemoteServer string   `json:"remote_server"` // адрес удаленного сервера
	LocalDB      Database `json:"local_db"`      // информация о подключении к локальной базе данных
	RemoteDB     Database `json:"remote_db"`     // информация о подключении к удаленной базе данных
	SSHKeyPath   string   `json:"ssh_key_path"`  // путь до ключа для SSH-соединения
	MaxRoutines  int      `json:"max_routines"`  // максимальное количество горутин
	CompressDump bool     `json:"compress_dump"` // необходимо ли сжимать данные true\false
	Debug        bool     `json:"debug"`         // вывод информации о запускаемых командах true\false
}

func LoadConfig(filename string) Config {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	var cfg Config
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	myConfig, _ := getMyCnfData("client")

	/*todo подумать, нужны ли остальные значения из конфига*/
	if myConfig.Password != "" {
		cfg.LocalDB.Password = myConfig.Password
	}

	checkConfigParameters(cfg)

	if cfg.MaxRoutines == 0 {
		cfg.MaxRoutines = 5
	}

	return cfg
}

func checkConfigParameters(cfg Config) {
	missingParams := []string{}

	if len(cfg.Tables) == 0 {
		missingParams = append(missingParams, "Tables")
	}
	if cfg.SSHUser == "" {
		missingParams = append(missingParams, "SSHUser")
	}
	if cfg.RemoteServer == "" {
		missingParams = append(missingParams, "RemoteServer")
	}
	if cfg.SSHKeyPath == "" {
		missingParams = append(missingParams, "SSHKeyPath")
	}
	if cfg.LocalDB.Name == "" {
		missingParams = append(missingParams, "LocalDB.Name")
	}
	if cfg.LocalDB.User == "" {
		missingParams = append(missingParams, "LocalDB.User")
	}
	if cfg.LocalDB.Address == "" {
		missingParams = append(missingParams, "LocalDB.Address")
	}
	if cfg.RemoteDB.Name == "" {
		missingParams = append(missingParams, "RemoteDB.Name")
	}
	if cfg.RemoteDB.User == "" {
		missingParams = append(missingParams, "RemoteDB.User")
	}
	if cfg.RemoteDB.Address == "" {
		missingParams = append(missingParams, "RemoteDB.Address")
	}

	if len(missingParams) > 0 {
		log.Fatal("Все параметры в файле конфигурации должны быть заполнены. Незаполненные параметры: ", strings.Join(missingParams, ", "))
	}
}

func getMyCnfData(profile string) (*DatabaseConfig, error) {
	cfg, err := ini.LoadSources(ini.LoadOptions{
		AllowBooleanKeys:    true,
		IgnoreInlineComment: true,
	}, os.Getenv("HOME")+"/.my.cnf")
	if err != nil {
		return nil, err
	}
	for _, s := range cfg.Sections() {
		if profile != "" && s.Name() != profile {
			continue
		}
		config := &DatabaseConfig{
			Host:     s.Key("host").String(),
			Port:     s.Key("port").String(),
			DBName:   s.Key("dbname").String(),
			User:     s.Key("user").String(),
			Password: s.Key("password").String(),
		}
		return config, nil
	}
	return nil, fmt.Errorf("не найден профиль %s в ~/.my.cnf", profile)
}
