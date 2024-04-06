package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

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

	checkConfigParameters(cfg)

	if cfg.MaxRoutines == 0 {
		cfg.MaxRoutines = 5
	}

	return cfg
}

func checkConfigParameters(cfg Config) {
	if len(cfg.Tables) == 0 || cfg.SSHUser == "" || cfg.RemoteServer == "" || cfg.SSHKeyPath == "" || cfg.LocalDB.Name == "" || cfg.LocalDB.User == "" || cfg.LocalDB.Password == "" || cfg.LocalDB.Address == "" || cfg.RemoteDB.Name == "" || cfg.RemoteDB.User == "" || cfg.RemoteDB.Password == "" || cfg.RemoteDB.Address == "" {
		log.Fatal("Все параметры в файле конфигурации должны быть заполнены")
	}
}
