# DBSyncGo

DBSyncGo - это инструмент, написанный на Go, который автоматизирует процесс дампа определенных таблиц из MySQL базы данных, подключается к удаленному серверу через SSH и обновляет базу данных на нем.

Инструмментр реализован в рамках изучения языка Go.

## Особенности

- Поддержка чтения параметров из файла конфигурации
- Многопоточное выполнение для ускорения процесса
- Поддержка SSH для безопасного подключения к удаленным серверам
- Проверка подключения к базам данных перед началом работы

## Использование

1. Установите Go на вашем компьютере.
2. Склонируйте этот репозиторий.
3. Заполните файл `config.json` соответствующими данными.
4. Запустите `go run main.go` из корневой директории проекта.

## Конфигурация

Все параметры конфигурации читаются из файла `config.json`. Вот пример содержимого этого файла:

```json
{
  "tables": ["table1", "table2", "table3"], // Список таблиц, которые нужно синхронизировать
  "ssh_user": "your_ssh_username", // Имя пользователя для SSH-соединения
  "remote_server": "remote_server_address:port", // Адрес и порт удаленного сервера
  "ssh_key_path": "path_to_your_ssh_key", // Путь до файла с ключом для SSH-соединения
  "max_routines": 5, // Максимальное количество одновременно работающих горутин
  "compress_dump": true, // необходимо ли сжимать данные true\false
  "debug": false, // вывод информации о запускаемых командах true\false
  "local_db": {
    "name": "local_database_name", // Имя локальной базы данных
    "user": "local_database_user", // Имя пользователя для подключения к локальной базе данных
    "password": "local_database_password", // Пароль для подключения к локальной базе данных
    "address": "local_database_address", // Адрес сервера локальной базы данных
    "port": "local_database_port" // Порт сервера локальной базы данных
  },
  "remote_db": {
    "name": "remote_database_name", // Имя удаленной базы данных
    "user": "remote_database_user", // Имя пользователя для подключения к удаленной базе данных
    "password": "remote_database_password", // Пароль для подключения к удаленной базе данных
    "address": "remote_database_address", // Адрес сервера удаленной базы данных
    "port": "remote_database_port" // Порт сервера удаленной базы данных
  }
}
```