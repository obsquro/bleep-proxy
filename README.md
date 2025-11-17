# Bleep Proxy

Простой TCP прокси для проброса портов с локальной машины на публичный сервер. Пробрасывай свой вебсервер, Minecraft или что угодно через VPS в интернет.

## Как это работает

- **Сервер** запускается на VPS с белым IP
- **Клиент** запускается локально и подключается к серверу
- Все подключения к публичному порту на VPS перенаправляются к тебе

Одно TCP соединение, мультиплексирование каналов, никаких зависимостей.

## Сборка

Нужен Go 1.21+

```bash
go build -tags server -o server protocol.go server.go
go build -tags client -o client protocol.go client.go
```

Или используй `build.bat` для сборки под все платформы сразу.

## Настройка

### Сервер (VPS)

Создай `server.json`:

```json
{
  "client_listen_port": "7000",
  "public_listen_ports": {
    "web": {
      "port": 80,
      "key": "твой_секретный_ключ"
    },
    "minecraft": {
      "port": 25565,
      "key": "другой_секретный_ключ"
    }
  }
}
```

- `client_listen_port` - порт для подключения клиентов
- `public_listen_ports` - какие порты слушать публично
  - `web`, `minecraft` - ID клиента (любое имя)
  - `port` - публичный порт
  - `key` - пароль для аутентификации

### Клиент (локальная машина)

Создай `client.json`:

```json
{
  "client_id": "web",
  "auth_key": "твой_секретный_ключ",
  "server_address": "123.45.67.89",
  "server_port": "7000",
  "service_address": "localhost:3000"
}
```

- `client_id` - должен совпадать с ID в `server.json`
- `auth_key` - должен совпадать с ключом в `server.json`
- `server_address` - IP твоего VPS
- `server_port` - порт сервера (`client_listen_port`)
- `service_address` - локальный адрес сервиса

## Примеры использования

### Веб-сервер

Допустим, у тебя запущен веб-сервер на `localhost:8080`. Хочешь чтобы он был доступен через VPS на порту 80.

**server.json** на VPS:
```json
{
  "client_listen_port": "7000",
  "public_listen_ports": {
    "mywebapp": {
      "port": 80,
      "key": "randompassword123"
    }
  }
}
```

**client.json** локально:
```json
{
  "client_id": "mywebapp",
  "auth_key": "randompassword123",
  "server_address": "123.45.67.89",
  "server_port": "7000",
  "service_address": "localhost:8080"
}
```

Запускай:
```bash
./server -config server.json
```

```bash
./client -config client.json
```

Готово. Теперь заходи на `http://123.45.67.89` - увидишь свой локальный сервер.

### Minecraft сервер

У тебя Minecraft сервер на `localhost:25565`, хочешь чтобы друзья могли зайти через твой VPS.

**server.json** на VPS:
```json
{
  "client_listen_port": "7000",
  "public_listen_ports": {
    "minecraft": {
      "port": 25565,
      "key": "minecraftkey999"
    }
  }
}
```

**client.json** локально:
```json
{
  "client_id": "minecraft",
  "auth_key": "minecraftkey999",
  "server_address": "123.45.67.89",
  "server_port": "7000",
  "service_address": "localhost:25565"
}
```

Запускай так же. Друзья подключаются к `123.45.67.89:25565`.

### Несколько сервисов одновременно

Можешь пробросить несколько портов с одной или разных машин:

**server.json**:
```json
{
  "client_listen_port": "7000",
  "public_listen_ports": {
    "web": {
      "port": 80,
      "key": "webkey"
    },
    "api": {
      "port": 8080,
      "key": "apikey"
    },
    "minecraft": {
      "port": 25565,
      "key": "mckey"
    }
  }
}
```

Создай несколько `client.json` с разными `client_id` и запускай несколько клиентов.

## Установка на VPS

### Вариант 1: Screen

Быстрый способ запустить в фоне.

```bash
screen -S bleep-server
./server -config server.json
```

Нажми `Ctrl+A` потом `D` чтобы отключиться от screen.

Подключиться обратно:
```bash
screen -r bleep-server
```

Посмотреть все screen сессии:
```bash
screen -ls
```

### Вариант 2: systemd

Создай файл `/etc/systemd/system/bleep-proxy.service`:

```ini
[Unit]
Description=Bleep Proxy Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/bleep-proxy
ExecStart=/opt/bleep-proxy/server -config /opt/bleep-proxy/server.json
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Положи бинарник и конфиг в `/opt/bleep-proxy/`:
```bash
mkdir -p /opt/bleep-proxy
cp server /opt/bleep-proxy/
cp server.json /opt/bleep-proxy/
chmod +x /opt/bleep-proxy/server
```

Запускай сервис:
```bash
systemctl daemon-reload
systemctl enable bleep-proxy
systemctl start bleep-proxy
```

Проверить статус:
```bash
systemctl status bleep-proxy
```

Логи:
```bash
journalctl -u bleep-proxy -f
```

Перезапустить:
```bash
systemctl restart bleep-proxy
```

Остановить:
```bash
systemctl stop bleep-proxy
```

## Установка клиента

### Linux/macOS через screen

```bash
screen -S bleep-client
./client -config client.json
```

`Ctrl+A`, `D` чтобы отключиться.

### Linux/macOS через systemd

Файл `/etc/systemd/system/bleep-client.service`:

```ini
[Unit]
Description=Bleep Proxy Client
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/home/youruser/bleep-proxy
ExecStart=/home/youruser/bleep-proxy/client -config /home/youruser/bleep-proxy/client.json
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable bleep-client
systemctl start bleep-client
```

### Windows

Просто запусти `client.exe`. Или создай bat файл:

```batch
@echo off
cd /d %~dp0
client.exe -config client.json
pause
```

Или добавь в автозагрузку через планировщик задач.

## Безопасность

- Используй сложные ключи в `auth_key`
- Настрой firewall на VPS (разреши только нужные порты)
- Клиент автоматически переподключается при обрыве
- Используй разные ключи для разных сервисов

## Требования

- Go 1.21+ для сборки
- VPS с белым IP
- Открытые порты на VPS (указанные в конфиге)

## Лицензия

MIT
