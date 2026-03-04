# MMDVM Last Heard Bot

This project is a Python-based Telegram bot that monitors MMDVM logs and sends updates to a specified Telegram chat. It uses the `python-telegram-bot` library to interact with Telegram and parses MMDVM log files to extract relevant information.

## Features

- Monitors MMDVM log files for new entries.
- Parses log entries and formats them into readable messages.
- Sends updates to a Telegram chat using a bot.
- Configurable via environment variables.

## Prerequisites

- Python 3.13 or higher
- A Telegram bot token (create one using [BotFather](https://core.telegram.org/bots#botfather)).
- A Telegram chat ID where the bot will send messages.
- Access to the DStar log files on your Pi-Star system.

## 🛠️ Installation

```bash
git clone https://github.com/HafiziRuslan/MMDVM-LastHeard.git mmdvmlhbot
cd mmdvmlhbot
```

Mirror Repositories (delayed daily update):

- GitLab: <https://gitlab.com/hafiziruslan/MMDVM-LastHeard>
- Codeberg: <https://codeberg.org/hafiziruslan/MMDVM-LastHeard>
- Gitea: <https://gitea.com/HafiziRuslan/MMDVM-LastHeard>

## ⚙️ Configuration

Copy the file `default.env` into `.env`, and edit the configuration using your favorite editor.

```bash
cp default.env .env
nano .env
```

## AutoStart

Copy & Paste this line into last line (before blank line) of `/etc/crontab` or any other cron program that you're using.

```bash
@reboot pi-star cd /home/pi-star/mmdvmlhbot && ./main.sh > /var/log/lastheard.log 2>&1
```

change the `pi-star` username into your username

## Update

Manual update are **NOT REQUIRED** as it has integrated into `main.sh`.

Use this command for manual update:-

```bash
git pull --autostash
```

## 🚀 Usage

Run the main script with root privileges. This script automatically:

- Checks for and installs system dependencies (`gcc`, `git`, `python3-dev`, `curl`).
- Installs `uv` and sets up the Python virtual environment.
- Updates the repository to the latest version.
- Runs the application in a monitoring loop.

```bash
sudo ./main.sh
```

Note: to install uv using `apt`, you may use `debian.griffo.io` repository.

```bash
curl -sS https://debian.griffo.io/EA0F721D231FDD3A0A17B9AC7808B4DD62C41256.asc | sudo gpg --dearmor --yes -o /etc/apt/trusted.gpg.d/debian.griffo.io.gpg

echo "deb https://debian.griffo.io/apt $(lsb_release -sc 2>/dev/null) main" | sudo tee /etc/apt/sources.list.d/debian.griffo.io.list

sudo apt update && sudo apt install uv
```

## Logging

The bot uses Python's `logging` module to log events and errors. Logs are displayed in the console for easy debugging.

### Source

[iu2frl/pistar-lastheard-telegram](https://github.com/iu2frl/pistar-lastheard-telegram)
