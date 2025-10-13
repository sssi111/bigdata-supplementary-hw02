# bigdata-supplementary-hw02

YARN кластер для обработки данных с веб-интерфейсами основных и вспомогательных демонов.

## Архитектура кластера

### Основные демоны YARN:
- **ResourceManager** (192.168.1.15:8088) - управляющий демон кластера
- **NodeManager** (на всех нодах) - демоны для управления контейнерами на узлах

### Вспомогательные демоны:
- **JobHistoryServer** (192.168.1.15:19888) - сервер истории выполнения задач MapReduce

### Веб-интерфейсы:
Все веб-интерфейсы доступны только через SSH туннель (см. раздел "Доступ к веб-интерфейсам" ниже):
- ResourceManager UI (порт 8088)
- NodeManager UI (порт 8042 на каждой ноде)
- JobHistoryServer UI (порт 19888)

## Step-by-step guide

### Требования

Перед развертыванием YARN убедитесь, что:
1. HDFS кластер уже развернут и работает (см. [bigdata-supplementary-hw01](https://github.com/blonded04/bigdata-supplementary-hw01))
2. У вас есть доступ к джамп-ноде с правами sudo

### Подключение к джамп-ноде

Подключитесь к jump-ноде:
```bash
ssh team@176.109.91.5
```

Оттуда у вас есть доступ к внутренней сети кластера (192.168.1.x)

### Предварительная настройка для первого запуска

На джамп-ноде установите `ansible-core` и `sshpass`
```bash
sudo apt install ansible-core
sudo apt install sshpass
```
Клонируйте репозиторий:
```bash
git clone git@github.com:sssi111/bigdata-supplementary-hw02.git
```

### Развертывание YARN кластера

Перейдите в папку с плейбуком:
```bash
cd bigdata-supplementary-hw02/cluster
```

Запустите деплой Ansible плейбука:
```bash
ansible-playbook -i inventory.ini deploy-yarn.yml --ask-become-pass
```

После введите пароль от пользователя `team` на узлах кластера.

Процесс развертывания включает:
1. Установку/проверку Hadoop (если еще не установлен)
2. Настройку конфигурации YARN на всех узлах (веб-интерфейсы доступны только через SSH туннель)
3. Запуск ResourceManager на главном узле
4. Запуск NodeManager на всех узлах
5. Запуск JobHistoryServer для отслеживания истории задач

### Копирование вспомогательных скриптов (опционально)

Если вы обновляете существующий кластер и хотите скопировать только скрипты без перезапуска служб:

```bash
ansible-playbook -i inventory.ini deploy-scripts.yml --ask-become-pass
```

### Проверка кластера

После завершения развертывания проверьте статус YARN кластера:

```bash
ssh team@192.168.1.15
bash /opt/hadoop/scripts/check-yarn.sh
```

### Доступ к веб-интерфейсам

**Важно:** Веб-интерфейсы настроены для прослушивания только на localhost (127.0.0.1) из соображений безопасности. Доступ возможен только через SSH туннель с вашего pc.

Веб-интерфейсы кластера:

1. **ResourceManager Web UI** (порт 8088):
   - Показывает состояние кластера, запущенные приложения, использование ресурсов

2. **NodeManager Web UI** (порт 8042 на каждой ноде):
   - Показывает контейнеры, запущенные на конкретном узле

3. **JobHistoryServer Web UI** (порт 19888):
   - Показывает историю выполнения MapReduce задач

#### Настройка SSH туннеля

Создайте SSH туннель для доступа к веб-интерфейсам с вашего pc:

**Простой вариант (основные интерфейсы):**
```bash
ssh -J team@176.109.91.5 \
    -L 8088:127.0.0.1:8088 \
    -L 8042:127.0.0.1:8042 \
    -L 19888:127.0.0.1:19888 \
    team@192.168.1.15
```

После подключения откройте в браузере на ноутбуке:
- ResourceManager: http://localhost:8088
- NodeManager: http://localhost:8042
- JobHistoryServer: http://localhost:19888


### Запуск тестового задания

Чтобы проверить работу YARN, выполните тестовое MapReduce задание:

```bash
ssh team@192.168.1.15
sudo -u hadoop JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 /opt/hadoop/bin/hadoop jar \
  /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.0.jar pi 10 100
```

Вы можете наблюдать за выполнением задания через ResourceManager Web UI.

### Остановка кластера

Для остановки YARN кластера:

```bash
ssh team@192.168.1.15
sudo -u hadoop JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 /opt/hadoop/sbin/stop-yarn.sh
sudo -u hadoop JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 /opt/hadoop/bin/mapred --daemon stop historyserver
```

### Перезапуск кластера

Для перезапуска просто выполните:
```bash
ansible-playbook -i inventory.ini deploy-yarn.yml --ask-become-pass
```

## Конфигурация

### Основные файлы конфигурации:

- `cluster/inventory.ini` - инвентарь узлов кластера
- `cluster/vars.yml` - переменные для настройки кластера
- `cluster/config/yarn-site.xml.j2` - конфигурация YARN
- `cluster/config/mapred-site.xml.j2` - конфигурация MapReduce
- `cluster/config/core-site.xml.j2` - базовая конфигурация Hadoop

### Ansible Playbooks:

- `deploy-yarn.yml` - основной playbook для полного развертывания YARN кластера
- `deploy-scripts.yml` - копирование вспомогательных скриптов без перезапуска служб
- `update-mapreduce-config.yml` - обновление конфигурации MapReduce без перезапуска кластера
- `fix-hostname.yml` - быстрое исправление проблем с hostname resolution

### Вспомогательные скрипты:

- `scripts/check-yarn.sh` - проверка статуса YARN кластера
- `scripts/check-mapreduce-logs.sh` - просмотр логов NodeManager/ResourceManager
- `scripts/check-failed-job.sh` - детальная диагностика упавших MapReduce задач

## Параметры ресурсов

### YARN параметры

Настроены следующие параметры ресурсов:
- Память на NodeManager: 4096 МБ
- Виртуальных ядер: 2
- Минимальное выделение памяти: 512 МБ
- Максимальное выделение памяти: 4096 МБ

Эти параметры можно изменить в файле `cluster/config/yarn-site.xml.j2`.

### MapReduce параметры

Параметры памяти для MapReduce задач:
- ApplicationMaster память: 1024 МБ
- Map task память: 1024 МБ
- Reduce task память: 1024 МБ

Эти параметры можно изменить в файле `cluster/config/mapred-site.xml.j2`.

## Устранение неполадок

### MapReduce задачи падают (FAILED status)

**Проверка логов NodeManager/ResourceManager:**

```bash
ssh team@192.168.1.15
bash /opt/hadoop/scripts/check-mapreduce-logs.sh
```

Этот скрипт показывает системные логи YARN служб.

**Проверка логов конкретной упавшей задачи:**

Скопируйте `application_id` из вывода упавшей задачи, затем:

```bash
ssh team@192.168.1.15
bash /opt/hadoop/scripts/check-failed-job.sh application_1760186917153_0006
```

Этот скрипт покажет:
- Статус приложения
- Агрегированные логи всех контейнеров из HDFS
- Точную причину падения задачи

**Поиск недавних упавших задач:**

```bash
ssh team@192.168.1.15
sudo -u hadoop JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 /opt/hadoop/bin/yarn application -list -appStates FAILED
```

## Состав команды

ВШЭ СПб, ПМИ, 4 курс
* Нейков Даниил
* Панов Андрей
* Мацкевич Валерий
