# Проект "Электронный ловец фрода в реальном режиме"



## Описание проекта

Задачей проекта "Электронный ловец фрода" является создание базы данных для системы, способной анализировать каждую транзакцию в реальном времени для выявления потенциального мошенничества в виде аномальных данных.

Требования:

- данные должны поступать из источников в потоковом режиме;
- необходимо организовать хранение данных, для дальнейшего изучения их аналитиками;
- для мгновенного детектирования фрода важна скорость обработки данных;
- необходимо также отслеживать работоспособность системы в целом.

## Начало работы

Клонирование репозитория:

```
git clone https://git.astondevs.ru/laboratory/hadoop/lab-projects/wave18_team_b.git
```

## Используемые технологии

- Hadoop (для размещения и хранения данных)
- Spark/Spark streaming (для обработки данных)
- Kafka (для потоковой доставки данных)
- GreenPlum (для организации хранилища данных)
- ClickHouse (для организации витрин данных)
- Airflow (для оркестрации доставки данных)

## Логика архитектуры проекта

1) Генерируем файл транзакций для последующей потоковой обработки на предмет фрода. 
2) Kafka используется для получения данных в режиме потока и последующей передачи в SparkStreaming.
3) В SparkStreaming прописана логика поиска фрода в потоке транзакций.
4) Prometheus и Grafana могут быть использованы для общего мониторинга передачи данных в рамках нашего пайплайн и поиска узких мест при повышениях нагрузки. В целом, данный шаг должен предполагать передачу данных в реальном времени другой команде, ответственной за действия с найденным фродом (блокировка счёта клиента, уведомление и тд)
5) Транзакции, после проверки + разметки на фрод, заливаются в hdfs. Данные собираются в Spark пакетами через spark.writeStreaming. Тут организовано долгосрочное хранение с возможностью доступа других команд.
6) В GreenPlum создаются необходимые слои для формирования финальных витрин, данные обогащаются описательными данными, словарями (которые попадают сюда из бд компании и периодически обновляются).
7) В некоторых случаях запросы, требующие больших вычислений и агрегаций, могут выполняться быстрее в ClickHouse из-за оптимизированной колоночной структуры хранения.


## Схема архитектуры проекта

 <Тут будет схема>

### GreenPlum:
ODS-слой: Сюда будут приходить сырые данные из hdfs (разнообразные базы данных о клиентах, их счетах и т.д.) и информация о транзакциях в режиме реального времени. 
DDS-слой: Cлой, содержащий детальные данные по основным сущностям (клиент, карточка). Цель слоя: накапливать данные о сущностях модели, консолидировать данные между источниками.
DM-слой: Витрины с рассчитанными агрегатами и прочей статистикой.

## Title
Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing (SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

***

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or feel free to structure it however you want - this is just a starting point!). Thanks to [makeareadme.com](https://www.makeareadme.com/) for this template.


