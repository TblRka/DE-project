# shop-source

Отдельный стенд-источник: Postgres `source` (схема `shop`) и генератор изменений.
С лейкхаусом не связан, внешние проекты только подключаются к базе.

```bash
docker compose up -d            # поднять (схема, роли, публикация, seed создаются сами)
docker compose logs -f source-generator
docker compose down -v          # снести вместе с данными
```

## Подключение

| Откуда | Хост:порт |
|---|---|
| с машины (DBeaver, psql, скрипты) | `localhost:25432` |
| из другого docker compose | `source-db:5432` в сети `shop-source-net` |

База `source`. Роли:

| Роль | Пароль | Для чего |
|---|---|---|
| `cdc_user` | `cdcpass123` | LOGIN, REPLICATION, SELECT на `shop.*`: коннекторы CDC и батч-выгрузка |
| `shop_app` | `apppass123` | запись, ей пользуется только генератор |
| `admin` | `sourcepass123` | суперпользователь |

CDC: `wal_level=logical`, публикация `shop_pub` (customers, delivery_points, orders),
`REPLICA IDENTITY FULL`. Слот репликации не создан, его создает коннектор (plugin `pgoutput`).

Подключение сервиса из другого compose к сети источника:

```yaml
services:
  some-service:
    networks: [default, shop-source-net]

networks:
  shop-source-net:
    external: true
```

Сначала должен быть поднят `shop-source`, иначе сети еще нет.

## Генератор

Параметры заданы переменными окружения в `docker-compose.yml`, значения по умолчанию описаны
в начале `generator/generator.py`. Аномалия с `updated_at` задним числом (около 2% операций)
заложена намеренно, см. функцию `change_lag_sec`. Темп новых заказов: `NEW_ORDERS_PER_MIN`,
по умолчанию 20 в минуту.

## Ограничение размера

Том базы не должен превышать `SIZE_LIMIT_MB` (по умолчанию 3072, то есть 3 ГБ). Раз в
`SIZE_CHECK_EVERY_CYCLES` циклов генератор меряет размер: `pg_database_size` всех баз плюс файлы WAL
(`pg_ls_waldir`). Результат пишется в лог строкой `size:`.

| Режим | Когда | Что делает |
|---|---|---|
| `normal` | ниже `SIZE_THRESHOLD_PCT` (80%) | работает как обычно |
| `hold` | от 80% | новые заказы не создаются, вместо них удаляются самые старые доставленные и отмененные, примерно в том же темпе |
| `aggressive` | от `SIZE_AGGRESSIVE_PCT` (95%) | удаляет в `HOLD_AGGRESSIVE_FACTOR` (10) раз больше, пока размер не опустится ниже 80% |

Переходы статусов, правки справочников, чистка по сроку и удаление клиентов идут во всех режимах.
Каждое переключение пишется в лог отдельной строкой `MODE ... -> ...`. Удержание не удаляет
заказы, если их остается меньше `HOLD_MIN_ORDERS`.

`DELETE` не возвращает место ОС, поэтому в удержании размер перестает расти, но сам почти
не уменьшается. `VACUUM FULL` генератор не вызывает. Вниз размер уходит в основном за счет WAL,
но медленно: Postgres держит заранее созданные файлы WAL и удаляет их, только когда через них
пройдет запись. При тихой работе в удержании на это уходят часы.

Размер WAL на диске при обычной работе ограничен `max_wal_size=256MB`, поэтому лимит
меньше ~300 МБ имеет смысл только для проверки.

### Проверка на низком лимите

Большой seed раздувает WAL, и генератор сразу уходит в удержание:

```bash
docker compose down -v
SIZE_LIMIT_MB=200 SEED_ORDERS=200000 docker compose up -d
docker compose logs -f source-generator | grep -E "MODE|size:"
```

Ожидаемо: `MODE normal -> hold`, размер около 177 МБ (данные ~65, WAL ~112) и дальше не растет,
в строках `cycle N [hold]` новых заказов 0, статусы и удаления идут.

Чтобы не ждать, пока WAL уйдет сам, можно ускорить руками (переключение сегмента и чекпоинт,
на CDC не влияет). Через минуту в логе `MODE hold -> normal`, новые заказы снова создаются:

```bash
for i in $(seq 12); do docker compose exec -T source-db psql -U admin -d source -qc "SELECT pg_switch_wal()" -c "CHECKPOINT"; done
```

Если на уже работающем томе в логе `no access to pg_ls_waldir()` (том создан до появления
лимита), выдайте право один раз или пересоздайте том через `down -v`:

```bash
docker compose exec source-db psql -U admin -d source -c "GRANT EXECUTE ON FUNCTION pg_ls_waldir() TO shop_app"
```

## Про WAL и слоты репликации

**Незанятый слот логической репликации не дает Postgres чистить WAL.** Если коннектор
создал слот, а потом остановлен или удален, WAL копится под этот слот без ограничений.
Ни `max_wal_size`, ни лимиты генератора это не сдерживают: генератор увидит рост размера
и уйдет в удержание, но место будет занимать WAL, а не данные.

Посмотреть слоты и сколько WAL держит каждый:

```sql
SELECT slot_name, plugin, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;
```

Удалить слот, который больше не нужен (активный слот удалить нельзя, сначала остановите коннектор):

```sql
SELECT pg_drop_replication_slot('имя_слота');
```

`max_slot_wal_keep_size` специально не задан: при превышении Postgres инвалидирует слот,
и CDC придется перезапускать с новым снапшотом.
