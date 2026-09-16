"""Генератор данных для источника shop (Postgres).

Режимы задаются переменной GENERATOR_MODE, можно несколько через запятую:
  seed - начальное наполнение (один раз, на непустой базе ничего не делает)
  run  - бесконечный цикл: новые заказы, движение по статусам, отмены,
         редкие правки справочников и редкие физические удаления
По умолчанию GENERATOR_MODE=seed,run.

Модель времени заказа. Для каждого заказа моменты смены статусов считаются
детерминированно от его created_at (колонка после вставки не меняется):
  created -> assembling -> in_delivery -> delivered
Задержки у разных заказов разные, масштаб задает ORDER_STEP_SEC. На отмену
в каждом переходе есть шанс CANCEL_RATE. Таймлайн не опирается на updated_at,
чтобы аномалия с updated_at (см. ниже) не влияла на поведение заказов.

Ограничение размера. Раз в SIZE_CHECK_EVERY_CYCLES циклов генератор меряет
размер тома базы (все базы + WAL) и выбирает режим, см. класс SizeGuard.
"""

import logging
import math
import os
import random
import signal
import sys
import time
from datetime import timedelta

import psycopg

log = logging.getLogger("generator")


# ---------------------------------------------------------------------------
# Параметры
# ---------------------------------------------------------------------------

def env_str(name, default):
    return os.environ.get(name, default)


def env_int(name, default):
    return int(os.environ.get(name, default))


def env_float(name, default):
    return float(os.environ.get(name, default))


MODES = [m.strip() for m in env_str("GENERATOR_MODE", "seed,run").split(",") if m.strip()]

DB_DSN = {
    "host": env_str("DB_HOST", "source-db"),
    "port": env_int("DB_PORT", 5432),
    "dbname": env_str("DB_NAME", "source"),
    "user": env_str("DB_USER", "shop_app"),
    "password": env_str("DB_PASSWORD", "apppass123"),
    "application_name": "source-generator",
}

# seed
SEED_CUSTOMERS = env_int("SEED_CUSTOMERS", 500)
SEED_POINTS = env_int("SEED_POINTS", 40)
SEED_ORDERS = env_int("SEED_ORDERS", 5000)
SEED_DAYS = env_int("SEED_DAYS", 30)  # глубина истории заказов

# run
CYCLE_PAUSE_SEC = env_float("CYCLE_PAUSE_SEC", 5)
NEW_ORDERS_PER_MIN = env_float("NEW_ORDERS_PER_MIN", 20)  # средний темп новых заказов
ORDER_STEP_SEC = env_float("ORDER_STEP_SEC", 60)  # характерное время одного шага статуса
NEW_CUSTOMER_PROB = env_float("NEW_CUSTOMER_PROB", 0.2)  # вероятность нового клиента за цикл
CANCEL_RATE = env_float("CANCEL_RATE", 0.05)  # доля переходов статуса, которые становятся отменой
CUSTOMER_UPDATE_PROB = env_float("CUSTOMER_UPDATE_PROB", 0.1)  # вероятность правки клиента за цикл
POINT_UPDATE_PROB = env_float("POINT_UPDATE_PROB", 0.05)  # вероятность правки точки за цикл
PURGE_PROB = env_float("PURGE_PROB", 0.02)  # вероятность чистки старых заказов за цикл
ORDER_RETENTION_DAYS = env_float("ORDER_RETENTION_DAYS", 25)  # старше этого заказы удаляются физически
PURGE_BATCH = env_int("PURGE_BATCH", 500)  # не больше стольких заказов за одну чистку
CUSTOMER_DELETE_PROB = env_float("CUSTOMER_DELETE_PROB", 0.01)  # удаление клиента (запрос на удаление ПДн)

# аномалия updated_at
ANOMALY_RATE = env_float("ANOMALY_RATE", 0.02)
ANOMALY_MIN_SEC = env_float("ANOMALY_MIN_SEC", 120)  # 2 минуты
ANOMALY_MAX_SEC = env_float("ANOMALY_MAX_SEC", 3 * 3600)  # 3 часа

# ограничение размера тома базы: данные всех баз + WAL
SIZE_LIMIT_MB = env_float("SIZE_LIMIT_MB", 3072)  # жесткий лимит, 3 ГБ
SIZE_THRESHOLD_PCT = env_float("SIZE_THRESHOLD_PCT", 80)  # порог режима удержания, % от лимита
SIZE_AGGRESSIVE_PCT = env_float("SIZE_AGGRESSIVE_PCT", 95)  # с этого % лимита удаляем агрессивно
SIZE_CHECK_EVERY_CYCLES = env_int("SIZE_CHECK_EVERY_CYCLES", 12)  # мерить размер раз в N циклов
HOLD_AGGRESSIVE_FACTOR = env_int("HOLD_AGGRESSIVE_FACTOR", 10)  # во сколько раз больше удалять у лимита
HOLD_MIN_ORDERS = env_int("HOLD_MIN_ORDERS", 1000)  # удержание не удаляет заказы ниже этого числа
ORDERS_RECOUNT_EVERY_CYCLES = env_int("ORDERS_RECOUNT_EVERY_CYCLES", 720)  # полный count(*) раз в N циклов

LOG_LEVEL = env_str("LOG_LEVEL", "INFO")


# ---------------------------------------------------------------------------
# Справочные значения для генерации
# ---------------------------------------------------------------------------

CITIES = {  # город -> (lat, lon) центра
    "Москва": (55.7558, 37.6173),
    "Санкт-Петербург": (59.9343, 30.3351),
    "Новосибирск": (55.0084, 82.9357),
    "Екатеринбург": (56.8389, 60.6057),
    "Казань": (55.7963, 49.1088),
    "Нижний Новгород": (56.2965, 43.9361),
    "Самара": (53.1959, 50.1002),
    "Краснодар": (45.0355, 38.9753),
    "Ростов-на-Дону": (47.2357, 39.7015),
    "Пермь": (58.0105, 56.2502),
}
FIRST_NAMES = ["Александр", "Мария", "Иван", "Анна", "Дмитрий", "Елена", "Сергей", "Ольга",
               "Андрей", "Наталья", "Михаил", "Татьяна", "Алексей", "Екатерина", "Павел", "Ирина"]
LAST_NAMES = ["Иванов", "Петров", "Смирнов", "Кузнецов", "Попов", "Васильев", "Соколов",
              "Михайлов", "Новиков", "Федоров", "Морозов", "Волков", "Алексеев", "Лебедев"]
STREETS = ["Ленина", "Мира", "Советская", "Гагарина", "Садовая", "Лесная", "Школьная",
           "Молодежная", "Центральная", "Набережная", "Победы", "Пушкина"]
SEGMENTS = (["retail"] * 80) + (["b2b"] * 15) + (["vip"] * 5)
POINT_TYPES = ["pickup", "locker", "courier_zone"]

FEMALE_FIRST = {"Мария", "Анна", "Елена", "Ольга", "Наталья", "Татьяна", "Екатерина", "Ирина"}

# множители ORDER_STEP_SEC для каждого перехода: created->assembling, ->in_delivery, ->delivered
STEP_MULTIPLIERS = (1.0, 2.0, 4.0)
NEXT_STATUS = {"created": "assembling", "assembling": "in_delivery", "in_delivery": "delivered"}


def random_full_name():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    if first in FEMALE_FIRST:
        last += "а"
    return f"{last} {first}"


def random_address():
    return f"ул. {random.choice(STREETS)}, д. {random.randint(1, 150)}"


def random_coords(city):
    lat, lon = CITIES[city]
    return round(lat + random.uniform(-0.15, 0.15), 6), round(lon + random.uniform(-0.25, 0.25), 6)


def random_order_amounts():
    items = random.randint(1, 10)
    total = round(sum(random.uniform(150, 5000) for _ in range(items)), 2)
    return items, total


def random_ts_between(start, end):
    return start + (end - start) * random.random()


def poisson(lam):
    """Случайное число событий при среднем lam (для темпа заказов)."""
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, round(random.gauss(lam, math.sqrt(lam))))
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= random.random()
        if p <= limit:
            return k
        k += 1


# ---------------------------------------------------------------------------
# Таймлайн заказа
# ---------------------------------------------------------------------------

def order_timeline(created_at):
    """Моменты перехода в assembling, in_delivery, delivered и плановая доставка.

    Задержки псевдослучайные, но зависят только от created_at, поэтому
    одинаково считаются и при seed, и в каждом цикле run.
    """
    rnd = random.Random(int(created_at.timestamp() * 1_000_000))
    moments = []
    t = created_at
    for mult in STEP_MULTIPLIERS:
        t = t + timedelta(seconds=ORDER_STEP_SEC * mult * rnd.uniform(0.4, 1.8))
        moments.append(t)
    expected_total = ORDER_STEP_SEC * sum(STEP_MULTIPLIERS)
    planned = created_at + timedelta(seconds=expected_total * 1.1)
    return moments[0], moments[1], moments[2], planned


class Stats:
    """Счетчики одного цикла run для лога."""

    def __init__(self):
        self.ins_orders = self.ins_customers = 0
        self.upd_status = self.upd_cancelled = self.upd_delivered = 0
        self.upd_customers = self.upd_points = 0
        self.del_orders = self.del_customers = 0
        self.del_hold = 0  # из del_orders: удалено режимом удержания
        self.anomalies = 0


# ---------------------------------------------------------------------------
# Аномалия updated_at
# ---------------------------------------------------------------------------

def change_lag_sec(stats):
    """На сколько секунд назад сдвинуть updated_at для очередной операции.

    !!! АНОМАЛИЯ updated_at (намеренная, по ТЗ, НЕ ЧИНИТЬ) !!!
    У доли ANOMALY_RATE операций updated_at проставляется задним числом:
    на ANOMALY_MIN_SEC..ANOMALY_MAX_SEC раньше реального момента изменения.
    Такие строки пропускает инкрементальная выгрузка по верхней границе
    updated_at: к моменту коммита граница уже ушла вперед.
    Все записи, меняющие updated_at, берут сдвиг только отсюда.
    """
    if random.random() < ANOMALY_RATE:
        stats.anomalies += 1
        return random.uniform(ANOMALY_MIN_SEC, ANOMALY_MAX_SEC)
    return 0.0


# SQL-выражение для времени изменения: реальный момент минус сдвиг аномалии
CHANGE_TS = "clock_timestamp() - make_interval(secs => %s::float8)"


def db_now(conn):
    return conn.execute("SELECT clock_timestamp()").fetchone()[0]


# ---------------------------------------------------------------------------
# Подключение
# ---------------------------------------------------------------------------

def connect():
    delay = 1
    while True:
        try:
            # транзакции открываются явно через conn.transaction()
            conn = psycopg.connect(**DB_DSN, connect_timeout=5, autocommit=True)
            log.info("connected to %s:%s/%s as %s", DB_DSN["host"], DB_DSN["port"],
                     DB_DSN["dbname"], DB_DSN["user"])
            return conn
        except psycopg.OperationalError as e:
            log.warning("db not available (%s), retry in %ss", str(e).strip().splitlines()[0], delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)


# ---------------------------------------------------------------------------
# seed
# ---------------------------------------------------------------------------

SEED_LOCK_ID = 7_401_001  # advisory lock, чтобы два seed не пошли параллельно
SEED_CHUNK = 10_000  # заказов в одной пачке вставки


def seed(conn):
    started = time.monotonic()
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (SEED_LOCK_ID,))
        counts = conn.execute(
            "SELECT (SELECT count(*) FROM shop.customers),"
            "       (SELECT count(*) FROM shop.delivery_points),"
            "       (SELECT count(*) FROM shop.orders)"
        ).fetchone()
        if any(counts):
            log.info("seed: skipped, database is not empty (customers=%s points=%s orders=%s)", *counts)
            return

        now = db_now(conn)
        history_start = now - timedelta(days=SEED_DAYS)
        # справочники старше заказов, чтобы created_at заказа не был раньше клиента
        dict_end = history_start - timedelta(days=1)

        with conn.cursor() as cur:
            points = []
            for i in range(SEED_POINTS):
                city = random.choice(list(CITIES))
                lat, lon = random_coords(city)
                ptype = random.choice(POINT_TYPES)
                created = random_ts_between(dict_end - timedelta(days=730), dict_end)
                updated = random_ts_between(created, now) if random.random() < 0.3 else created
                points.append((f"{city}, пункт №{i + 1}", city, random_address(), lat, lon, ptype,
                               created, updated))
            cur.executemany(
                "INSERT INTO shop.delivery_points"
                " (name, city, address, lat, lon, point_type, created_at, updated_at)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                points,
            )

            customers = []
            for _ in range(SEED_CUSTOMERS):
                created = random_ts_between(dict_end - timedelta(days=365), dict_end)
                updated = random_ts_between(created, now) if random.random() < 0.2 else created
                customers.append((random_full_name(), random.choice(list(CITIES)),
                                  random.choice(SEGMENTS), created, updated))
            cur.executemany(
                "INSERT INTO shop.customers (full_name, city, segment, created_at, updated_at)"
                " VALUES (%s, %s, %s, %s, %s)",
                customers,
            )

            customer_ids = [r[0] for r in cur.execute("SELECT customer_id FROM shop.customers")]
            point_ids = [r[0] for r in cur.execute("SELECT point_id FROM shop.delivery_points")]

            # order_id растет вместе с created_at: моменты создания сортируем заранее,
            # строки вставляем пачками, чтобы большой SEED_ORDERS не держать в памяти
            span = (now - history_start).total_seconds()
            offsets = sorted(random.random() * span for _ in range(SEED_ORDERS))
            status_counts = {}
            for chunk_start in range(0, SEED_ORDERS, SEED_CHUNK):
                orders = []
                for offset in offsets[chunk_start:chunk_start + SEED_CHUNK]:
                    created = history_start + timedelta(seconds=offset)
                    row = seed_order_state(created, now)
                    items, total = random_order_amounts()
                    orders.append((random.choice(customer_ids), random.choice(point_ids), row["status"],
                                   total, items, row["is_cancelled"], created, row["updated_at"],
                                   row["planned"], row["actual"]))
                    status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
                cur.executemany(
                    "INSERT INTO shop.orders (customer_id, point_id, status, total_amount, items_count,"
                    " is_cancelled, created_at, updated_at, planned_delivery_at, actual_delivery_at)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    orders,
                )

    log.info("seed: inserted customers=%d points=%d orders=%d %s over %d days in %.1fs",
             len(customers), len(points), SEED_ORDERS, status_counts, SEED_DAYS,
             time.monotonic() - started)


def seed_order_state(created, now):
    """Состояние исторического заказа на момент now по его таймлайну."""
    t_asm, t_dlv, t_done, planned = order_timeline(created)
    status, updated, actual, cancelled = "created", created, None, False
    for status_next, moment in (("assembling", t_asm), ("in_delivery", t_dlv), ("delivered", t_done)):
        if moment > now:
            break
        if random.random() < CANCEL_RATE:
            status, updated, cancelled = "cancelled", moment, True
            break
        status, updated = status_next, moment
        if status_next == "delivered":
            actual = moment
    return {"status": status, "updated_at": updated, "planned": planned,
            "actual": actual, "is_cancelled": cancelled}


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

def step_new_customer(conn, stats):
    if random.random() >= NEW_CUSTOMER_PROB:
        return
    with conn.transaction():
        # на вставке аномалия сдвигает и created_at: запись "пришла" задним числом
        ts = db_now(conn) - timedelta(seconds=change_lag_sec(stats))
        conn.execute(
            "INSERT INTO shop.customers (full_name, city, segment, created_at, updated_at)"
            " VALUES (%s, %s, %s, %s, %s)",
            (random_full_name(), random.choice(list(CITIES)), random.choice(SEGMENTS), ts, ts),
        )
        stats.ins_customers += 1


def step_new_orders(conn, stats, n):
    if n <= 0:
        return
    with conn.transaction():
        customer_ids = [r[0] for r in conn.execute(
            "SELECT customer_id FROM shop.customers ORDER BY random() LIMIT %s", (n,))]
        point_ids = [r[0] for r in conn.execute("SELECT point_id FROM shop.delivery_points")]
        if not customer_ids or not point_ids:
            log.warning("no customers or delivery points, run seed first")
            return
        for _ in range(n):
            items, total = random_order_amounts()
            # на вставке аномалия сдвигает и created_at: заказ "пришел" задним числом
            ts = db_now(conn) - timedelta(seconds=change_lag_sec(stats))
            conn.execute(
                "INSERT INTO shop.orders (customer_id, point_id, status, total_amount, items_count,"
                " created_at, updated_at, planned_delivery_at)"
                " VALUES (%s, %s, 'created', %s, %s, %s, %s, %s)",
                (random.choice(customer_ids), random.choice(point_ids), total, items,
                 ts, ts, order_timeline(ts)[3]),
            )
            stats.ins_orders += 1


def step_advance_orders(conn, stats):
    with conn.transaction():
        now = db_now(conn)
        active = conn.execute(
            "SELECT order_id, status, created_at FROM shop.orders"
            " WHERE status IN ('created', 'assembling', 'in_delivery')"
            " ORDER BY order_id FOR UPDATE SKIP LOCKED"
        ).fetchall()
        for order_id, status, created_at in active:
            t_asm, t_dlv, t_done, _ = order_timeline(created_at)
            due = {"created": t_asm, "assembling": t_dlv, "in_delivery": t_done}[status]
            if due > now:
                continue
            lag = change_lag_sec(stats)
            if random.random() < CANCEL_RATE:
                # логическое удаление: строка остается, помечена отменой
                conn.execute(
                    f"UPDATE shop.orders SET status = 'cancelled', is_cancelled = true,"
                    f" updated_at = {CHANGE_TS} WHERE order_id = %s",
                    (lag, order_id),
                )
                stats.upd_cancelled += 1
            elif NEXT_STATUS[status] == "delivered":
                # actual_delivery_at - реальный момент доставки, аномалия его не касается
                conn.execute(
                    f"UPDATE shop.orders SET status = 'delivered', actual_delivery_at = clock_timestamp(),"
                    f" updated_at = {CHANGE_TS} WHERE order_id = %s",
                    (lag, order_id),
                )
                stats.upd_delivered += 1
            else:
                conn.execute(
                    f"UPDATE shop.orders SET status = %s, updated_at = {CHANGE_TS} WHERE order_id = %s",
                    (NEXT_STATUS[status], lag, order_id),
                )
            stats.upd_status += 1


def step_update_dictionaries(conn, stats):
    if random.random() < CUSTOMER_UPDATE_PROB:
        with conn.transaction():
            lag = change_lag_sec(stats)
            if random.random() < 0.5:
                sql = (f"UPDATE shop.customers SET city = %s, updated_at = {CHANGE_TS}"
                       f" WHERE customer_id = (SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1)"
                       f" AND city <> %s")
                city = random.choice(list(CITIES))
                params = (city, lag, city)
            else:
                sql = (f"UPDATE shop.customers SET segment = %s, updated_at = {CHANGE_TS}"
                       f" WHERE customer_id = (SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1)"
                       f" AND segment <> %s")
                segment = random.choice(["retail", "b2b", "vip"])
                params = (segment, lag, segment)
            stats.upd_customers += conn.execute(sql, params).rowcount

    if random.random() < POINT_UPDATE_PROB:
        with conn.transaction():
            lag = change_lag_sec(stats)
            if random.random() < 0.5:
                ptype = random.choice(POINT_TYPES)
                cur = conn.execute(
                    f"UPDATE shop.delivery_points SET point_type = %s, updated_at = {CHANGE_TS}"
                    f" WHERE point_id = (SELECT point_id FROM shop.delivery_points ORDER BY random() LIMIT 1)"
                    f" AND point_type <> %s",
                    (ptype, lag, ptype),
                )
            else:
                point = conn.execute(
                    "SELECT point_id, city FROM shop.delivery_points ORDER BY random() LIMIT 1").fetchone()
                if point is None:
                    return
                lat, lon = random_coords(point[1])
                cur = conn.execute(
                    f"UPDATE shop.delivery_points SET address = %s, lat = %s, lon = %s,"
                    f" updated_at = {CHANGE_TS} WHERE point_id = %s",
                    (random_address(), lat, lon, lag, point[0]),
                )
            stats.upd_points += cur.rowcount


def step_physical_deletes(conn, stats):
    if random.random() < PURGE_PROB:
        # чистка по сроку хранения: физический DELETE старых завершенных заказов
        with conn.transaction():
            cur = conn.execute(
                "DELETE FROM shop.orders WHERE order_id IN ("
                "  SELECT order_id FROM shop.orders"
                "  WHERE created_at < clock_timestamp() - make_interval(secs => %s::float8 * 86400)"
                "    AND status IN ('delivered', 'cancelled')"
                "  ORDER BY order_id LIMIT %s)",
                (ORDER_RETENTION_DAYS, PURGE_BATCH),
            )
            stats.del_orders += cur.rowcount
            if cur.rowcount:
                log.info("purge: deleted %d orders older than %s days", cur.rowcount, ORDER_RETENTION_DAYS)

    if random.random() < CUSTOMER_DELETE_PROB:
        # запрос на удаление персональных данных: клиент и все его заказы
        with conn.transaction():
            row = conn.execute(
                "SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED"
            ).fetchone()
            if row is None:
                return
            orders = conn.execute("DELETE FROM shop.orders WHERE customer_id = %s", row).rowcount
            conn.execute("DELETE FROM shop.customers WHERE customer_id = %s", row)
            stats.del_orders += orders
            stats.del_customers += 1
            log.info("pii delete: customer_id=%s with %d orders", row[0], orders)


def step_hold_deletes(conn, stats, n):
    """Режим удержания: вместо новых заказов удаляем n самых старых завершенных."""
    if n <= 0:
        return
    with conn.transaction():
        cur = conn.execute(
            "DELETE FROM shop.orders WHERE order_id IN ("
            "  SELECT order_id FROM shop.orders"
            "  WHERE status IN ('delivered', 'cancelled')"
            "  ORDER BY order_id LIMIT %s FOR UPDATE SKIP LOCKED)",
            (n,),
        )
        stats.del_orders += cur.rowcount
        stats.del_hold += cur.rowcount


# ---------------------------------------------------------------------------
# Ограничение размера базы
# ---------------------------------------------------------------------------

MB = 1024 * 1024


def fmt_mb(n):
    return f"{n / MB:.1f} MB"


class SizeGuard:
    """Выбирает режим генератора по размеру тома базы.

    normal     - размер ниже порога: работаем как обычно
    hold       - размер >= SIZE_THRESHOLD_PCT лимита: новые заказы не создаются,
                 вместо них удаляются самые старые доставленные/отмененные,
                 примерно столько же, сколько создалось бы
    aggressive - размер >= SIZE_AGGRESSIVE_PCT лимита: удаляем в HOLD_AGGRESSIVE_FACTOR
                 раз больше, пока размер не опустится ниже порога
    Размер = pg_database_size всех баз + файлы WAL (pg_ls_waldir).
    DELETE не отдает место ОС, а только освобождает его внутри таблицы, поэтому
    в удержании размер стабилизируется, а вниз уходит в основном за счет WAL,
    который Postgres подрезает на чекпоинтах. VACUUM FULL здесь не вызываем.
    """

    def __init__(self):
        self.limit = SIZE_LIMIT_MB * MB
        self.threshold = self.limit * SIZE_THRESHOLD_PCT / 100
        self.aggressive = self.limit * SIZE_AGGRESSIVE_PCT / 100
        self.mode = "normal"
        self.wal_denied_logged = False
        self.min_orders_logged = False

    def measure(self, conn):
        databases = conn.execute("SELECT sum(pg_database_size(oid))::bigint FROM pg_database").fetchone()[0]
        try:
            wal = conn.execute("SELECT coalesce(sum(size), 0)::bigint FROM pg_ls_waldir()").fetchone()[0]
        except psycopg.errors.InsufficientPrivilege:
            if not self.wal_denied_logged:
                log.error("size: no access to pg_ls_waldir(), WAL is not counted. Run as admin: "
                          "GRANT EXECUTE ON FUNCTION pg_ls_waldir() TO %s", DB_DSN["user"])
                self.wal_denied_logged = True
            wal = None
        return databases, wal

    def check(self, conn, orders_count):
        databases, wal = self.measure(conn)
        total = databases + (wal or 0)
        log.info("size: total=%s (databases=%s wal=%s) = %.0f%% of limit %s | threshold=%s aggressive=%s"
                 " | orders~%d | mode=%s",
                 fmt_mb(total), fmt_mb(databases), "n/a" if wal is None else fmt_mb(wal),
                 total * 100 / self.limit, fmt_mb(self.limit), fmt_mb(self.threshold),
                 fmt_mb(self.aggressive), orders_count, self.mode)

        if total >= self.aggressive:
            new_mode = "aggressive"
        elif total >= self.threshold:
            # из aggressive выходим только ниже порога
            new_mode = "aggressive" if self.mode == "aggressive" else "hold"
        else:
            new_mode = "normal"
        if new_mode == self.mode:
            return

        if new_mode == "hold":
            log.warning("MODE %s -> hold: size %s >= threshold %s (%g%% of limit %s); new orders paused, "
                        "deleting ~%g oldest delivered/cancelled orders per minute instead",
                        self.mode, fmt_mb(total), fmt_mb(self.threshold), SIZE_THRESHOLD_PCT,
                        fmt_mb(self.limit), NEW_ORDERS_PER_MIN)
        elif new_mode == "aggressive":
            log.warning("MODE %s -> aggressive: size %s >= %s (%g%% of limit %s); deleting x%d "
                        "until size is below threshold %s",
                        self.mode, fmt_mb(total), fmt_mb(self.aggressive), SIZE_AGGRESSIVE_PCT,
                        fmt_mb(self.limit), HOLD_AGGRESSIVE_FACTOR, fmt_mb(self.threshold))
        else:
            log.warning("MODE %s -> normal: size %s < threshold %s; creating new orders again",
                        self.mode, fmt_mb(total), fmt_mb(self.threshold))
        self.mode = new_mode
        self.min_orders_logged = False

    def orders_to_delete(self, would_create, orders_count):
        """Сколько заказов удалить в цикле вместо создания новых."""
        n = would_create
        if self.mode == "aggressive":
            n = max(n, 1) * HOLD_AGGRESSIVE_FACTOR
        allowed = max(0, orders_count - HOLD_MIN_ORDERS)
        if n > allowed and not self.min_orders_logged:
            log.warning("%s: only ~%d orders left (HOLD_MIN_ORDERS=%d), not deleting below that; "
                        "the remaining size is WAL and free space inside tables",
                        self.mode, orders_count, HOLD_MIN_ORDERS)
            self.min_orders_logged = True
        return min(n, allowed)


def count_orders(conn):
    return conn.execute("SELECT count(*) FROM shop.orders").fetchone()[0]


def run(conn, stop):
    log.info("run: pause=%ss new_orders=%g/min step=%ss cancel=%.3f purge=%.3f "
             "customer_delete=%.3f anomaly=%.3f",
             CYCLE_PAUSE_SEC, NEW_ORDERS_PER_MIN, ORDER_STEP_SEC, CANCEL_RATE,
             PURGE_PROB, CUSTOMER_DELETE_PROB, ANOMALY_RATE)
    log.info("size limit: %s, hold from %g%%, aggressive from %g%%, check every %d cycles",
             fmt_mb(SIZE_LIMIT_MB * MB), SIZE_THRESHOLD_PCT, SIZE_AGGRESSIVE_PCT, SIZE_CHECK_EVERY_CYCLES)
    guard = SizeGuard()
    orders_count = 0  # число заказов держим в памяти, count(*) только при пересчете
    need_recount = True
    cycle = 0
    prev_started = None
    while not stop["flag"]:
        cycle += 1
        started = time.monotonic()
        # сколько заказов пришло бы за время с прошлого цикла (не больше минуты, например после реконнекта)
        elapsed = CYCLE_PAUSE_SEC if prev_started is None else min(started - prev_started, 60.0)
        prev_started = started
        stats = Stats()
        try:
            if need_recount or cycle % ORDERS_RECOUNT_EVERY_CYCLES == 0:
                actual = count_orders(conn)
                if not need_recount:
                    log.info("orders recount: counter=%d actual=%d", orders_count, actual)
                orders_count, need_recount = actual, False
            if cycle == 1 or cycle % SIZE_CHECK_EVERY_CYCLES == 0:
                guard.check(conn, orders_count)

            would_create = poisson(NEW_ORDERS_PER_MIN * elapsed / 60)
            step_new_customer(conn, stats)
            if guard.mode == "normal":
                step_new_orders(conn, stats, would_create)
            else:
                step_hold_deletes(conn, stats, guard.orders_to_delete(would_create, orders_count))
            step_advance_orders(conn, stats)
            step_update_dictionaries(conn, stats)
            step_physical_deletes(conn, stats)
        except psycopg.OperationalError as e:
            log.error("cycle %d: connection problem: %s", cycle, str(e).strip())
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()
            need_recount = True
            continue
        except psycopg.Error:
            # транзакция шага уже откатилась, следующий цикл начнется с чистого листа
            log.exception("cycle %d: db error, skipping the rest of the cycle", cycle)
            need_recount = True
        orders_count += stats.ins_orders - stats.del_orders

        log.info(
            "cycle %d [%s]: inserted orders=%d customers=%d | updated status=%d (delivered=%d cancelled=%d)"
            " customers=%d points=%d | deleted orders=%d (hold=%d) customers=%d | backdated updated_at=%d"
            " | %.0fms",
            cycle, guard.mode, stats.ins_orders, stats.ins_customers, stats.upd_status, stats.upd_delivered,
            stats.upd_cancelled, stats.upd_customers, stats.upd_points, stats.del_orders, stats.del_hold,
            stats.del_customers, stats.anomalies, (time.monotonic() - started) * 1000,
        )
        sleep_until = time.monotonic() + CYCLE_PAUSE_SEC
        while not stop["flag"] and time.monotonic() < sleep_until:
            time.sleep(max(0.0, min(0.5, sleep_until - time.monotonic())))
    log.info("run: stopped after %d cycles", cycle)


def main():
    logging.basicConfig(level=LOG_LEVEL, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")
    unknown = set(MODES) - {"seed", "run"}
    if not MODES or unknown:
        log.error("GENERATOR_MODE must be seed, run or seed,run; got %r", os.environ.get("GENERATOR_MODE"))
        sys.exit(2)
    if SIZE_LIMIT_MB <= 0 or not (0 < SIZE_THRESHOLD_PCT <= SIZE_AGGRESSIVE_PCT <= 100):
        log.error("need SIZE_LIMIT_MB > 0 and 0 < SIZE_THRESHOLD_PCT <= SIZE_AGGRESSIVE_PCT <= 100")
        sys.exit(2)
    if SIZE_CHECK_EVERY_CYCLES < 1 or ORDERS_RECOUNT_EVERY_CYCLES < 1:
        log.error("SIZE_CHECK_EVERY_CYCLES and ORDERS_RECOUNT_EVERY_CYCLES must be >= 1")
        sys.exit(2)

    stop = {"flag": False}

    def handle_stop(signum, _frame):
        log.info("got signal %s, stopping", signum)
        stop["flag"] = True

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    conn = connect()
    try:
        if "seed" in MODES:
            seed(conn)
        if "run" in MODES:
            run(conn, stop)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
