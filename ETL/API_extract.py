from requests import get
import configparser
import datetime
import pandas as pd
import psycopg2
import logging
import os
logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="a",
                        format="%(asctime)s %(levelname)s %(message)s")
API_URL = "http://final-project.simulative.ru/data"
BASE_DIRECT = os.path.dirname(os.path.abspath(__file__))  # текущая директория
PARENT_DIR = os.path.dirname(BASE_DIRECT)
config_path = os.path.join(PARENT_DIR, "config.ini")
config = configparser.ConfigParser()
config.read(config_path)
def data_load(data):
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(
            database=config['database'].get('databname'),
            user=config['database'].get('user'),
            password=config['database'].get('password'),
            host=config['database'].get('host'),
            port=config['database'].get('port')
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Создание таблицы
        add_table = """
        CREATE TABLE IF NOT EXISTS sale_market (
            client_id INTEGER,
            gender VARCHAR(10),
            purchase_datetime date,
            purchase_time_as_seconds_from_midnight INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            price_per_item NUMERIC,
            discount_per_item NUMERIC,
            total_price NUMERIC
        )
        """

        # Добавление уникального ограничения
        add_unique_constraint = """
        ALTER TABLE public.sale_market 
        ADD CONSTRAINT sale_market_unique 
        UNIQUE (
            client_id, 
            gender, 
            purchase_datetime, 
            purchase_time_as_seconds_from_midnight, 
            product_id, 
            quantity, 
            price_per_item, 
            discount_per_item, 
            total_price
        )
        """

        # SQL-запрос для вставки данных
        insert_table = """
        INSERT INTO sale_market (
            client_id, 
            gender,
            purchase_datetime,
            purchase_time_as_seconds_from_midnight,
            product_id,
            quantity,
            price_per_item,
            discount_per_item,
            total_price
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
        ON CONFLICT (
            client_id, 
            gender, 
            purchase_datetime, 
            purchase_time_as_seconds_from_midnight, 
            product_id, 
            quantity, 
            price_per_item, 
            discount_per_item, 
            total_price
        ) 
        DO NOTHING
        """

        # SQL-запрос для проверки дат
        check_date = """SELECT purchase_datetime FROM sale_market"""

        # Выполняем создание таблицы
        cursor.execute(add_table)

        try:
            # Пытаемся добавить уникальное ограничение
            cursor.execute(add_unique_constraint)
        except psycopg2.Error as e:
            # Проверяем, является ли ошибка ошибкой дублирования
            if "already exists" in str(e).lower():
                logging.debug("Уникальное ограничение уже существует, продолжаем работу")
            else:
                raise  # Если это другая ошибка - поднимаем исключение

        # Проверяем существующие даты
        cursor.execute(check_date)
        load_date = cursor.fetchall()
        load_date_df = pd.DataFrame(load_date, columns=['purchase_datetime'])

        # Проверка наличия данных
        if data.loc[0, 'purchase_datetime'].date() in list(load_date_df['purchase_datetime']):
            logging.info(f"Данные за {data.loc[0, 'purchase_datetime'].date()} уже есть в базе")
            return None

        # Вставка данных
        for i in data.itertuples(index=False):
            cursor.execute(insert_table, (
                i.client_id,
                i.gender,
                i.purchase_datetime,
                i.purchase_time_as_seconds_from_midnight,
                i.product_id,
                i.quantity,
                i.price_per_item,
                i.discount_per_item,
                i.total_price
            ))

        logging.info(f"Подключение успешно установлено! {config['database'].get('databname')} данные загружены в базу")

    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")
    finally:
        if conn:
            conn.close()
            print("Соединение закрыто.")
def api_res_to_df(api_url,all=0):
    ytd = datetime.datetime.today().date()-datetime.timedelta(days=1)
    all = float(config['data_settings'].get('all_load'))
    START_DT = ytd
    if all > 0:
        START_DT = config['data_settings'].get('date_start')
    else:
        START_DT = ytd
    print(all)
    try:
        df = pd.DataFrame()
        print(pd.date_range(START_DT,ytd))
        for d in pd.date_range(START_DT,ytd):
            params = {"date": d.date()}
            res = get(API_URL, params)
            if res.status_code == 200:
                df = pd.DataFrame(res.json())
                logging.info(f"Данные за {d.date()} получены в количестве {len(df)}")
                try:
                    df['purchase_datetime']=pd.to_datetime(df['purchase_datetime'], format='%Y-%m-%d')
                    if data_load(df):
                        logging.info(f"Данные за {d.date()} загружены в количестве {len(df)}")
                except Exception as e:
                    logging.info(f"Ошибка при конвертации данных {e} ")
            else:
                logging.error(f"Ошибка получения данных, статус код запроса:{res.status_code}")
        return df
    except Exception as e:
        logging.info(f"Ошибка при получении данных: {e} ")
api_res_to_df(api_url = API_URL)