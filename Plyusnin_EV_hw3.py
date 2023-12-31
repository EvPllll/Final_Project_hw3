# Импорт необходимых библиотек
# решил выполнить импорт в начале несмотря на ущерб в производительности (в данном случае крайне незначительный),
# т.к. визуально нравится :)
# + регламентировано PEP, а именно: написание в начале файла
# сначала встроенные (стандартные) пакеты, затем сторонние

import json
import zipfile
import logging
import asyncio

from datetime import datetime, timedelta
from collections import Counter

import requests

from aiohttp import TCPConnector, ClientSession
from airflow import DAG
# from airflow.operators.bash import BashOperator - закоменчено, т.к. в данном случае не используется.
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

# Аргументы, передаваемые в параметр default_args создаваемого потока работ (DAG)
default_args = {
    'owner': 'Plyusnin_E.V.',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Bash команда для скачивания архива
bash_command = "wget https://ofdata.ru/open-data/download/egrul.json.zip -O " \
               "/home/rtstudent/students/Plyusnin_EV/egrul.json.zip"


def get_data_with_file() -> None:
    '''
    Функция читает файл и записывает нужные данные в БД
    :return: Функция ничего не возвращает
    '''
    logger = logging.getLogger(__name__)

    logger.info('Пытаюсь соединиться с БД...')
    sqlite_hook = SqliteHook(sqlite_get_conn='sqlite_default')
    logger.info('Соединение с БД успешно!')

    # Использовал файл, который предварительно закачан на ВМ
    path_to_file = '/home/rtstudent/egrul.json.zip'

    count = 0

    logger.info('Читаю файл...')
    with zipfile.ZipFile(path_to_file, 'r') as zip_object:
        name_list = zip_object.namelist()
        for name in name_list:
            with zip_object.open(name) as file:
                json_data = file.read()
                data = json.loads(json_data)

                logger.info(f'Формирую строки для записи в БД из файла...')
                for item in data:
                    try:
                        if item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'][0:2] == '61':
                            if item['inn'] == "":
                                item['inn'] = 0
                            okved = item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'],
                            inn = item['inn'],
                            full_name = item['full_name'],
                            kpp = item['kpp'],
                            rows = [(okved, inn, full_name, kpp), ]
                            fields = ['okved', 'inn', 'full_name', 'kpp']

                            logger.info(f'Попытка записи данных {rows} в БД...')
                            sqlite_hook.insert_rows(
                                table='telecom_companies',
                                rows=rows,
                                target_fields=fields
                            )
                            logger.info(f'Данные записаны в БД! Количество записанных строк: {count}.')

                            count += 1

                    except KeyError:
                        logger.debug(f'Что-то пошло не так... Запись в БД данных не осуществлена.')
                        continue


def get_data_with_hh() -> None:
    '''
    Функция производит парсинг данных с ресурса hh и записывает нужные данные в БД
    :return: Функция ничего не возвращает
    '''

    def get_ids() -> list:
        '''
        Функция производит получение ID вакансий
        :return: возвращает список ID
        '''

        # Создаём список, в которы будут записываться ID вакансий
        ids = []

        for i in range(20):
            url_params = url_params = {
                "text": "middle python developer",
                "search_field": "name",
                "industry": "9",
                "per_page": "100",
                "page": f'{str(i)}'
            }

            # Обращается к api hh для поиска ID
            logger.info('Попытка соединения с api hh...')
            result = requests.get('https://api.hh.ru/vacancies', params=url_params)
            logger.info('Соединение с api hh успешно!')

            vacancies = result.json().get('items')

            logger.info(f'Данные со страницы {i + 1} прочитаны')

            for vacancy in vacancies:
                ids.append(vacancy['id'])
                logging.debug(f'ID записан в список, кол-во ID: {len(ids)}')

        return ids

    async def get_vacancy(id: str,
                          session: ClientSession) -> json:
        '''
        Асинхронная функция, получающая данные вакансии, обращаясь в ID
        :param id: ID вакансии
        :param session: Установленная клиентская сессия для парсинга данных посредством библиотеки aiohttp
        :return: возвращает данные вакансии в формате JSON
        '''

        url = f'/vacancies/{id}'

        logger.info(f'Начата загрузка вакансии {url}')

        async with session.get(url=url) as responce:
            vacancy_json = await responce.json()
            logger.info(f'Завершена загрузка вакансии {url}')

            return vacancy_json

    async def main(list_id: list,
                   url: str = 'https://api.hh.ru/') -> None:
        '''
        Функция создаёт клиентскую сессию aiohttp и tasks для eventloop
        Результатом выполнения tasks являются данные вакансии
        :param list_id: список ID вакансий
        :param url: адрес api hh
        :return: функция ничего не возвращает
        '''

        # Задаём ограничение в 5 за обращение с целью защиты от капчи или бана со стороны hh
        connector = TCPConnector(limit=5)
        async with ClientSession(url, connector=connector) as session:
            task_list = []
            for id in list_id:
                task_list.append(asyncio.create_task(get_vacancy(id, session)))

            results = await asyncio.gather(*task_list)
            logger.info('Данные всех найденных вакансий получены!')

        count = 1

        logger.info('Осуществляю поиск нужны данных для записи в БД...')
        for result in results:
            try:
                key_skills_list = []

                company_name = result['employer']['name']
                position = result['name']
                job_description = result['description']
                key_skills = result['key_skills']
                for skill in key_skills:
                    skill = skill['name']
                    key_skills_list.append(skill)
                key_skills_for_write = ', '.join(key_skills_list)

                if key_skills_for_write == '':
                    continue
                else:

                    # Создаётся файл, содержащий в себе все ключевые навыки для составления ТОП в следующем task
                    with open('Key_skills.txt', 'a', encoding='UTF-8') as file:
                        file.write(key_skills_for_write)
                        logger.info(f'Данные {key_skills_for_write} записаны в файл Key_skills.txt!')

                    rows = [(company_name, position, job_description, key_skills_for_write), ]
                    fields = ['company_name', 'position', 'job_description', 'key_skills']

                    logger.info(f'Попытка записи данных {rows} в БД...')
                    sqlite_hook.insert_rows(
                        table='vacancies',
                        rows=rows,
                        target_fields=fields
                    )
                    logger.info(f'Данные записаны в БД! Количество записанных строк: {count}.')

                    count += 1

                    # Осуществляется запись не более 100-а строк с данными вакансий в БД
                    if count == 101:
                        return

            except KeyError:
                logger.debug(f'не удалось осуществить запись данных.')
                continue

    logger = logging.getLogger(__name__)

    logger.info('Пытаюсь соединиться с БД...')
    sqlite_hook = SqliteHook(sqlite_get_conn='sqlite_default')
    logger.info('Соединение с БД успешно!')

    # Осуществляем поиск ID
    ids = get_ids()

    # Обращаемся к api hh с целью получения данных о вакансии по ID
    asyncio.run(main(ids))


def get_top_key_skills() -> None:
    '''
    Функция производит подсчёт ТОП ключевых скиллов и выводит результат в логе
    :return: функция ничего не возвращает
    '''
    logger = logging.getLogger(__name__)

    with open('Key_skills.txt', 'r', encoding='UTF-8') as file:
        logger.info('Файл прочитан')
        top = dict(Counter(list(map(str, file.read().split(',')))))
        logger.info(f'ТОП скиллов сформирован: {sorted(top.items(), key=lambda x: x[1], reverse=True)}')


# Создаём, непосредственно, поток работ
with DAG(
    dag_id='Plyusnin_E.V._DAG_hw3',
    default_args=default_args,
    description='DAG for third homework',
    start_date=datetime(2023, 8, 1),
    schedule_interval='@daily'
) as dag:

    # Закоментил bash-команду т.к. архив ОКВЭД уже имеется в каталоге.
    # download_file = BashOperator(
    #     task_id='download_file',
    #     bash_command=bash_command
    # )

    # создание таблицы telecom companies в БД
    create_table_for_okved = SqliteOperator(
        task_id='create_table_for_okved',
        sqlite_conn_id='sqlite_default',
        sql='''CREATE TABLE IF NOT EXISTS telecom_companies (okved varchar,
                                                             inn bigint,
                                                             full_name varchar,
                                                             kpp bigint);'''
    )

    # создание таблицы vacancies в БД
    create_table_for_vacancies = SqliteOperator(
        task_id='create_table_for_vacancies',
        sqlite_conn_id='sqlite_default',
        sql='''CREATE TABLE IF NOT EXISTS vacancies (company_name varchar,
                                                        position varchar,
                                                        job_description text,
                                                        key_skills varchar);'''
    )
    get_okved_data = PythonOperator(
        task_id='get_data_with_file',
        python_callable=get_data_with_file,
    )
    get_vacancy_data = PythonOperator(
        task_id='get_data_with_hh',
        python_callable=get_data_with_hh,
    )
    get_top_skills = PythonOperator(
        task_id='get_top_key_skills',
        python_callable=get_top_key_skills,
    )

    # Следовало бы еще сделать task, который чистит файл key_skills.txt для того, что бы данные при каждом запуске
    # потока не складывались в топ - он получается неверен, но свою основную функцию выполняет.

    # Формируем граф
    [create_table_for_okved, create_table_for_vacancies] >> get_okved_data >> get_vacancy_data >> get_top_skills

