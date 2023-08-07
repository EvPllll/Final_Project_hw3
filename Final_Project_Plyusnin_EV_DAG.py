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
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

# Аргументы, передаваемые в параметр default_args создаваемого потока работ (DAG)
default_args = {
    'owner': 'Plyusnin_EV',
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
    sqlite_hook = SqliteHook(sqlite_get_conn='Plyusnin_EV')
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

                logger.info(f'Формирую строку для БД из {data}...')
                for item in data:
                    try:
                        if item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'][0:2] == '61':
                            if item['inn'] == "":
                                item['inn'] = 0
                            okved = item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'],
                            inn = item['inn'],
                            full_name = item['full_name'],
                            kpp = item['kpp'],
                            orgn = item['ogrn']
                            rows = [(okved, inn, full_name, kpp, orgn), ]
                            fields = ['okved', 'inn', 'full_name', 'kpp', 'orgn']

                            logger.info(f'Попытка записи данных {rows} в БД...')
                            sqlite_hook.insert_rows(
                                table='telecom_companies',
                                rows=rows,
                                target_fields=fields
                            )
                            logger.info(f'Данные записаны в БД! Количество записанных строк: {count}.')

                            count += 1

                    except ValueError:
                        logger.debug(f'Что-то пошло не так... Запись в БД данных {rows} не осуществлена.')


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

            except ValueError:
                logging.debug('не удалось осуществить запись :(')
                continue

    logger = logging.getLogger(__name__)

    logger.info('Пытаюсь соединиться с БД...')
    sqlite_hook = SqliteHook(sqlite_get_conn='Plyusnin_EV')
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


with DAG(
    dag_id='Plyusnin_EV_hw3',
    default_args=default_args,
    description='DAG for third homework',
    start_date=datetime(2023, 8, 8, 8),
    schedule_interval='@daily'
) as dag:
    task_1 = BashOperator(
        task_id='download_file',
        bash_command=bash_command
    )
    task_2 = PythonOperator(
        task_id='get_data_with_file',
        python_callable=get_data_with_file
    )
    task_3 = PythonOperator(
        task_id='get_data_sith_hh',
        python_callable=get_data_with_hh
    )
    task_4 = PythonOperator(
        task_id='get_top_key_skills',
        python_callable=get_top_key_skills
    )

    task_1 >> task_2 >> task_3 >> task_4
