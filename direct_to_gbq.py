# -*- coding: utf-8 -*-
from __future__ import print_function
import requests
from requests.exceptions import ConnectionError
from time import sleep
import json
import pandas_gbq
import pandas as pd
import io
import random
import datetime
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import numpy as np

def direct_to_bq(clientLogin, token, date_from, date_to, bq_project_id,bq_table_clientname, to_gbq = "none"):
    rand_number_for_reports = random.randint(1,10000)
    rand_number_for_reports = str(rand_number_for_reports)

    print("Начинаю скачивать отчет из Яндекс Директ")

    # Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
    import sys

    if sys.version_info < (3,):
        def u(x):
            try:
                return x.encode("utf8")
            except UnicodeDecodeError:
                return x
    else:
        def u(x):
            if type(x) == type(b''):
                return x.decode('utf8')
            else:
                return x

    # --- Входные данные ---
    # Адрес сервиса Reports для отправки JSON-запросов (регистрозависимый)
    ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'


    # --- Подготовка, выполнение и обработка запроса ---
    # Создание HTTP-заголовков запроса
    headers = {
               # OAuth-токен. Использование слова Bearer обязательно
               "Authorization": "Bearer " + token,
               # Логин клиента рекламного агентства
               "Client-Login": clientLogin,
               # Язык ответных сообщений
               "Accept-Language": "ru",
               # Режим формирования отчета
               "processingMode": "auto",
               # Формат денежных значений в отчете
               # "returnMoneyInMicros": "false",
               # Не выводить в отчете строку с названием отчета и диапазоном дат
                "skipReportHeader": "true",
               # Не выводить в отчете строку с названиями полей
               # "skipColumnHeader": "true",
               # Не выводить в отчете строку с количеством строк статистики
                "skipReportSummary": "true"
               }

    # Создание тела запроса
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
        },
            "FieldNames": [
                "Date",
                "CampaignName",
                "Impressions",
                "Clicks",
                "Cost"
            ],
            "ReportName": u("Дэшборд" + rand_number_for_reports),
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    # Кодирование тела запроса в JSON
    body = json.dumps(body, indent=4)

    # Запуск цикла для выполнения запросов
    # Если получен HTTP-код 200, то выводится содержание отчета
    # Если получен HTTP-код 201 или 202, выполняются повторные запросы
    while True:
        try:
            req = requests.post(ReportsURL, body, headers=headers)
            req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке "UTF-8"
            if req.status_code == 400:
                print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(u(body)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            elif req.status_code == 200:
                print("Отчет создан успешно")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("Содержание отчета: \n{}".format(u(req.text)))
                break
            elif req.status_code == 201:
                print("Отчет успешно поставлен в очередь в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 202:
                print("Отчет формируется в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 500:
                print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            elif req.status_code == 502:
                print("Время формирования отчета превысило серверное ограничение.")
                print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                print("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            else:
                print("Произошла непредвиденная ошибка")
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            print("Произошла ошибка соединения с сервером API")
            # Принудительный выход из цикла
            break

        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанализировать действия приложения
            print("Произошла непредвиденная ошибка")
            # Принудительный выход из цикла
            break


    s = req.content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')), sep = '\t')
    df['Cost'] = df['Cost'].apply(lambda x: x*0.000001)

    campaigns = []
    sources = []

    #df = df.assign(utm_campaign=campaigns)
    #df = df.assign(utm_source=sources)

    print("Загружаю отчет в Big Query")


    if to_gbq == 'replace':
        df.to_gbq(destination_table = bq_table_clientname +'.direct',
                  project_id = bq_project_id,
                  private_key='C:/Users/itimokhin/Desktop/BigQuery Connectors/WM Dashboard Builder-a7f022d47911.json',
                  if_exists = 'replace')
        print("Отчет из деректа загружен в Google BigQuery с ЗАМЕНОЙ")
    elif to_gbq == 'append':
        df.to_gbq(destination_table = bq_table_clientname +'.direct',
                  project_id = bq_project_id,
                  private_key='C:/Users/itimokhin/Desktop/BigQuery Connectors/WM Dashboard Builder-a7f022d47911.json',
                  if_exists = 'append')
        print("Отчет из деректа загружен в Google BigQuery с ДОБАВЛЕНИЕМ")
    else:
        print("Отчет из директа есть в каком-то там датафрейме, но никуда я его так и не загрузил")
    