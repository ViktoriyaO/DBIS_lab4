""" Лабораторна робота №4. Орєхова Вікторія, КМ-82.
    Варіант №7
    Завдання. Порівняти найкращий бал з Математики у 2020 та 2019 роках серед тих кому було зараховано тест.
"""
from pymongo import MongoClient
import csv
import time
from datetime import timedelta
try:
    # відкриття текстового файлу, у який записується час виконання завантаження даних у базу даних
    f = open('text.txt', 'w')
    start_time = time.monotonic()
    csvfile1 = open('D:\DBIS\Odata2019File.csv', 'r')
    reader1 = csv.DictReader(csvfile1, delimiter=';')
    csvfile2 = open('D:\DBIS\Odata2020File.csv', 'r')
    reader2 = csv.DictReader(csvfile2, delimiter=';')
    mongo_client=MongoClient()
    db=mongo_client.mydb
    db.zno.drop()
    header= ['OUTID', 'Birth', 'SEXTYPENAME', 'REGNAME', 'AREANAME', 'TERNAME', 'REGTYPENAME', 'TerTypeName','ClassProfileNAME',
            'ClassLangName', 'EONAME', 'EOTYPENAME', 'EORegName', 'EOAreaName', 'EOTerName', 'EOParent', 'UkrTest', 'UkrTestStatus',
            'UkrBall100', 'UkrBall12', 'UkrBall', 'UkrAdaptScale', 'UkrPTName', 'UkrPTRegName', 'UkrPTAreaName', 'UkrPTTerName',
            'histTest', 'HistLang',	'histTestStatus',	'histBall100', 'histBall12',	'histBall', 'histPTName', 'histPTRegName',
            'histPTAreaName',	'histPTTerName', 'mathTest', 'mathLang', 'mathTestStatus',	'mathBall100', 'mathBall12', 'mathBall',
            'mathPTName', 'mathPTRegName', 'mathPTAreaName', 'mathPTTerName', 'physTest', 'physLang',	'physTestStatus', 'physBall100',
            'physBall12', 'physBall', 'physPTName', 'physPTRegName', 'physPTAreaName',	'physPTTerName', 'chemTest', 'chemLang',
            'chemTestStatus',	'chemBall100', 'chemBall12', 'chemBall', 'chemPTName',	'chemPTRegName', 'chemPTAreaName', 'chemPTTerName',
            'bioTest', 'bioLang', 'bioTestStatus', 'bioBall100', 'bioBall12', 'bioBall', 'bioPTName',	'bioPTRegName', 'bioPTAreaName',
            'bioPTTerName', 'geoTest', 'geoLang',	'geoTestStatus', 'geoBall100',	'geoBall12', 'geoBall', 'geoPTName',
            'geoPTRegName', 'geoPTAreaName', 'geoPTTerName', 'engTest', 'engTestStatus',	'engBall100',	'engBall12',	'engDPALevel',
            'engBall', 'engPTName',	'engPTRegName', 'engPTAreaName', 'engPTTerName', 'fraTest', 'fraTestStatus', 'fraBall100',
            'fraBall12', 'fraDPALevel',	'fraBall', 'fraPTName', 'fraPTRegName', 'fraPTAreaName', 'fraPTTerName','deuTest',
            'deuTestStatus', 'deuBall100', 'deuBall12',	'deuDPALevel', 'deuBall', 'deuPTName',	'deuPTRegName', 'deuPTAreaName',
            'deuPTTerName', 'spaTest', 'spaTestStatus', 'spaBall100', 'spaBall12', 'spaDPALevel', 'spaBall', 'spaPTName',
            'spaPTRegName', 'spaPTAreaName', 'spaPTTerName', 'Year']

    for each in reader1:
        row={}
        for field in header:
            row[field]=each[field]
        db.zno.insert_one(row)
    for each in reader2:
        row={}
        for field in header:
            row[field]=each[field]
        db.zno.insert_one(row)
    end_time = time.monotonic()
    print('На запис файлів витрачено --' + str(timedelta(seconds=end_time - start_time)))
    # запис у текстовий файл часу завантаження даних
    l = [str(timedelta(seconds=end_time - start_time))]
    for index in l:
        f.write('На запис файлів витрачено --' + index + '\n')
    f.close()

    # виконання завдання відповідно до варіанту
    query = db.zno.aggregate(
        [
            {'$match': {'mathTestStatus': 'Зараховано'}},
            {'$group': {'_id': {
                                'year': '$Year',
                                'regname': '$REGNAME'},
                        'max': {
                                '$max': '$mathBall100'}
            }},
    ])
    # запис результату виконаного завдання у csv-файл
    with open('task.csv', 'w') as csvfile:
        task_writer = csv.writer(csvfile, delimiter=',')
        task_writer.writerow(['Регіон', 'Максимальний бал', 'Рік'])
        for row in query:
            year = row['_id']['year']
            regname = row['_id']['regname']
            max = row['max']
            task_writer.writerow([regname, max, year])
except Exception:
    print("Помилка з'єднання")