from flask import Flask, jsonify
import mysql.connector
import csv
from datetime import datetime, timedelta
import json

app = Flask(__name__)

def exist_data(table, datetime, id_estacion, cursor):
    sql = f'SELECT EXISTS (SELECT 1 FROM {table} WHERE fecha_y_hora = %s AND estacion = %s)'
    parameters = (datetime, id_estacion)
    cursor.execute(sql, parameters)
    return cursor.fetchone()[0]


def get_csv_station(station, cursor):
    sql = 'Select ID_meteorologica from estacion_meteorologica where nombre_estacion = %s'
    cursor.execute(sql, (station,))

    sql_response = cursor.fetchall()

    if len(sql_response) > 1:
        return ValueError("Existen varias estaciones iguales")

    if len(sql_response) == 0:
        return ValueError("No existe la estacion solicitada")

    return sql_response[0][0]


def insert_csv_data(reader, cursor, id_station):
    placeholders = ', '.join(['%s'] * 24)
    columns = 'fecha_y_hora, estacion, AQI, AQI_alto, PM_1, alto_PM_1, PM_2_5, alto_PM_2_5, PM_10, alto_PM_10, ' \
              'temperatura, temperatura_alta, temperatura_baja, humedad, alta_humedad, punto_de_rocio, ' \
              'maxima_del_punto_de_rocio, minima_del_punto_de_rocio, bulbo_humedo, baja_humedad, ' \
              'maxima_temperatura_de_bulbo_humedo, minima_temperatura_de_bulbo_humedo, indice_de_calor, ' \
              'maxima_del_indice_de_calor'

    sql1 = f"INSERT INTO calidad_del_aire ({columns}) VALUES ({placeholders})"

    for row in reader:
        row.insert(1, id_station)
        row[0] = datetime.strptime(row[0], '%m/%d/%y %I:%M %p')
        for i in range(len(row)):
            if i > 1:
                row[i] = int(row[i])

        if not exist_data('calidad_del_aire', row[0], id_station, cursor):
            cursor.execute(sql1, row)


def insert_average_half_hour(cursor, id_station, datetime_start, datetime_end):
    if datetime_start.minute == 15 or datetime_start.minute == 45: # si es posible, obtener los datos desde el 30 o 00 anterior
        datetime_start_query = datetime_start - timedelta(minutes=15)
    elif datetime_start.minute == 00 or datetime_start.minute == 30:
        datetime_start_query = datetime_start - timedelta(minutes=30)

    if datetime_end.minute == 15 or datetime_end.minute == 45:  # si es posible, obtener los datos hasta el 30 o 00 siguiente
        datetime_end = datetime_end + timedelta(minutes=15)
    elif datetime_end.minute == 0 or datetime_end.minute == 30:
        datetime_end = datetime_end + timedelta(minutes=30)

    sql = 'SELECT fecha_y_hora, PM_1, PM_2_5, PM_10, temperatura FROM calidad_del_aire WHERE fecha_y_hora BETWEEN %s AND %s AND estacion = %s'
    parameters = (datetime_start_query, datetime_end, id_station)
    cursor.execute(sql, parameters)
    sql_response = cursor.fetchall()

    PM_1_average_half_hour = 0
    PM_2_5_average_half_hour = 0
    PM_10_average_half_hour = 0
    temp_average_half_hour = 0
    divisor = 0

    columns = 'fecha_y_hora, estacion, PM_1_promedio_media_hora, PM_2_5_promedio_media_hora, PM_10_promedio_media_hora, temperatura_promedio_media_hora'
    placeholders = ', '.join(['%s'] * 6)
    sql1 = f'INSERT INTO promedio_media_hora ({columns}) VALUES ({placeholders})'
    sql2 = 'UPDATE promedio_media_hora SET PM_1_promedio_media_hora = %s, PM_2_5_promedio_media_hora = %s, PM_10_promedio_media_hora = %s, temperatura_promedio_media_hora = %s WHERE fecha_y_hora = %s AND estacion = %s'

    for row in sql_response:
        PM_1_average_half_hour += row[1]
        PM_2_5_average_half_hour += row[2]
        PM_10_average_half_hour += row[3]
        temp_average_half_hour += row[4]
        divisor += 1
        if not exist_data('calidad_del_aire', row[0] + timedelta(minutes=15), id_station, cursor) and not exist_data('calidad_del_aire', row[0] + timedelta(minutes=30), id_station, cursor):
            if exist_data('promedio_media_hora', row[0], id_station, cursor):
                # si existe
                parameters2 = (PM_1_average_half_hour/divisor, PM_2_5_average_half_hour/divisor,
                               PM_10_average_half_hour/divisor, temp_average_half_hour/divisor,
                               row[0], id_station)
                cursor.execute(sql2, parameters2)
            else:
                # si no existe
                parameters1 = (row[0], id_station, PM_1_average_half_hour/divisor,
                               PM_2_5_average_half_hour/divisor,
                               PM_10_average_half_hour/divisor, temp_average_half_hour/divisor)
                cursor.execute(sql1, parameters1)
            PM_1_average_half_hour = 0
            PM_2_5_average_half_hour = 0
            PM_10_average_half_hour = 0
            temp_average_half_hour = 0
            divisor = 0
        elif sql_response[0] == row and sql_response[0][0] == datetime_start - timedelta(minutes=30):
            pass
        elif row[0].minute == 0 or row[0].minute == 30:
            if exist_data('promedio_media_hora', row[0] - timedelta(minutes=30), id_station, cursor):
                # si existe
                parameters2 = (PM_1_average_half_hour / divisor, PM_2_5_average_half_hour / divisor,
                               PM_10_average_half_hour / divisor, temp_average_half_hour / divisor,
                               row[0] - timedelta(minutes=30), id_station)
                cursor.execute(sql2, parameters2)
            else:
                # si no existe
                parameters1 = (row[0] - timedelta(minutes=30), id_station, PM_1_average_half_hour / divisor,
                               PM_2_5_average_half_hour / divisor,
                               PM_10_average_half_hour / divisor, temp_average_half_hour / divisor)
                cursor.execute(sql1, parameters1)

            PM_1_average_half_hour = row[1]
            PM_2_5_average_half_hour = row[2]
            PM_10_average_half_hour = row[3]
            temp_average_half_hour = row[4]
            divisor = 1
        elif (row[0].minute == 15 or row[0].minute == 45) and not exist_data('calidad_del_aire', row[0] + timedelta(minutes=15), id_station, cursor):
            if exist_data('promedio_media_hora', row[0] - timedelta(minutes=15), id_station, cursor):
                # si existe
                parameters2 = (PM_1_average_half_hour / divisor, PM_2_5_average_half_hour / divisor,
                               PM_10_average_half_hour / divisor, temp_average_half_hour / divisor,
                               row[0] - timedelta(minutes=15), id_station)
                cursor.execute(sql2, parameters2)
            else:
                # si no existe
                parameters1 = (row[0] - timedelta(minutes=15), id_station, PM_1_average_half_hour / divisor,
                               PM_2_5_average_half_hour / divisor,
                               PM_10_average_half_hour / divisor, temp_average_half_hour / divisor)
                cursor.execute(sql1, parameters1)
            PM_1_average_half_hour = 0
            PM_2_5_average_half_hour = 0
            PM_10_average_half_hour = 0
            temp_average_half_hour = 0
            divisor = 0



def insert_average_hour(cursor, id_station, datetime_start, datetime_end):
    if datetime_start.minute == 0:
        datetime_start_query = datetime_start - timedelta(hours=1)
    else:
        datetime_start_query = datetime_start - timedelta(minutes=datetime_start.minute)
    datetime_end_query = datetime_end + timedelta(hours=1) - timedelta(minutes=datetime_end.minute)
    datetime = datetime_start_query

    sql1 = 'SELECT PM_1, PM_2_5, PM_10, temperatura FROM calidad_del_aire WHERE fecha_y_hora BETWEEN %s AND %s AND estacion = %s ORDER BY fecha_y_hora ASC'
    sql2 = 'UPDATE promedio_hora SET PM_1_promedio_hora = %s, PM_2_5_promedio_hora = %s, PM_10_promedio_hora = %s, temperatura_promedio_hora = %s WHERE fecha_y_hora = %s AND estacion = %s'
    columns = 'fecha_y_hora, estacion, PM_1_promedio_hora, PM_2_5_promedio_hora, PM_10_promedio_hora, temperatura_promedio_hora'
    placeholders = ', '.join(['%s'] * 6)
    sql3 = f'INSERT INTO promedio_hora ({columns}) VALUES ({placeholders})'

    while datetime < datetime_end_query:
        parameters1 = (datetime, datetime + timedelta(hours=1), id_station)
        cursor.execute(sql1, parameters1)
        sql_response = cursor.fetchall()
        response_len = len(sql_response)
        if response_len != 0:
            PM_1_average_hour = 0
            PM_2_5_average_hour = 0
            PM_10_average_hour = 0
            temp_average_hour = 0
            for row in sql_response:
                PM_1_average_hour += row[0]
                PM_2_5_average_hour += row[1]
                PM_10_average_hour += row[2]
                temp_average_hour += row[3]
            if exist_data('promedio_hora', datetime, id_station, cursor):
                parameters2 = (PM_1_average_hour / response_len, PM_2_5_average_hour / response_len,
                               PM_10_average_hour / response_len, temp_average_hour / response_len,
                               datetime, id_station)
                cursor.execute(sql2, parameters2)
            else:
                parameters3 = (datetime, id_station, PM_1_average_hour / response_len,
                               PM_2_5_average_hour / response_len,
                               PM_10_average_hour / response_len, temp_average_hour / response_len)
                cursor.execute(sql3, parameters3)

        datetime += timedelta(hours=1)


def insert_csv_to_db(csv_route):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="n0m3l0",
        database="calidad_del_aire"
    )

    cursor = conn.cursor()

    with open(csv_route, 'r') as file:
        reader = csv.reader(file)
        station_row = next(reader)
        station = station_row[0]
        id_station = get_csv_station(station, cursor)

        for i in range(5):
            next(reader)

        insert_csv_data(reader, cursor, id_station)
        file.seek(0)
        datetime_start_file = file.readlines()[6].strip().split(',')[0].replace('"', '')
        file.seek(0)
        datetime_end_file = file.readlines()[-1].strip().split(',')[0].replace('"', '')

    datetime_start_file = datetime.strptime(datetime_start_file, "%m/%d/%y %I:%M %p")
    datetime_end_file = datetime.strptime(datetime_end_file, "%m/%d/%y %I:%M %p")

    #insert_average_half_hour(cursor, id_station, datetime_start_file, datetime_end_file)
    insert_average_hour(cursor, id_station, datetime_start_file, datetime_end_file)

    conn.commit()
    cursor.close()
    conn.close()


def get_climate_data(datetime_start, datetime_end, estacion):
    parameters = [datetime.strptime(datetime_start, "%Y-%m-%dT%H:%M:%S"), datetime.strptime(datetime_end, "%Y-%m-%dT%H:%M:%S"), estacion]

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="n0m3l0",
        database="calidad_del_aire"
    )

    cursor = conn.cursor()

    sql = 'SELECT fecha_y_hora, PM_1, PM_2_5, PM_10, temperatura FROM calidad_del_aire WHERE fecha_y_hora BETWEEN %s AND %s AND estacion = %s'
    cursor.execute(sql, parameters)
    sql_response = cursor.fetchall()

    conn.commit()

    cursor.close()
    conn.close()

    return sql_response


@app.route("/")
def menu():
    insert_csv_to_db("C:\\Users\\Erwin\\Downloads\\UrbanDataLab_1-1-24_12-00_AM_1_Year_1720733515_v2.csv")
    #insert_csv_to_db("C:\\Users\\Erwin\\Downloads\\datos calidad del aire upiita.csv")
    return jsonify({"mensaje": "okay"})


@app.route("/get_all_data_all_station/<string:datetime_start>/<string:datetime_end>")
def get_all_station_data(datetime_start, datetime_end):

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="n0m3l0",
        database="calidad_del_aire"
    )

    cursor = conn.cursor()
    cursor.execute('Select ID_meteorologica, nombre_ubicacion_estacion from estacion_meteorologica')
    sql_response = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    json = {}

    for estacion in sql_response:
        climate_data = get_climate_data(datetime_start, datetime_end, estacion[0])
        if climate_data:
            json[estacion[1]] = []

            for data in climate_data:
                structured_data = {
                    "date_and_time": data[0].__str__(),
                    "PM_1": data[1],
                    "PM_2.5": data[2],
                    "PM_10": data[3],
                    "temp": data[4]
                }
                json[estacion[1]].append(structured_data)

    return json


if __name__ == '__main__':
    app.run(debug=True)
