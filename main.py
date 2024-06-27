from flask import Flask, jsonify
import mysql.connector
import csv
from datetime import datetime
import json

app = Flask(__name__)


def datetime_converter(str):
    if isinstance(str, datetime):
        return str.__str__()


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

        sql = 'Select ID_meteorologica from estacion_meteorologica where estacion = %s'
        cursor.execute(sql, (station,))

        sql_response = cursor.fetchall()

        if len(sql_response) != 1:
            return 'error'

        id_meteorologica = sql_response[0][0]

        for _ in range(5):
            next(reader)

        placeholders = ', '.join(['%s'] * 24)
        columns = 'fecha_y_hora, estacion, AQI, AQI_alto, PM_1, alto_PM_1, PM_2_5, alto_PM_2_5, PM_10, alto_PM_10, ' \
                  'temperatura, temperatura_alta, temperatura_baja, humedad, alta_humedad, punto_de_rocio, ' \
                  'maxima_del_punto_de_rocio, minima_del_punto_de_rocio, bulbo_humedo, baja_humedad, ' \
                  'maxima_temperatura_de_bulbo_humedo, minima_temperatura_de_bulbo_humedo, indice_de_calor, ' \
                  'maxima_del_indice_de_calor'

        sql = f"INSERT INTO calidad_del_aire ({columns}) VALUES ({placeholders})"

        for row in reader:
            row.insert(1, id_meteorologica)
            row[0] = datetime.strptime(row[0], '%m/%d/%y %I:%M %p')

            for i in range(len(row)):
                if i > 1:
                    row[i] = int(row[i])
            cursor.execute(sql, row)

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
    # insert_csv_to_db("C:\\Users\\Erwin\\Downloads\\datos calidad del aire upiita.csv")
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
    cursor.execute('Select ID_meteorologica, estacion from estacion_meteorologica')
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
                    "date_and_time": datetime_converter(data[0]),
                    "PM_1": data[1],
                    "PM_2.5": data[2],
                    "PM_10": data[3],
                    "temp": data[4]
                }
                json[estacion[1]].append(structured_data)

    return json #primera funcion :b


if __name__ == '__main__':
    app.run(debug=True)
