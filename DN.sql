CREATE DATABASE CALIDAD_DEL_AIRE;

USE CALIDAD_DEL_AIRE;

CREATE TABLE ESTACION_METEOROLOGICA (
    ID_meteorologica INTEGER PRIMARY KEY AUTO_INCREMENT,
    estacion VARCHAR(255) UNIQUE NOT NULL DEFAULT 'Sin nombre',
    latitud FLOAT NOT NULL DEFAULT 0 CHECK (latitud <= 85 AND latitud >= -85),
    longitud FLOAT NOT NULL DEFAULT 0 CHECK (longitud <= 180 AND longitud >= -180)
);


CREATE TABLE CALIDAD_DEL_AIRE(
    fecha_y_hora DATETIME,
    estacion INTEGER,
    AQI SMALLINT NOT NULL DEFAULT 999,
    AQI_alto SMALLINT NOT NULL DEFAULT 999,
    PM_1 SMALLINT NOT NULL DEFAULT 999,
    alto_PM_1 SMALLINT NOT NULL DEFAULT 999,
    PM_2_5 SMALLINT NOT NULL DEFAULT 999,
    alto_PM_2_5 SMALLINT NOT NULL DEFAULT 999,
    PM_10 SMALLINT NOT NULL DEFAULT 999,
    alto_PM_10 SMALLINT NOT NULL DEFAULT 999,
    temperatura SMALLINT NOT NULL DEFAULT 999,
    temperatura_alta SMALLINT NOT NULL DEFAULT 999,
    temperatura_baja SMALLINT NOT NULL DEFAULT 999,
    humedad SMALLINT NOT NULL DEFAULT 999,
    alta_humedad SMALLINT NOT NULL DEFAULT 999,
    baja_humedad SMALLINT NOT NULL DEFAULT 999,
    punto_de_rocio SMALLINT NOT NULL DEFAULT 999,
    maxima_del_punto_de_rocio SMALLINT NOT NULL DEFAULT 999,
    minima_del_punto_de_rocio SMALLINT NOT NULL DEFAULT 999,
    bulbo_humedo SMALLINT NOT NULL DEFAULT 999,
    maxima_temperatura_de_bulbo_humedo SMALLINT NOT NULL DEFAULT 999,
    minima_temperatura_de_bulbo_humedo SMALLINT NOT NULL DEFAULT 999,
    indice_de_calor SMALLINT NOT NULL DEFAULT 999,
    maxima_del_indice_de_calor SMALLINT NOT NULL DEFAULT 999,
    PM_1_promedio_media_hora FLOAT,
    PM_2_5_promedio_media_hora FLOAT,
    PM_10_promedio_media_hora FLOAT,
    temperatura_promedio_media_hora FLOAT,
    PM_1_promedio_hora FLOAT,
    PM_2_5_promedio_hora FLOAT,
    PM_10_promedio_hora FLOAT,
    temperatura_promedio_hora FLOAT,
    PM_1_promedio_dia FLOAT,
    PM_2_5_promedio_dia FLOAT,
    PM_10_promedio_dia FLOAT,
    temperatura_promedio_dia FLOAT,
    PM_1_promedio_mes FLOAT,
    PM_2_5_promedio_mes FLOAT,
    PM_10_promedio_mes FLOAT,
    temperatura_promedio_mes FLOAT,
    PRIMARY KEY (fecha_y_hora, estacion),
    FOREIGN KEY (estacion) REFERENCES ESTACION_METEOROLOGICA(ID_meteorologica)
);

INSERT INTO ESTACION_METEOROLOGICA (estacion, latitud, longitud)
VALUES ('Upiita-IPN', 19.511312393693427, -99.12656804392977);

INSERT INTO ESTACION_METEOROLOGICA (estacion, latitud, longitud)
VALUES ('Escom-IPN', 19.527658401819263, -99.14390068122486);
