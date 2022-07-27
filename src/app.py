import json
from flask import Flask, jsonify
from flask_cors import CORS

from sqlalchemy import create_engine

import pandas as pd

app = Flask(__name__)
CORS(app)

engine_motel = create_engine('mysql+pymysql://root:arcARC@localhost/motel')
engine_dm = create_engine('mysql+pymysql://root:arcARC@localhost/calidad_servicio')

conn_motel = engine_motel.connect()
con_two_dm = engine_dm.connect()

@app.route("/sync-motel", methods=["GET"])
def syncMotelToDatamart():
  try:

    templateServicio = "INSERT INTO `dim_servicio` VALUES ";
    templateCalificacion = "INSERT INTO `dim_calificacion` VALUES ";
    templateTiempo = "INSERT INTO `dim_tiempo` VALUES ";
    templateHecho = "INSERT INTO `hec_calidad_servicio` VALUES ";

    engine_dm.execute("delete from hec_calidad_servicio")
    engine_dm.execute("delete from dim_servicio")
    engine_dm.execute("delete from dim_calificacion")
    engine_dm.execute("delete from dim_tiempo")
    
    result = engine_motel.execute("SELECT servicios.id_servicio, nombre_servicio, cliente.nombrecliente, calificacion, desc_calificacion, idreservaciones, fechadereservacion FROM cliente, servicios, calificacion, reservaciones, calificacion_servicio WHERE cliente.idcliente = calificacion_servicio.id_cliente AND servicios.id_servicio = calificacion_servicio.id_servicio AND calificacion.calificacion = calificacion_servicio.id_calificacion AND reservaciones.idreservaciones = calificacion_servicio.id_reserva")

    row = result.fetchall()
    datos = [dict(ro) for ro in row]

    #Llenar tabla servicios
    for servicio in datos:
      index = datos.index(servicio) + 1
      name = servicio["nombre_servicio"]
      if (index == len(datos)):
        templateServicio += "(" + str(index) + ", '" + name + "')"
      else:
        templateServicio += "(" + str(index) + ", '" + name + "'),"
    
    #Llenar tabla calificacion
    for servicio in datos:
      index = datos.index(servicio) + 1
      desc_name = servicio["desc_calificacion"]
      name = servicio["calificacion"]
      if (index == len(datos)):
        templateCalificacion += "(" + str(index) + ", '" + desc_name + "', " + str(name) + ")"
      else:
        templateCalificacion += "(" + str(index) + ", '" + desc_name + "', " + str(name) + "),"

    #Llenar tabla tiempo
    for servicio in datos:
      index = datos.index(servicio) + 1
      name = servicio["fechadereservacion"]
      if (index == len(datos)):
        templateTiempo += "(" + str(index) + ", '" + str(name) + "')"
      else:
        templateTiempo += "(" + str(index) + ", '" + str(name) + "'),"

    #Llenar tabla hecho
    for servicio in datos:
      index = datos.index(servicio) + 1
      name = servicio["fechadereservacion"]
      if (index == len(datos)):
        templateHecho += "(" + str(index) + ", " + str(index) + ", " + str(index) + ", " + str(index)  + ")"
      else:
        templateHecho += "(" + str(index) + ", " + str(index) + ", " + str(index) + ", " + str(index)  + "),"

    #inserts to datamart
    result = engine_dm.execute(templateServicio)
    result = engine_dm.execute(templateCalificacion)
    result = engine_dm.execute(templateTiempo)
    result = engine_dm.execute(templateHecho)


    return { 'message': "Datamart Sincronizado" }
  except Exception as e:
    print(e)

@app.route("/datamart-data", methods=["GET"])
def datamartData():
  try:
    result = engine_dm.execute("SELECT dim_servicio.id_servicio, nombre_servicio, calificacion as id_calificacion, pon_calificacion, desc_calificacion, dim_tiempo.id_tiempo, fecha_visita FROM dim_servicio, dim_calificacion, dim_tiempo, hec_calidad_servicio WHERE dim_servicio.id_servicio = hec_calidad_servicio.id_servicio AND dim_calificacion.calificacion = hec_calidad_servicio.id_calificacion AND dim_tiempo.id_tiempo = hec_calidad_servicio.id_tiempo")

    print(result)

    row = result.fetchall()

    return jsonify({'result': [dict(ro) for ro in row]})
  except Exception as e:
    print(e)


if(__name__ == "main"):
  app.run(debug=True)

app.run()

