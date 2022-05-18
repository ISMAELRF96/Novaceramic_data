import psycopg2
import schedule
import time
import psycopg2.extras
import pyodbc
import datetime
import logging


# Tablas Prod,ProdD,inv,invD
# VPN Host: 189.204.110.92   User: Bosonit  Pass: bos637#nv
# columna syncroid 58 en prod, no en prodD,73 en inv,no en invd
# Nºcolumnas prod 63 col,prodD 79  col ,inv 91 col ,invD 74
# Para copiar las tablas de sql a postgres es necesario cambiar datetimea timestamp,bit a bool y quitar constrains idioma

# conexion elliot
user_elliot='postgres'
db_elliot='postgres'
Host_elliot='novaceramic.elliotcloud.com'
pass_elliot='sBq3hR8FawsRpKBjv'
port_elliot=5432

# string with the columns of each table
col_prod="id,empresa,mov,movid,fechaemision,ultimocambio,concepto,proyecto,actividad,uen,moneda,tipocambio,usuario,autorizacion,referencia,docfuente,observaciones,estatus,situacion,situacionfecha,situacionusuario,situacionnota,directo,verdestino,autoreservar,costoadicional,renglonid,almacen,prioridad,fechainicio,fechaentrega,peso,volumen,paquetes,origentipo,origen,origenid,poliza,polizaid,generarpoliza,contid,ejercicio,periodo,fecharegistro,fechaconclusion,fechacancelacion,sucursal,importe,logico1,logico2,logico3,logico4,logico5,logico6,logico7,logico8,logico9,sincroid,sincroc,sucursalorigen,sucursaldestino,posiciondwms,crossdocking"
col_inv="ID,Empresa,Mov,MovID,FechaEmision,UltimoCambio,Concepto,Proyecto,Actividad,UEN,Moneda,TipoCambio,Usuario,Autorizacion,Referencia,DocFuente,Observaciones,Estatus,Situacion,SituacionFecha,SituacionUsuario,SituacionNota,Prioridad,Directo,RenglonID,Almacen,AlmacenDestino,AlmacenTransito,Largo,FechaRequerida,Condicion,Vencimiento,FormaEnvio,OrigenTipo,Origen,OrigenID,Poliza,PolizaID,GenerarPoliza,ContID,Ejercicio,Periodo,FechaRegistro,FechaConclusion,FechaCancelacion,FechaOrigen,Peso,Volumen,Paquetes,FechaEntrega,EmbarqueEstado,Sucursal,Importe,Logico1,Logico2,Logico3,Logico4,Logico5,Logico6,Logico7,Logico8,Logico9,VerLote,EspacioResultado,VerDestino,EstaImpreso,Personal,Reabastecido,Conteo,Agente,ACRetencion,SubModulo,PedimentoExtraccion,SincroID,SincroC,SucursalOrigen,SucursalDestino,ReferenciaMES,PedidoMES,Serie,IDMES,IDMarcaje,MovMES,Motivo,PosicionWMS,PosicionDWMS,PasilloEsp,FiltroEspecifico,ContUso,CrossDocking,MesLanzamiento"
col_prodD="id,renglon,renglonsub,renglonid,renglontipo,autogenerado,almacen,codigo,articulo,subcuenta,cantidad,costo,prodserielote,cantidadpendiente,cantidadreservada,cantidadcancelada,cantidadordenada,cantidada,paquete,destinotipo,destino,destinoid,aplica,aplicaid,cliente,centro,centrodestino,orden,ordendestino,unidad,factor,cantidadinventario,ruta,volumen,sustitutoarticulo,sustitutosubcuenta,fecharequerida,fechaentrega,descripcionextra,ultimoreservadocantidad,ultimoreservadofecha,merma,desperdicio,tipo,comision,manoobra,indirectos,maquila,personal,estacion,estaciondestino,tiempo,tiempounidad,sucursal,turno,tiempoestandarfijo,tiempoestandarvariable,tiempomuerto,causa,logico1,logico2,logico3,ajustecosteo,costoueps,costopeps,ultimocosto,costoestandar,preciolista,departamentodetallista,posicion,tarima,sucursalorigen,costopromedio,costoreposicion,pesoart,aplicarenglon,materiaprima,estacioncat,estacioncatdestino"
col_invD="ID,Renglon,RenglonSub,RenglonID,RenglonTipo,Cantidad,Almacen,Codigo,Articulo,ArticuloDestino,SubCuenta,SubCuentaDestino,Costo,CostoInv,ContUso,Espacio,CantidadReservada,CantidadCancelada,CantidadOrdenada,CantidadPendiente,CantidadA,Paquete,FechaRequerida,Aplica,AplicaID,DestinoTipo,Destino,DestinoID,Cliente,Unidad,Factor,CantidadInventario,UltimoReservadoCantidad,UltimoReservadoFecha,ProdSerieLote,Merma,Desperdicio,Producto,SubProducto,Tipo,Sucursal,Precio,SegundoConteo,DescripcionExtra,AjusteCosteo,CostoUEPS,CostoPEPS,UltimoCosto,CostoEstandar,PrecioLista,DepartamentoDetallista,AjustePrecioLista,Posicion,Tarima,Seccion,FechaCaducidad,SucursalOrigen,CostoPromedio,CostoReposicion,PesoInvArt,AplicaRenglon,PosicionActual,PosicionReal,PosicionDestino,AsignacionUbicacion,MesLanzamiento,MesProdCostoMaquinaria,MesProdCostoMaquila,MesProdCostoConsumoMat,MesProdCostoIndirecto,MesProdCostoManoObra,INFORCostoConsumoMat,INFORCostoManoObra,INFORCostoIndirecto"

query_read_prod = "SELECT * from dbo.prod where Ultimocambio > getdate()-1"
query_read_inv = "SELECT * from dbo.inv where Ultimocambio > getdate()-1"


def get_cambio(query):
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=192.168.2.4,1433; DATABASE=Industrias; UID=Innova; PWD=Nova936#')
        # Defining the connection string
    except:
        logging.info("Error al conectar con base de datos Novaceramic")

    # Fetching the data from the selected table using SQL query
    cursor = conn.cursor()
    cursor.execute(query)
    #cursor.execute("SELECT * from dbo.inv")
    row = cursor.fetchall()
    conn.close()
    return row


def read_Dtable(tabla,nombre):
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=192.168.2.4,1433; DATABASE=industrias; UID=Innova; PWD=Nova936#')
        # Defining the connection string
    except:
        logging.info("Error al conectar con base de datos Novaceramic")
    cursor = conn.cursor()
    x = []
    for i in range(len(tabla)):
        query = 'SELECT * from dbo.'+nombre+' where ID='+str(tabla[i][0])
        cursor.execute(query)
        row = cursor.fetchone()
        if (row != None):
            x.append(row)

    conn.close()
    return x


# conn obj to write,table name,column string,query to read original db,number of columns,number of sincroid column


def upload_many(cursor, nombre, col, tabla,rango,sincroid):
    # change sincroid column from timestamp to datetime
    try:
        for row in tabla:
            fecha = int.from_bytes(row[sincroid], 'big', signed=False)
            date = datetime.datetime.fromtimestamp(fecha)
            row[sincroid] = date

        # create insert query with all columns and correct fields
        row = ""
        for x in range(rango):
            row += "%s,"
        row = row[:len(row) - 1]
        # cursor = conn.cursor()
        Query_write = "INSERT INTO " + nombre + " (" + col + ") VALUES (" + row + ")"
        cursor.executemany(Query_write, tabla)
    except:
        logging.info("No se ha podido subir los datos a elliot")


def upload_manyD(cursor, nombre, col, tabla,rango):
    # subir datos tablasD sin sincroid

    # create insert query with all columns and correct fields
    try:
        row = ""
        for x in range(rango):
            row += "%s,"
        row = row[:len(row) - 1]
        # cursor = conn.cursor()
        Query_write = "INSERT INTO " + nombre + " (" + col + ") VALUES (" + row + ")"
        # print(Query_write)
        cursor.executemany(Query_write, tabla)
    except:
        logging.info("No se ha podido subir los datos a elliot")


def tarea():
    try:
        conn = psycopg2.connect(dbname=db_elliot, user=user_elliot, host=Host_elliot, port=port_elliot, password=pass_elliot)

        #conn = psycopg2.connect(dbname='Novaceramic', user='postgres', host='localhost', password='Mass2077')
    except:
        logging.info("Error al conectar con base de datos elliot")

    cursor = conn.cursor()

    # prod
    tabla = get_cambio(query_read_prod)
    upload_many(cursor,'Prod1',col_prod,tabla,63,57)
    # prodd
    tablaD = read_Dtable(tabla,'ProdD')
    upload_manyD(cursor, 'prodd1', col_prodD, tablaD, 79)

    # inv
    tabla = get_cambio(query_read_inv)
    upload_many(cursor, 'inv', col_inv, tabla, 91, 73)
    tablaD = read_Dtable(tabla, 'invD')
    upload_manyD(cursor,'invd1', col_invD, tablaD, 74)

    conn.commit()
    cursor.close()
    conn.close()


tarea()
