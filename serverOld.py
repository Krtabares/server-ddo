import asyncio
import aiohttp
import unidecode
import uuid
import aiosmtplib
import socketio
import requests
import random
import cx_Oracle
import traceback
from datetime import datetime, timedelta
from sanic import Sanic
from sanic import response
from sanic_cors import CORS, cross_origin
from sanic.handlers import ErrorHandler
from sanic.exceptions import SanicException
from sanic.log import logger
# from sanic_jwt_extended import (JWTManager, jwt_required, create_access_token,create_refresh_token)
from sanic_jwt_extended import (JWT, jwt_required)
from sanic_jwt_extended.exceptions import JWTExtendedException
from sanic_jwt_extended.tokens import Token
from motor.motor_asyncio import AsyncIOMotorClient
from sanic.exceptions import ServerError
from sanic_openapi import swagger_blueprint
from sanic_openapi import doc
from sanic_compress import Compress
from sanic_jinja2 import SanicJinja2
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr

import http.client
import json

class CustomHandler(ErrorHandler):
    def default(self, request, exception):
        # print("[EXCEPTION] "+str(exception))
        return response.json(str(exception),501)

app = Sanic(__name__)
port = 3500

sio = socketio.AsyncServer(async_mode='sanic')
sio.attach(app)
# handler = CustomHandler()
# app.error_handler = handler
with JWT.initialize(app) as manager:
    manager.config.secret_key = "ef8f6025-ec38-4bf3-b40c-29642ccd63128995"
    manager.config.jwt_access_token_expires = timedelta(minutes=120)
    manager.config.rbac_enable = True
# app.config.JWT_SECRET_KEY = "ef8f6025-ec38-4bf3-b40c-29642ccd6312"
# app.config.JWT_ACCESS_TOKEN_EXPIRES = timedelta(minutes=120)
# app.config.RBAC_ENABLE = True
# jwt = JWTManager(app)
app.blueprint(swagger_blueprint)
CORS(app, automatic_options=True)
Compress(app)
jinja = SanicJinja2(app)

def get_mongo_db():
    mongo_uri = "mongodb://127.0.0.1:27017/ddo"
    client = AsyncIOMotorClient(mongo_uri)
    db = client['ddo']
    return db

def get_db():
    dsn_tns = cx_Oracle.makedsn('192.168.168.218', '1521', service_name='DELOESTE')
    # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    conn = cx_Oracle.connect(user=r'APLPAGWEB', password='4P1P4GWE3', dsn=dsn_tns)
    #conn = cx_Oracle.connect(user=r'paginaweb', password='paginaweb', dsn=dsn_tns)
    # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
    #For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
    return conn

def searchUser(username, password):
    for item in users:
        if item["username"] == username and item["password"] == password:
            return item
    return False

@app.route('/resetPass', ["POST", "GET"])
async def availableUser(request):
    data = request.json
    db = get_mongo_db()
    # username = data.get("username", None)
    user = None

    if 'username' in data : 
        user = await db.user.find_one({'username' : data.get("username", None)}, {'_id' : 0})

    elif user == None : 
        user = await db.user.find_one({'email' : data.get("username", None)}, {'_id' : 0})
    
    else:
        response.json({"msg": "Missing username parameter"}, status=400)

    if user == None:
        response.json({"msg": "Missing username parameter"}, status=400)

    

    emailData = dict(
        template = "",
        to = user['email'],
        subject = "Reinicio de Password",
        user = user,
        newpass = data.get("newpass", None),
        password = data.get("password", None)
    )
    print(emailData)
    smtp = http.client.HTTPConnection('127.0.0.1', 8888)
    result = smtp.request("POST", "/put",
                json.dumps(emailData)
                )


    # await prepareMail(emailData)

    return response.json(result,200)    

@app.route("/login", ["POST"])
async def login(request):
    data = request.json
    # print(data)
    username = data.get("username", None)
    password = data.get("password", None)

    if not username:
        return response.json({"msg": "Missing username parameter"}, status=400)
    if not password:
        return response.json({"msg": "Missing password parameter"}, status=400)

    db = get_mongo_db()

    user = await db.user.find_one({'username' : username}, {'_id' : 0})
    # print(user)
    if user:

        if user['password'] == password:

            if user['estatus'] != "Activo" :
                return response.json({"msg": "Usuario inactivo"}, status=430)

            # access_token = await create_access_token(identity=username, app=request.app)
            access_token = JWT.create_access_token(identity=username)
            return response.json({'access_token': access_token, 'user': user}, 200)



    return response.json({"msg": "Bad username or password"}, status=403)

@app.route("/refresh_token", ["POST", "GET"])
@jwt_required
async def refresh_token(request):
    # refresh_token = await create_refresh_token( identity=str(uuid.uuid4()), app=request.app )
    refresh_token = JWT.create_refresh_token(identity=str(uuid.uuid4()))
    return response.json({'refresh_token': refresh_token}, status=200)

@app.route("/validate_token", ["POST", "GET"])
async def validate_token(request, token : Token):
    #print("valido")
    return response.json({'msg': "OK"}, status=200)

@app.route('/add/user', ["POST", "GET"])
@jwt_required
async def addUser(request, token : Token):
    user = request.json
    db = get_mongo_db()

    userEmail = await db.user.find_one({'email' : user.get("email", None)}, {'_id' : 0})

    if userEmail != None:
        return response.json({"msg": "Email no disponible"}, status=400)
    
    username = await db.user.find_one({'username' : user.get("username", None)}, {'_id' : 0})

    if username != None:
        return response.json({"msg": "Username no disponible"}, status=400)

    await db.user.insert_one(user)

    return response.json("OK", 200)

@app.route('/upd/user', ["POST", "GET"])
@jwt_required
async def addUser(request, token : Token):
    user = request.json
    db = get_mongo_db()

    # await db.user.insert_one(user)

    await db.user.update({'username' : user.get("username", None)},user)

    return response.json("OK", 200)

@app.route('/upd/user_pass', ["POST", "GET"])
@jwt_required
async def user_pass(request, token : Token):
    user = request.json
    db = get_mongo_db()

    # await db.user.insert_one(user)

    await db.user.update({'username' : user.get("username", None)},{ "$set" : {'password':user.get("password", None)}})

    return response.json("OK", 200)

@app.route('/del/user', ["POST", "GET"])
@jwt_required
async def addUser(request, token : Token):
    user = request.json
    db = get_mongo_db()

    # await db.user.insert_one(user)

    await db.user.remove({'username' : user.get("username", None)})

    return response.json("OK", 200)

@app.route('/get/users', ["POST", "GET"])
@jwt_required
async def listUser(request, token : Token):
    data = request.json
    db = get_mongo_db()

    users = []
    print(data)
    if 'role' in data :

        if  data['role'] == "root" :

            users = await db.user.find({'role':{'$in':['root','sisAdm','seller']}}, {'_id' : 0}).to_list(length=None)

        if  data['role'] == "sisAdm" :

            users = await db.user.find({'role':{'$in':['sisAdm','seller']}}, {'_id' : 0}).to_list(length=None)

    else:
        users = await db.user.find({'COD_CLIENTE' : data['pCliente']}, {'_id' : 0}).to_list(length=None)



    return response.json(users,200)

@app.route('/get/user', ["POST", "GET"])
@jwt_required
async def availableUser(request, token : Token):
    data = request.json
    db = get_mongo_db()
    # username = data.get("username", None)
    users = await db.user.find_one({'username' : data.get("username", None)}, {'_id' : 0})

    return response.json(users,200)

@app.route('/available/user', ["POST", "GET"])
@jwt_required
async def availableUser(request, token : Token):
    data = request.json
    db = get_mongo_db()
    # username = data.get("username", None)
    users = await db.user.find_one({'username' : data.get("username", None)}, {'_id' : 0})

    return response.json(users,200)

@app.route('/available/email', ["POST", "GET"])
@jwt_required
async def availableUser(request, token : Token):
    data = request.json
    db = get_mongo_db()
    # username = data.get("username", None)
    users = await db.user.find_one({'email' : data.get("email", None)}, {'_id' : 0})

    return response.json(users,200)

@app.route('/disponible_cliente', ["POST", "GET"])
async def procedure(request):

    data = request.json

    db = get_db()
    c = db.cursor()


    if not 'pCliente' in data :
        return response.json({"msg": "Missing parameter"}, status=400)

    if not 'pNoCia' in data :
        return response.json({"msg": "Missing parameter"}, status=400)

    if not 'pNoGrupo' in data :
        return response.json({"msg": "Missing parameter"}, status=400)


    #print(data)

    c.callproc("dbms_output.enable")
    sql = """
                DECLARE

                  vdisp_bs varchar2(20);
                  vdisp_usd varchar2(30);


                BEGIN

                    PROCESOSPW.disponible_cliente(vdisp_bs, vdisp_usd, \'{pNoCia}\', \'{pNoGrupo}\', \'{pCliente}\');

                    dbms_output.put_line(vdisp_bs|| '|'||vdisp_usd);
                END;
            """.format(pNoCia = data['pNoCia'],
                        pNoGrupo = data['pNoGrupo'],
                        pCliente = data['pCliente'])
    # #print("==========================disponible_cliente================================")
    # #print(sql)
    c.execute(sql)
    textVar = c.var(str)
    statusVar = c.var(int)
    obj = {}
    while True:
        c.callproc("dbms_output.get_line", (textVar, statusVar))
        if textVar.getvalue() == None:
            break
        #print("==========================================================")
        #print(textVar.getvalue())
        arr = str(textVar.getvalue()).split("|")
        obj = {
        'disp_bs' : formatFloatDdo(arr[0]),
        'disp_usd': formatFloatDdo(arr[1])
        }
        if statusVar.getvalue() != 0:
            break


    return response.json({"msj": "OK", "obj": obj}, 200)

@app.route('/procedure_clientes', ["POST", "GET"])
async def procedure(request):

    data = request.json

    db = get_db()
    c = db.cursor()

    #print(data)
    if not 'pTotReg' in data or data['pTotReg'] == 0 :
        data['pTotReg'] = 100

    if not 'pTotPaginas' in data or data['pTotPaginas'] == 0 :
        data['pTotPaginas'] = 100

    if not 'pPagina' in data  :
        data['pPagina'] = 'null'

    if not 'pLineas' in data or data['pLineas'] == 0 :
        data['pLineas'] = 100

    if not 'pDireccion' in data :
        data['pDireccion'] = 'null'
    else:
        data['pDireccion'] = "'"+data['pDireccion']+"'"

    if not 'pNoCia' in data :
        data['pNoCia'] = 'null'
    else:
        data['pNoCia'] = "'"+data['pNoCia']+"'"

    if not 'pNoGrupo' in data :
        data['pNoGrupo'] = 'null'
    else:
        data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"
    if not 'pCliente' in data :
        data['pCliente'] = 'null'
    else:
        data['pCliente'] = "'"+data['pCliente']+"'"

    if not 'pNombre' in data :
        data['pNombre'] = 'null'
    else:
        data['pNombre'] = "'"+data['pNombre']+"'"

    #print(data)
    c.callproc("dbms_output.enable")

    sql = """
                DECLARE
            l_cursor  SYS_REFCURSOR;
            pTotReg number DEFAULT 100;
            pTotPaginas number DEFAULT 100;
            pPagina number DEFAULT null;
            pLineas number DEFAULT 100;
            pCliente varchar2(50) DEFAULT null;
            pNoCia varchar2(10) DEFAULT null;
            pNoGrupo varchar2(10) DEFAULT null;
            pNombre varchar2(50) DEFAULT null;
            pDireccion varchar2(50) DEFAULT null;
            output number DEFAULT 1000000;
                v_cod_cia varchar2(2);
                v_nombre_cia varchar2(100);
                v_grupo_cliente varchar2(2);
                v_nom_grupo_cliente varchar2(50);
                v_cod_cliente varchar2(50);
                v_nombre_cliente varchar2(200);
                v_direccion_cliente varchar2(250);
                v_direccion_entrega_cliente varchar2(250);
                v_docu_identif_cliente varchar2(50);
                v_nombre_encargado varchar2(100);
                v_telefono1 varchar2(50);
                v_telefono2 varchar2(50);
                v_telefono3 varchar2(50);
                v_telefono4 varchar2(50);
                v_email1 varchar2(100);
                v_email2 varchar2(100);
                v_email3 varchar2(100);
                v_email4 varchar2(100);
                v_plazo varchar2(100);
                v_persona_cyc varchar2(100);
                v_zona varchar2(25);
                v_monto_minimo number;
                v_tipo_venta varchar2(100);
                v_limite_credito number;
                v_vendedor varchar2(100);
                v_max_unid_med_emp number;
                v_max_unid_misc_emp number;
                v_unid_fact_med_emp number;
                v_unid_fact_misc_emp number;
                v_unid_disp_med_emp number;
                v_unid_disp_misc_emp number;
                v_monto_min_pick number;
                ind_emp_nolim varchar2(1);
                v_email_vendedor varchar2(100);
                v_email_persona_cyc varchar2(100);
                v_tot number;
                v_pagina number;
                v_linea number;
            BEGIN

                    pTotReg  := {pTotReg};
                    pTotPaginas  := {pTotPaginas};
                    pPagina  := {pPagina};
                    pLineas  := {pLineas};
                    pNoCia := {pNoCia};
                    pNoGrupo := {pNoGrupo};
                    pCliente := {pCliente};
                    pNombre := {pNombre};
                    pDireccion := {pDireccion};

                dbms_output.enable(output);
                PROCESOSPW.clientes (l_cursor, pTotReg, pTotPaginas, pPagina, pLineas, pNoCia, pNoGrupo, pCliente, pNombre, pDireccion);

            LOOP
            FETCH l_cursor into
                v_cod_cia,
                v_nombre_cia,
                v_grupo_cliente,
                v_nom_grupo_cliente,
                v_cod_cliente,
                v_nombre_cliente,
                v_direccion_cliente,
                v_direccion_entrega_cliente,
                v_docu_identif_cliente,
                v_nombre_encargado,
                v_telefono1,
                v_telefono2,
                v_telefono3,
                v_telefono4,
                v_email1,
                v_email2,
                v_email3,
                v_email4,
                v_plazo,
                v_persona_cyc,
                v_zona,
                v_monto_minimo,
                v_tipo_venta,
                v_limite_credito,
                v_vendedor,
                v_max_unid_med_emp,
                v_max_unid_misc_emp,
                v_unid_fact_med_emp,
                v_unid_fact_misc_emp,
                v_unid_disp_med_emp,
                v_unid_disp_misc_emp,
                v_monto_min_pick,
                ind_emp_nolim,
                v_email_vendedor,
                v_email_persona_cyc,
                v_pagina,
                v_linea;
                EXIT WHEN l_cursor%NOTFOUND;
            dbms_output.put_line
                (
                v_cod_cia|| '|'||
                v_nombre_cia|| '|'||
                v_grupo_cliente|| '|'||
                v_nom_grupo_cliente|| '|'||
                v_cod_cliente|| '|'||
                v_nombre_cliente|| '|'||
                v_direccion_cliente|| '|'||
                v_direccion_entrega_cliente|| '|'||
                v_docu_identif_cliente|| '|'||
                v_nombre_encargado|| '|'||
                v_telefono1|| '|'||
                v_telefono2|| '|'||
                v_telefono3|| '|'||
                v_telefono4|| '|'||
                v_email1|| '|'||
                v_email2|| '|'||
                v_email3|| '|'||
                v_email4|| '|'||
                v_plazo|| '|'||
                v_persona_cyc|| '|'||
                v_zona|| '|'||
                v_monto_minimo|| '|'||
                v_tipo_venta|| '|'||
                v_limite_credito|| '|'||
                v_vendedor|| '|'||
                v_max_unid_med_emp|| '|'||
                v_max_unid_misc_emp|| '|'||
                v_unid_fact_med_emp|| '|'||
                v_unid_fact_misc_emp|| '|'||
                v_unid_disp_med_emp|| '|'||
                v_unid_disp_misc_emp|| '|'||
                v_monto_min_pick|| '|'||
                ind_emp_nolim|| '|'||
                v_email_vendedor|| '|'||
                v_email_persona_cyc|| '|'||
                v_pagina|| '|'||
                v_linea
                );
            END LOOP;
            CLOSE l_cursor;
        END;
            """.format(
                        pTotReg = data['pTotReg'],
                        pTotPaginas = data['pTotPaginas'],
                        pPagina = data['pPagina'],
                        pLineas = data['pLineas'],
                        pDireccion = data['pDireccion'],
                        pNoCia = data['pNoCia'],
                        pNoGrupo = data['pNoGrupo'],
                        pCliente = data['pCliente'],
                        pNombre = data['pNombre'],
                    )
    print(sql)
    c.execute(sql)
    textVar = c.var(str)
    statusVar = c.var(int)
    list = []
    while True:
        c.callproc("dbms_output.get_line", (textVar, statusVar))
        if statusVar.getvalue() != 0:
            break
        arr = str(textVar.getvalue()).split("|")
        obj = {
        'cod_cia' : arr[0],
        'nombre_cia': arr[1],
        'grupo_cliente': arr[2],
        'nom_grupo_cliente': arr[3],
        'cod_cliente': arr[4],
        'nombre_cliente': arr[5],
        'direccion_cliente': arr[6],
        'direccion_entrega_cliente' :arr[7],
        'docu_identif_cliente': arr[8],
        'nombre_encargado': arr[9],
        'telefono1': arr[10],
        'telefono2': arr[11],
        'telefono3': arr[12],
        'telefono4': arr[13],
        'email1': arr[14],
        'email2': arr[15],
        'email3': arr[16],
        'email4': arr[17],
        'v_plazo': arr[18],
        'v_persona_cyc': arr[19],
        'zona': arr[20],
        'monto_minimo':arr[21],
        'tipo_venta':arr[22],
        'limite_credito':arr[23],
        'vendedor':arr[24],
        'max_unid_med_emp' :arr[25],
        'max_unid_misc_emp' :arr[26],
        'unid_fact_med_emp' :arr[27],
        'unid_fact_misc_emp' :arr[28],
        'unid_disp_med_emp' :arr[29],
        'unid_disp_misc_emp' :arr[30],
        'monto_min_pick' :arr[31],
        'ind_emp_nolim' :arr[32],
        'email_vendedor' : arr[33],
        'email_persona_cyc' : arr[34],
        'pagina': arr[35],
        'linea': arr[36]
        }
        list.append(obj)
    return response.json({"msj": "OK", "obj": list}, 200)

@app.route('/procedure_deudas', ["POST", "GET"])
@jwt_required
async def procedure(request , token : Token):
# async def procedure(request):

    data = request.json

    if not 'pTotReg' in data or data['pTotReg'] == 0 :
        data['pTotReg'] = 100

    if not 'pTotPaginas' in data or data['pTotPaginas'] == 0 :
        data['pTotPaginas'] = 100

    if not 'pPagina' in data or data['pPagina'] == 0 :
        data['pPagina'] = 1

    if not 'pLineas' in data or data['pLineas'] == 0 :
        data['pLineas'] = 100

    if not 'pDeuda' in data :
        data['pDeuda'] = 'null'

    if not 'pNoCia' in data :
        data['pNoCia'] = '01'
    else:
        data['pNoCia'] = "'"+data['pNoCia']+"'"

    if not 'pNoGrupo' in data :
        data['pNoGrupo'] = '01'
    else:
        data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"

    if not 'pCLiente' in data :
        data['pCLiente'] = 'null'
    else:
        data['pCLiente'] = "'"+data['pCLiente']+"'"

    if not 'pNombre' in data :
        data['pNombre'] = 'null'
    else:
        data['pNombre'] = "'"+data['pNombre']+"'"

    if not 'pTipo' in data :
        data['pTipo'] = 'null'
    else:
        data['pTipo'] = "'"+data['pTipo']+"'"

    if not 'pEstatus' in data :
        data['pEstatus'] = 'null'
    else:
        data['pEstatus'] = "'"+data['pEstatus']+"'"


    db = get_db()
    c = db.cursor()
    c.callproc("dbms_output.enable")
    #print(data)
    sql = """DECLARE
                l_cursor  SYS_REFCURSOR;
                    pTotReg number DEFAULT 100;
                    pTotPaginas number DEFAULT 100;
                    pPagina number DEFAULT 1;
                    pLineas number DEFAULT 100;
                    pDeuda varchar2(50) DEFAULT null;
                    pCLiente varchar2(50) DEFAULT null;
                    pNoCia varchar2(10) DEFAULT '01';
                    pNoGrupo varchar2(10) DEFAULT '01';
                    pNombre varchar2(50) DEFAULT null;
                    pTipo varchar2(50) DEFAULT null;
                    pEstatus varchar2(50) DEFAULT null;
                    output number DEFAULT 1000000;
                    v_no_fisico varchar2(50);
                    v_codigo_cliente varchar2(50);
                    v_nombre_cliente varchar2(50);
                    v_tipo_venta varchar2(50);
                    v_fecha_vencimiento varchar2(50);
                    v_monto_inicial varchar2(50);
                    v_monto_actual  varchar2(50);
                    v_monto_inicial_usd varchar2(50);
                    v_monto_actual_usd  varchar2(50);
                    v_fecha_ultimo_pago varchar2(50);
                    v_monto_ultimo_pago varchar2(50);
                    v_estatus_deuda varchar2(50);
                    v_codigo_tipo_doc varchar2(50);
                    v_nombre_tipo_doc varchar2(50);
                    v_cia varchar(2);
                    v_grupo varchar2(2);
                    v_tipo_cambio varchar2(50);
                    v_fecha_aviso varchar2(50) ;
                    v_docu_aviso varchar2(50);
                    v_serie_fisico varchar2(15);
                    v_fecha_documento varchar2(10);
                    v_aplica_corte varchar2(1);
                    v_fecha_entrega varchar2(10);
                    v_tot varchar2(50);
                    v_codigo_compani varchar2(10);
                    v_pagina varchar2(10);
                    v_linea varchar2(10);

                BEGIN
                    pTotReg  := {pTotReg};
                    pTotPaginas  := {pTotPaginas};
                    pPagina  := {pPagina};
                    pLineas  := {pLineas};
                    pDeuda := {pDeuda};
                    pNoCia := {pNoCia};
                    pNoGrupo := {pNoGrupo};
                    pCLiente := {pCLiente};
                    pNombre := {pNombre};
                    pTipo := {pTipo};
                    pEstatus := {pEstatus};

                    dbms_output.enable(output);

                    PROCESOSPW.deudas (l_cursor, pTotReg ,pTotPaginas, pPagina, pLineas, pNoCia, pNoGrupo, pCLiente);

                    -- PROCESOSPW.deudas (l_cursor, pTotReg ,pTotPaginas, pPagina, pLineas, pDeuda , pCLiente , pNombre, pTipo, pEstatus);

                    LOOP
                    FETCH l_cursor into
                    v_no_fisico,
                    v_codigo_cliente,
                    v_nombre_cliente,
                    v_tipo_venta,
                    v_fecha_vencimiento,
                    v_monto_inicial,
                    v_monto_actual,
                    v_monto_inicial_usd,
                    v_monto_actual_usd,
                    v_fecha_ultimo_pago,
                    v_monto_ultimo_pago,
                    v_estatus_deuda,
                    v_codigo_tipo_doc,
                    v_nombre_tipo_doc,
                    v_cia,
                    v_grupo,
                    v_tipo_cambio,
                    v_fecha_aviso,
                    v_docu_aviso,
                    v_serie_fisico,
                    v_fecha_documento,
                    v_aplica_corte,
                    v_fecha_entrega,
                    v_pagina,
                    v_linea;
                        EXIT WHEN l_cursor%NOTFOUND;
                    dbms_output.put_line
                        (

                        v_no_fisico|| '|'||
                        v_codigo_cliente|| '|'||
                        v_nombre_cliente|| '|'||
                        v_tipo_venta|| '|'||
                        v_fecha_vencimiento|| '|'||
                        v_monto_inicial|| '|'||
                        v_monto_actual|| '|'||
                        v_monto_inicial_usd|| '|'||
                        v_monto_actual_usd|| '|'||
                        v_fecha_ultimo_pago|| '|'||
                        v_monto_ultimo_pago|| '|'||
                        v_estatus_deuda|| '|'||
                        v_codigo_tipo_doc|| '|'||
                        v_nombre_tipo_doc|| '|'||
                        v_cia|| '|'||
                        v_grupo|| '|'||
                        v_tipo_cambio|| '|'||
                        v_fecha_aviso|| '|'||
                        v_docu_aviso|| '|'||
                        v_serie_fisico|| '|'||
                        v_fecha_documento|| '|'||
                        v_aplica_corte|| '|'||
                        v_fecha_entrega
                        );
                    END LOOP;
                CLOSE l_cursor;

                END;""".format(
                                    pTotReg = data['pTotReg'],
                                    pTotPaginas = data['pTotPaginas'],
                                    pPagina = data['pPagina'],
                                    pLineas = data['pLineas'],
                                    pDeuda = data['pDeuda'],
                                    pNoCia = data['pNoCia'],
                                    pNoGrupo = data['pNoGrupo'],
                                    pCLiente = data['pCLiente'],
                                    pNombre = data['pNombre'],
                                    pTipo = data['pTipo'],
                                    pEstatus = data['pEstatus']
                                )

    #print(sql)
    c.execute(sql)
    textVar = c.var(str)
    statusVar = c.var(int)
    list = []
    while True:
        c.callproc("dbms_output.get_line", (textVar, statusVar))
        if statusVar.getvalue() != 0:
            break
        arr = str(textVar.getvalue()).split("|")
        obj = {
            'no_fisico' :arr[0],
            'codigo_cliente' :arr[1],
            'nombre_cliente' :arr[2],
            'tipo_venta' :arr[3],
            'fecha_vencimiento' :arr[4],
            'monto_inicial' :formatFloatDdo(arr[5]),
            'monto_actual' :formatFloatDdo(arr[6]),
            'monto_inicial_usd' :formatFloatDdo(arr[7]),
            'monto_actual_usd' :formatFloatDdo(arr[8]),
            'fecha_ultimo_pago' :arr[9],
            'monto_ultimo_pago' :arr[10],
            'estatus_deuda' :arr[11],
            'codigo_tipo_doc' :arr[12],
            'nombre_tipo_doc' :arr[13],
            'cia' :arr[14],
            'grupo' :arr[15],
            'tipo_cambio' :arr[16],
            'fecha_aviso' :arr[17],
            'docu_aviso' :arr[18],
            'serie_fisico' :arr[19],
            'fecha_documento' :arr[20],
            'aplica_corte' :arr[21],
            'fecha_entrega' :arr[22]
        }
        list.append(obj)
    return response.json({"msj":"OK", "obj": list}, 200)

    # return response.json({"msj":"OK"}, 200)

@app.route('/procedure_productos', ["POST", "GET"])
async def procedure(request):

    data = request.json

    #print(data)

    if not 'pTotReg' in data or data['pTotReg'] == 0 :
        data['pTotReg'] = 100

    if not 'pTotPaginas' in data or data['pTotPaginas'] == 0 :
        data['pTotPaginas'] = 100

    if not 'pPagina' in data or data['pPagina'] == 0 :
        data['pPagina'] = 1

    if not 'pLineas' in data or data['pLineas'] == 0 :
        data['pLineas'] = 100

    if not 'pNoCia' in data :
        return response.json({"msg": "Missing username parameter"}, status=400)
    else:
        data['pNoCia'] = "'"+data['pNoCia']+"'"

    if not 'pNoGrupo' in data :
        return response.json({"msg": "Missing username parameter"}, status=400)
    else:
        data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"

    if not 'pCliente' in data :
        data['pCliente'] = 'null'
    else:
        data['pCliente'] = "'"+data['pCliente']+"'"

    if not 'pBusqueda' in data or data['pBusqueda'] == None :
        data['pBusqueda'] = 'null'
    else:
        data['pBusqueda'] = "'"+data['pBusqueda']+"'"

    if not 'pComponente' in data :
        data['pComponente'] = 'null'
    else:
        data['pComponente'] = "'"+data['pComponente']+"'"

    if not 'pArticulo' in data :
        data['pArticulo'] = 'null'
        data['haveArt'] = '--'
    else:
        data['haveArt'] = ""

    if not 'pCodProveedor' in data  or data['pCodProveedor'] == None :
        data['pCodProveedor'] = 'null'
        data['havePro'] = '--'
    else:
        data['havePro'] = ""

    if not 'pFiltroCategoria' in data or data['pFiltroCategoria'] == None :
        data['pFiltroCategoria'] = 'null'
        data['haveCat'] = '--'
    else:
        data['haveCat'] = ""

    if not 'pExistencia' in data or data['pExistencia'] == None :
        data['pExistencia'] = 'null'


    #print(data)
    db = get_db()
    c = db.cursor()
    # print(data)
    c.callproc("dbms_output.enable")

    sql = """

            DECLARE
            l_cursor  SYS_REFCURSOR;
            pTotReg number DEFAULT 10000;
            pTotPaginas number DEFAULT 1000;
            pPagina number DEFAULT null;
            pLineas number DEFAULT 10;
            pNoCia varchar2(10) DEFAULT '01';
            pNoGrupo varchar2(10) DEFAULT '01';
            pCliente varchar2(50) DEFAULT null;
            pBusqueda varchar2(50) DEFAULT null;
            pComponente varchar2(50) DEFAULT null;
            pArticulo varchar2(50) default null;
            pCodProveedor varchar2(15 )DEFAULT null;
            pFiltroCategoria varchar2(50) DEFAULT null;
            pExistencia number DEFAULT null;

            output number DEFAULT 1000000;

                v_cod_producto varchar2(100);
                v_nombre_producto varchar2(100);
                v_princ_activo varchar2(100);
                v_ind_regulado varchar2(100);
                v_ind_impuesto varchar2(100);
                v_ind_psicotropico varchar2(100);
                v_fecha_vence varchar2(100);
                v_existencia number;
                v_precio_bruto_bs number;
                v_precio_neto_bs number;
                v_iva_bs number;
                v_precio_usd varchar2(10);
                v_iva_usd varchar2(10);
                v_tipo_cambio number;
                v_proveedor varchar2(100);
                v_bodega varchar2(2);
                v_categoria varchar2(30);
                v_descuento1 number;
                v_descuento2 number;
                v_tipo_prod_emp varchar2(20);
                v_disp_prod_emp varchar2(1);
                v_dir_imagen varchar2(300);
                v_division varchar2(30);
                V_PAGINA number;
                V_LINEA number;
            BEGIN

                                pTotReg  := {pTotReg};
                                pTotPaginas  := {pTotPaginas};
                                -- pPagina  := {pPagina};
                                pLineas  := {pLineas};
                                pNoCia := {pNoCia};
                                pNoGrupo := {pNoGrupo};
                                pCliente := {pCliente};
                                pBusqueda := {pBusqueda};
                                pComponente := {pComponente};
                                pExistencia :={pExistencia};
            {haveArt}                    pArticulo := \'{pArticulo}\';
            {havePro}                    pCodProveedor := \'{pCodProveedor}\';
            {haveCat}                    pFiltroCategoria := \'{pFiltroCategoria}\' ;



                dbms_output.enable(output);

                PROCESOSPW.productos (l_cursor, pTotReg ,pTotPaginas, pPagina, pLineas, pNoCia, pNoGrupo,pCliente,pBusqueda,pComponente, pArticulo, pFiltroCategoria, pCodProveedor, pExistencia);

            LOOP
                FETCH l_cursor into
                v_cod_producto,
                v_nombre_producto,
                v_princ_activo,
                v_ind_regulado,
                v_ind_impuesto,
                v_ind_psicotropico,
                v_fecha_vence,
                v_existencia,
                v_precio_bruto_bs,
                v_precio_neto_bs,
                v_iva_bs,
                v_precio_usd,
                v_iva_usd,
                v_tipo_cambio,
                v_proveedor,
                v_bodega,
                v_categoria,
                v_descuento1,
                v_descuento2,
                v_tipo_prod_emp,
                v_disp_prod_emp,
                v_dir_imagen,
                v_division,
                V_PAGINA,
                V_LINEA;
                EXIT WHEN l_cursor%NOTFOUND;
                dbms_output.put_line
                (
                    v_cod_producto|| '|'||
                    v_nombre_producto|| '|'||
                    v_princ_activo|| '|'||
                    v_ind_regulado|| '|'||
                    v_ind_impuesto|| '|'||
                    v_ind_psicotropico|| '|'||
                    v_fecha_vence|| '|'||
                    v_existencia|| '|'||
                    v_precio_bruto_bs|| '|'||
                    v_precio_neto_bs|| '|'||
                    v_iva_bs|| '|'||
                    v_precio_usd|| '|'||
                    v_iva_usd|| '|'||
                    v_tipo_cambio|| '|'||
                    v_proveedor|| '|'||
                    v_bodega|| '|'||
                    v_categoria|| '|'||
                    v_descuento1|| '|'||
                    v_descuento2|| '|'||
                    v_tipo_prod_emp|| '|'||
                    v_disp_prod_emp|| '|'||
                    v_dir_imagen|| '|'||
                    v_division|| '|'||
                    V_PAGINA|| '|'||
                    V_LINEA
                );
            END LOOP;
            CLOSE l_cursor;

            END;

                """.format(
                        pTotReg = data['pTotReg'],
                        pTotPaginas = data['pTotPaginas'],
                        pPagina = data['pPagina'],
                        pLineas = data['pLineas'],
                        pNoCia = data['pNoCia'],
                        pNoGrupo = data['pNoGrupo'],
                        pCliente = data['pCliente'],
                        pBusqueda = data['pBusqueda'],
                        pComponente = data['pComponente'],
                        pArticulo = data['pArticulo'],
                        haveArt = data['haveArt'],
                        pFiltroCategoria = data['pFiltroCategoria'],
                        havePro = data['havePro'],
                        pCodProveedor = data['pCodProveedor'],
                        haveCat = data['haveCat'],
                        pExistencia= data['pExistencia']
                    )
    print(sql)
    c.execute(sql)
    textVar = c.var(str)
    statusVar = c.var(int)
    list = []
    while True:
        c.callproc("dbms_output.get_line", (textVar, statusVar))
        if statusVar.getvalue() != 0:
            break
        arr = str(textVar.getvalue()).split("|")
        obj = {
            'cod_producto' : arr[0],
            'nombre_producto' : arr[1],
            'princ_activo' : arr[2],
            'ind_regulado' : arr[3],
            'ind_impuesto' : arr[4],
            'ind_psicotropico' : arr[5],
            'fecha_vence' : arr[6],
            'existencia' : arr[7],
            'precio_bruto_bs' : formatFloatDdo(arr[8]),
            'precio_neto_bs' : formatFloatDdo(arr[9]),
            'iva_bs' : formatFloatDdo(arr[10]),
            'precio_neto_usd' : formatFloatDdo(arr[11]),
            'iva_usd' : formatFloatDdo(arr[12]),
            'tipo_cambio' : arr[13],
            'proveedor' :arr[14],
            'bodega' :arr[15],
            'categoria': arr[16],
            'descuento1' : arr[17],
            'descuento2' : arr[18],
            'tipo_prod_emp' : arr[19],
            'disp_prod_emp' :arr[20],
            'dir_imagen' : arr[21],
            'division' : arr[22],
            'pagina': arr[23],
            'linea': arr[24]
        }

        # if data['pArticulo'] == 'null'  :
        #     if int(arr[7]) > 0 :
        #         list.append(obj)
        # else:
        list.append(obj)
    return response.json({ "msg":"OK", "obj": list }, 200)

def agrupar_facturas(arreglo):

        list = {}
        for row in arreglo:
            if not row["nro_factura"] in list :
                list[int(row["nro_factura"])]=[]

        for row in arreglo:
            list[int(row["nro_factura"])].append(row)

        return list

@app.route('/procedure_facturacion', ["POST", "GET"])
async def procedure(request):

    data = request.json
    #print(data)
    if not 'pTotReg' in data or data['pTotReg'] == 0 :
        data['pTotReg'] = 100

    if not 'pTotPaginas' in data or data['pTotPaginas'] == 0 :
        data['pTotPaginas'] = 100

    if not 'pPagina' in data or data['pPagina'] == 0 :
        data['pPagina'] = 1

    if not 'pLineas' in data or data['pLineas'] == 0 :
        data['pLineas'] = 100

    if not 'pDeuda' in data :
        data['pDeuda'] = 'null'
    else:
        data['pDeuda'] = "'"+data['pDeuda']+"'"

    if not 'pNoCia' in data :
        data['pNoCia'] = 'null'
    # else:
    #     data['pNoCia'] = "'"+data['pNoCia']+"'"

    if not 'pNoCia' in data :
        data['pNoCia'] = '01'
    else:
        data['pNoCia'] = "'"+data['pNoCia']+"'"

    if not 'pNoGrupo' in data :
        data['pNoGrupo'] = '01'
    else:
        data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"

    if not 'pCliente' in data :
        data['pCliente'] = 'null'
    else:
        data['pCliente'] = "'"+data['pCliente']+"'"

    if not 'pNombre' in data :
        data['pNombre'] = 'null'
    else:
        data['pNombre'] = "'"+data['pNombre']+"'"

    if not 'pFechaFactura' in data :
        data['pFechaFactura'] = 'null'

    if not 'pFechaPedido' in data :
        data['pFechaPedido'] = 'null'


    #print(data)
    db = get_db()
    c = db.cursor()
    c.callproc("dbms_output.enable")
    sql = """DECLARE

      l_cursor  SYS_REFCURSOR;

                pTotReg number DEFAULT 100;
                pTotPaginas number DEFAULT 100;
                pPagina number DEFAULT 1;
                pLineas number DEFAULT 100;
                pDeuda number DEFAULT null;
                pNoCia varchar2(10) DEFAULT '01';
                pNoGrupo varchar2(10) DEFAULT '01';
                pCliente varchar2(50) DEFAULT null;
                pPedido varchar2(50) DEFAULT null;
                output number DEFAULT 1000000;
                pFechaFactura date;
                pFechaPedido date;

                v_nro_factura varchar2(50);
                v_fecha_factura date;
                v_cod_cliente varchar2(50);
                v_cod_vendedor varchar2(50);
                v_nombre_vendedor varchar2(150);
                v_email_vendedor varchar2(90);
                v_no_linea number;
                v_no_arti varchar2(50);
                v_nombre_arti varchar2(150);
                v_unidades_pedido number;
                v_unidades_facturadas number;
                v_total_producto_bs varchar(20);
                v_total_producto_usd varchar(20);
                v_cia          varchar2(2);
                v_grupo        varchar2(2);
                v_tipo_pedido  varchar2(15);
                v_fecha_entrega date;

                v_pag        number;
                v_lin        number;
                v_totreg     number;
                v_totpag     number;
                v_tot number:=0;

    BEGIN

            pTotReg  := {pTotReg};
            pTotPaginas  := {pTotPaginas};
            pPagina  := {pPagina};
            pLineas  := {pLineas};
            pDeuda := {pDeuda};
            pNoCia := {pNoCia};
            pNoGrupo := {pNoGrupo};
            pCliente := {pCliente};
            pFechaFactura := {pFechaFactura};
            pFechaPedido := {pFechaPedido};

            dbms_output.enable(output);

            procesospw.pedidos_facturados (l_cursor,pTotReg /*pTotReg*/,pTotPaginas /*pTotPaginas*/,NULL /*pPagina*/,100 /*pLineas*/, null /*pDeuda*/,pNoCia /*pCia*/,pNoGrupo /*pGrupo*/,pCliente /*pCliente*/,
                                         null/*FechaFactura*/);


      LOOP
        FETCH l_cursor into
                v_nro_factura,
                v_fecha_factura,
                v_cod_cliente,
                v_cod_vendedor,
                v_nombre_vendedor,
                v_email_vendedor,
                v_no_linea,
                v_no_arti,
                v_nombre_arti,
                v_unidades_pedido,
                v_unidades_facturadas,
                v_total_producto_bs,
                v_total_producto_usd,
                v_cia,
                v_grupo,
                v_tipo_pedido,
                v_fecha_entrega,
                v_pag,
                v_lin;
        EXIT WHEN l_cursor%NOTFOUND;
        dbms_output.put_line
          (
                v_nro_factura|| '|'||
                v_fecha_factura|| '|'||
                v_cod_cliente || '|'||
                v_cod_vendedor|| '|'||
                v_nombre_vendedor|| '|'||
                v_email_vendedor|| '|'||
                v_no_linea|| '|'||
                v_no_arti|| '|'||
                v_nombre_arti|| '|'||
                v_unidades_pedido|| '|'||
                v_unidades_facturadas|| '|'||
                v_total_producto_bs || '|'||
                v_total_producto_usd || '|'||
                v_cia || '|'||
                v_grupo || '|'||
                v_tipo_pedido || '|'||
                v_fecha_entrega || '|'||
                v_pag|| '|'||
                v_lin
          );

      END LOOP;
         --v_tot:=l_cursor%rowcount;
        -- dbms_output.put_line(v_tot || '|'|| v_totreg || '|'|| v_totpag );
      CLOSE l_cursor;

    END;
""".format(
                        pTotReg = data['pTotReg'],
                        pTotPaginas = data['pTotPaginas'],
                        pPagina = data['pPagina'],
                        pLineas = data['pLineas'],
                        pDeuda = data['pDeuda'],
                        pNoCia = data['pNoCia'],
                        pNoGrupo = data['pNoGrupo'],
                        pCliente = data['pCliente'],
                        pNombre = data['pNombre'],
                        pFechaFactura = data['pFechaFactura'],
                        pFechaPedido = data['pFechaPedido']


                    )

    #print(sql)
    c.execute(sql)
    textVar = c.var(str)
    statusVar = c.var(int)
    list = []
    fecha = ''
    while True:
        c.callproc("dbms_output.get_line", (textVar, statusVar))
        if statusVar.getvalue() != 0:
            break
        arr = str(textVar.getvalue()).split("|")
        fecha = arr[1]
        obj = {

                'nro_factura': arr[0],
                'fecha_factura': arr[1],
                'cod_cliente': arr[2],
                'cod_vendedor': arr[3],
                'nombre_vendedor': arr[4],
                'email_vendedor': arr[5],
                'no_linea': arr[6],
                'no_arti': arr[7],
                'nombre_arti': arr[8],
                'unidades_pedido': arr[9],
                'unidades_facturadas': arr[10],
                'total_producto' :  formatFloatDdo(arr[11]) ,
                'total_producto_usd': formatFloatDdo(arr[12]),
                'codigo_compani': arr[13],
                'grupo': arr[14],
                'tipo_pedido': arr[15],
                'fecha_entrega': arr[16]
                # 'linea': arr[17]
            }
        list.append(obj)
    # vale = datetime.strptime(arr[1],"%d/%m/%y").strftime("%d/%m/%Y")
    # print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++fecha+++++++++++++++++++++++++++++++++++++")
    # print(vale)

    return response.json({"msj": "OK", "obj": agrupar_facturas(list)}, 200)


@app.route('/valida/client', ["POST", "GET"])
@jwt_required
async def valida_client(request, token : Token):
    try:
        data = request.json

        if not 'pNoCia' in data :
            return response.json({"msg": "Missing password parameter cia"}, status=400)
        # else:
        #     data['pNoCia'] = "'"+data['pNoCia']+"'"

        if not 'pNoGrupo' in data :
            return response.json({"msg": "Missing password parameter grupo"}, status=400)
        # else:
        #     data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"

        if not 'pCliente' in data :
            return response.json({"msg": "Missing password parameter cliente"}, status=400)
        # else:
        #     data['pCliente'] = "'"+data['pCliente']+"'"

        if not 'pMoneda' in data :
                data['pMoneda'] = '\'P\''
        # else:
        #     data['pMoneda'] = "'"+data['pMoneda']+"'"

        db = get_db()
        c = db.cursor()
        sql = """select t2.DESCRIPCION
                        from dual
                    join TIPO_ERROR t2 on PAGINAWEB.PROCESOSPW.valida_cliente(\'{pNoCia}\',\'{pNoGrupo}\',\'{pCliente}\',{pMoneda},0) = t2.CODIGO""".format(
                    pNoCia = data['pNoCia'],
                    pNoGrupo = data['pNoGrupo'],
                    pCliente = data['pCliente'],
                    pMoneda = data['pMoneda']
                    )
        print(sql)
        c.execute(sql)
        row = c.fetchone()
        # print("==============================================================row")
        # print(row)
        if row == None:
            return response.json({"msg":"success"},200)

        return response.json({"data":row},450)

    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

async def crear_pedido(request):
    try:
        data = request.json

        db = get_db()
        c = db.cursor()

        sql = """SELECT
                COUNT(ID)
                FROM PAGINAWEB.PEDIDO WHERE COD_CLIENTE = :COD_CLIENTE and ESTATUS in(0,1,2)"""
        c.execute(sql, [data['COD_CLIENTE']])
        count = c.fetchone()
        #print(count)

        #print("========================================================================")
        #print("ejecuto el count")
        if int(count[0]) > 0 :
            # return response.json({"msg": "Cliente con pedidos abiertos"}, status=400)
            raise Exception("Cliente con pedidos abiertos")

        c.callproc("dbms_output.enable")
        sql = """
                declare
                    s2 number;

                begin

                    INSERT INTO PEDIDO ( COD_CIA, GRUPO_CLIENTE,
                                            COD_CLIENTE,  NO_PEDIDO_CODISA,
                                            OBSERVACIONES, ESTATUS, TIPO_PEDIDO) VALUES
                            (  :COD_CIA, :GRUPO_CLIENTE, :COD_CLIENTE, :NO_PEDIDO_CODISA, :OBSERVACIONES, :ESTATUS, 'N'  )
                             returning ID into s2;
                    dbms_output.put_line(s2);
                end;
            """

        c.execute(sql, [
                        data['COD_CIA'],
                        data['GRUPO_CLIENTE'],
                        data['COD_CLIENTE'],
                        data['NO_PEDIDO_CODISA'],
                        data['OBSERVACIONES'],
                        0
                    ]
                )
        #print("========================================================================")
        #print("ejecuto el query")
        statusVar = c.var(cx_Oracle.NUMBER)
        lineVar = c.var(cx_Oracle.STRING)
        ID = None
        while True:
          c.callproc("dbms_output.get_line", (lineVar, statusVar))
          if lineVar.getvalue() == None:
              break
          #print("==========================================================")
          #print(lineVar.getvalue())
          ID = lineVar.getvalue()

          if statusVar.getvalue() != 0:
            break
        db.commit()
        return ID
    except Exception as e:
        logger.debug(e)

async def update_detalle_pedido(detalle, ID,pCia, pGrupo ,pCliente):
    try:
            print("====================update_detalle_pedido=====================")
            db = get_db()
            c = db.cursor()
            #print(detalle)
            c.execute("""UPDATE PAGINAWEB.DETALLE_PEDIDO
                            SET
                                   CANTIDAD     = :CANTIDAD,
                                   PRECIO_BRUTO = :PRECIO_BRUTO
                            WHERE  ID_PEDIDO    = :ID_PEDIDO
                            AND    COD_PRODUCTO = :COD_PRODUCTO""",
                            [
                                int(0),
                                0,
                                ID,
                                detalle['COD_PRODUCTO']
                            ])
            db.commit()

            print("actualizo con cero")

            cantidad = 0
            respuesta = await valida_art(pCia, detalle['COD_PRODUCTO'],pGrupo,pCliente,detalle['CANTIDAD'],float(str(detalle['precio_bruto_bs']).replace(',','.')),int(ID))
            print("paso valida art")
            if respuesta != 1 :
                return respuesta
            # print("sigue")
            disponible = detalle['CANTIDAD']

            c.execute("""UPDATE PAGINAWEB.DETALLE_PEDIDO
                            SET
                                   CANTIDAD     = :CANTIDAD,
                                   PRECIO_BRUTO = :PRECIO_BRUTO
                            WHERE  ID_PEDIDO    = :ID_PEDIDO
                            AND    COD_PRODUCTO = :COD_PRODUCTO""",
                            [
                                int(disponible),
                                float(str(detalle['precio_bruto_bs']).replace(',','.')),
                                ID,
                                detalle['COD_PRODUCTO']
                            ])
            db.commit()
            print("ejecuto todo")
            return disponible

    except Exception as e:
        logger.debug(e)

@app.route('/upd/detalle_producto',["POST","GET"])
@jwt_required
async def upd_detalle_producto_serv (request, token: Token):
# async def procedure(request):
    try:
        data = request.json

        pedidoValido = await validate_Pedido(data['ID'])

        if not pedidoValido :
            return response.json({"msg": "NO PUEDE EDITAR ESTE PEDIDO" }, status=410)

        print(data)
        reservado = await update_detalle_pedido(data['pedido'], data['ID'], data['pNoCia'], data['pNoGrupo'] , data['pCliente'])
        print("======================reservado===================")
        if isinstance(reservado, str) :
            return response.json({"msg": respuesta  },480)

        msg = 0


        totales = await totales_pedido(int(data['ID']))

        await upd_estatus_pedido(6,data['ID'])

        # TODO:

        # if data['pedido']['CANTIDAD'] > reservado:
        #     msg = 1

        return response.json({"msg": msg, "reserved":reservado, "totales":totales },200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

async def crear_detalle_pedido(detalle, ID,pCia, pGrupo ,pCliente):

        try:

            cantidad = 0

            disponible = await existencia_disponible(pCia, detalle['COD_PRODUCTO'],detalle['CANTIDAD'] )
            print(">>>>>>>>>>>>>>>>>>>>>>>respuesta creas<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
            print(disponible)
            if disponible == -1:
                return "No se pudo completar por favor verifique la disponibilidad del producto"

            respuesta = await valida_art(pCia, detalle['COD_PRODUCTO'],pGrupo,pCliente,disponible,float(str(detalle['precio_bruto_bs']).replace(',','.')),int(ID))
            
            
            # print(respuesta)
            if respuesta != 1 :
                return respuesta
            # print("sigue")
            # disponible = detalle['CANTIDAD']

            # if int(detalle['CANTIDAD']) > disponible :
            #     cantidad = disponible
            # else:
            #     cantidad = detalle['CANTIDAD']

            db = get_db()
            c = db.cursor()

            sql = """INSERT INTO DETALLE_PEDIDO ( ID_PEDIDO, COD_PRODUCTO, CANTIDAD, PRECIO_BRUTO, TIPO_CAMBIO, BODEGA)
                            VALUES ( {ID_PEDIDO}, \'{COD_PRODUCTO}\' ,  {CANTIDAD} ,  {PRECIO} , {TIPO_CAMBIO}, \'{BODEGA}\' )""".format(
                                         ID_PEDIDO = int(ID),
                                         COD_PRODUCTO = str(detalle['COD_PRODUCTO']),
                                         CANTIDAD = int(disponible),
                                         PRECIO = float(str(detalle['precio_bruto_bs']).replace(',','.')),
                                         TIPO_CAMBIO = float(str(detalle['tipo_cambio']).replace(',','.')) ,
                                         BODEGA = detalle['bodega']
                                    )

            c.execute(sql)

            db.commit()

            return disponible

        except Exception as e:
            logger.debug(e)

async def upd_estatus_pedido(estatus, ID):
        print("upd_estatus")
        db = get_db()
        c = db.cursor()

        sql = """
                    UPDATE PAGINAWEB.PEDIDO
                    SET
                        ESTATUS          = {ESTATUS}
                    WHERE  ID               = {ID}

            """.format(  ESTATUS = estatus,ID = int(ID))
        print(sql)
        c.execute(sql)
        print("ejeuto query")
        db.commit()
        #print("============================ejecuto======================")
        sql = """select descripcion
                        from ESTATUS where codigo = :estatus"""
        c.execute(sql, [estatus])
        row = c.fetchone()
        return row[0]

async def upd_tipo_pedido( ID, tipoPedido = "N"):
        print("upd_tipo_pedido")
        db = get_db()
        c = db.cursor()

        sql = """
                    UPDATE PAGINAWEB.PEDIDO
                    SET
                        TIPO_PEDIDO      = :TIPO_PEDIDO
                    WHERE  ID               = :ID

            """

        c.execute(sql, [tipoPedido,ID])
        print("ejecuto query")

        db.commit()

        return


async def validate_Pedido( ID ):

    try:

        db = get_db()
        c = db.cursor()

        sql = """
                    SELECT ESTATUS FROM PAGINAWEB.PEDIDO
                    WHERE  ID  = :ID

            """

        c.execute(sql, [ID])

        row = c.fetchone()

        db.commit()

        if row[0] < 2 or row[0] == 6  :
            return True
        else:
            return False

    except Exception as e:
        logger.debug(e)

async def tiempo_resta_pedido(pIdPedido):
    try:

        db = get_db()
        c = db.cursor()
        respuesta = None

        sql = """SELECT PROCESOSPW.tiempo_resta_pedido ({pIdPedido}) from dual""".format(
                pIdPedido=pIdPedido
        )
        # print(sql)
        c.execute(sql)

        row = c.fetchone()
        print("RESUTADO tiempo")
        if row != None and row[0] != None:
            print(row[0])

        return row[0]

    except Exception as e:
        logger.debug(e)
@app.route('/tiempo_resta_pedido/articulo', ["POST", "GET"])
@jwt_required
async def tiempo_resta_pedido(request, token : Token):
    try:
        data = request.json
        db = get_db()
        c = db.cursor()

        sql = """SELECT PROCESOSPW.tiempo_resta_pedido ({pIdPedido}) from dual""".format(
                pIdPedido=data['pIdPedido']
        )
        # print(sql)
        c.execute(sql)

        row = c.fetchone()
        print("RESUTADO tiempo")

        if row != None and row[0] != None:
            return response.json({"time":row[0]},200)

        return response.json({"time":row[0]},407)

    except Exception as e:
        logger.debug(e)


async def valida_art(pCia, pNoArti,pGrupo,pCliente,pCantidad,pPrecio,pIdPedido):
    try:

        db = get_db()
        c = db.cursor()
        respuesta = None

        sql = """SELECT t1.DESCRIPCION FROM PAGINAWEB.TIPO_ERROR t1
              WHERE t1.CODIGO = PROCESOSPW.valida_articulo (\'{pCia}\',\'{pGrupo}\',\'{pCliente}\',\'{pNoArti}\',{pCantidad},{pPrecio},'P',{pIdPedido})""".format(
                pCia=pCia,
                pGrupo=pGrupo,
                pCliente=pCliente,
                pNoArti=pNoArti,
                pCantidad=pCantidad,
                pPrecio=pPrecio,
                pIdPedido=pIdPedido
        )
        # print(sql)
        c.execute(sql)

        row = c.fetchone()
        print("RESUTADO VALIDA ARTICULO")
        if row != None and row[0] != None:
            print(row[0])
            return row[0]

        return 1
    except Exception as e:
        logger.debug(e)

async def existencia_disponible(pCia, pNoArti, pCantidad ):
    try:

        db = get_db()
        c = db.cursor()
        respuesta = None
        disponible = 0
        sql = """SELECT PROCESOSPW.existencia_disponible (\'{pCia}\',\'{pNoArti}\') from dual""".format(
                pCia=pCia,
                pNoArti=pNoArti,

        )
        c.execute(sql)

        row = c.fetchone()
        
        print("RESUTADO existencia_disponible")
        print(row[0])
        if row != None and row[0] != None and row[0] > 0:
            if int(pCantidad) > int(row[0])  :
                return row[0]
            else:
                return int(pCantidad)

        return -1
    except Exception as e:
        logger.debug(e)

@app.route('/valida/articulo', ["POST", "GET"])
@jwt_required
async def valida_articulo(request, token : Token):
    try:
        data = request.json

        if not 'articulo' in data:
            return response.json({"msg": "Missing username parameter"}, status=480)

        articulo = data['articulo']

        respuesta = await valida_art(data['pNoCia'], articulo['COD_PRODUCTO'],data['pNoGrupo'],data['pCliente'],articulo['CANTIDAD'],float(str(articulo['precio_bruto_bs']).replace(',','.')),int(data['idPedido']))

        if respuesta != 1:
            return response.json({"msg": respuesta}, status=480)

        return response.json({"data":row},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

@app.route('/finalizar_pedido', ["POST", "GET"])
@jwt_required
async def finaliza_pedido(request, token : Token):
    try:
        data = request.json

        await upd_tipo_pedido(data['ID'], data['tipoPedido'] )
        await upd_estatus_pedido(2,data['ID'])

        return response.json("success",200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

@app.route('/editar_pedido', ["POST", "GET"])
@jwt_required
async def editar_pedido(request, token : Token):
    try:
        data = request.json

        estatus = await upd_estatus_pedido(6,data['ID'])

        return response.json({"estatus" : estatus},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

@app.route('/posponer_pedido', ["POST", "GET"])
@jwt_required
async def editar_pedido(request, token : Token):
    try:
        data = request.json

        estatus = await upd_estatus_pedido(data['estatus'],data['ID'])

        return response.json({"estatus" : estatus},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

@app.route('/cancel_pedido', ["POST", "GET"])
@jwt_required
async def editar_pedido(request, token : Token):
    try:
        data = request.json

        estatus = await upd_estatus_pedido(5,data['ID'])

        return response.json({"estatus" : estatus},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)


@app.route('/add/pedido',["POST","GET"])
@jwt_required
async def add_pedido (request, token: Token):
# async def procedure(request):
    try:
        data = request.json

        ID = await crear_pedido(request)

        iva_list = []

        for pedido in data['pedido']:

            row = await crear_detalle_pedido(pedido, ID)
            iva_list.append(row)


        mongodb = get_mongo_db()
        totales = dict(
            id_pedido = int(ID),
            productos = iva_list
        )

        await mongodb.order.insert_one(totales)

        return response.json("SUCCESS",200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

@app.route('/add/pedidoV2',["POST","GET"])
@jwt_required
async def add_pedidoV2 (request, token: Token):
# async def procedure(request):
    try:
        data = request.json

        ID = await crear_pedido(request)

        # await mongodb.order.insert_one(totales)
        if ID == None :
            response.json("ERROR",400)

        await logAudit(data['username'], 'pedido', 'add', ID)
        return response.json({"ID":ID},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

@app.route('/add/detalle_producto',["POST","GET"])
@jwt_required
async def add_detalle_producto (request, token: Token):
# async def procedure(request):
    try:
        data = request.json
        pedidoValido = False

        pedidoValido = await validate_Pedido(data['ID'])

        if not pedidoValido :
            return response.json({"msg": "NO PUEDE EDITAR ESTE PEDIDO" }, status=410)


        respuesta = await crear_detalle_pedido(data['pedido'], data['ID'], data['pNoCia'], data['pNoGrupo'], data['pCliente'])
        # print("=============respuesta=================")
        # print(respuesta)
        if isinstance(respuesta, str) :
            return response.json({"msg": respuesta  },480)

        totales = await totales_pedido(int(data['ID']))

        msg = 0

        if data['pedido']['CANTIDAD'] < respuesta:
            msg = 1

        await upd_estatus_pedido(6,data['ID'])

        await logAudit(data['username'], 'pedido', 'upd', int(data['ID']))

        return response.json({"msg": msg, "reserved":respuesta, "totales": totales },200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

@app.route('/del/detalle_producto',["POST","GET"])
@jwt_required
async def del_detalle_producto (request, token: Token):
# async def procedure(request):
    try:
        data = request.json

        pedidoValido = await validate_Pedido(data['id_pedido'])

        if not pedidoValido :
            return response.json({"msg": "NO PUEDE EDITAR ESTE PEDIDO" }, status=410)

        await upd_estatus_pedido(6,data['id_pedido'])

        db = get_db()
        c = db.cursor()

        c.execute("""DELETE FROM DETALLE_PEDIDO WHERE ID_PEDIDO = :ID AND COD_PRODUCTO = :COD_PRODUCTO""",
            [
                data['id_pedido'],
                data['COD_PRODUCTO']
            ])
        db.commit()

        totales = await totales_pedido(int(data['id_pedido']))

        await logAudit(data['username'], 'pedido', 'del', int(data['id_pedido']))

        return response.json({"totales":totales},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)


@app.route('/del/pedido',["POST","GET"])
@jwt_required
async def update_pedido (request, token: Token):
# async def procedure(request):
    try:
        data = request.json
        #print(data)

        db = get_db()
        c = db.cursor()

        c.execute("""DELETE FROM DETALLE_PEDIDO WHERE ID_PEDIDO = :ID""",[data['ID']])

        c.execute("""DELETE FROM PEDIDO WHERE ID = :ID""",[data['ID']])

        db.commit()


        return response.json("SUCCESS",200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

@app.route('/get/pedidos',["POST","GET"])
@jwt_required
async def pedidos (request , token: Token):
    try:
        data = request.json

        if not 'pCliente' in data :
            data['pCliente'] = 'null'
            data['filter'] = '--'
        else:
            data['pCliente'] = "'"+data['pCliente']+"'"
            data['filter'] = ''

        db = get_db()
        c = db.cursor()
        c.execute("""SELECT COD_CIA, GRUPO_CLIENTE,
                            COD_CLIENTE, TO_CHAR(FECHA_CARGA, 'DD-MM-YYYY'), NO_PEDIDO_CODISA,
                            OBSERVACIONES,  t2.descripcion, (sum(t3.PRECIO_BRUTO * t3.CANTIDAD ))
                                monto, count(t3.COD_PRODUCTO) producto,ID, t1.ESTATUS, TO_CHAR(FECHA_ESTATUS, 'DD-MM-YYYY')
                            FROM PAGINAWEB.PEDIDO t1
                            join PAGINAWEB.ESTATUS t2
                                on t1.ESTATUS = t2.CODIGO
                            left join PAGINAWEB.DETALLE_PEDIDO t3
                                on t1.ID = t3.ID_PEDIDO
                            {filter} WHERE COD_CLIENTE = {pCliente}
                             GROUP BY ID, COD_CIA, GRUPO_CLIENTE,
                                   COD_CLIENTE, FECHA_CARGA, NO_PEDIDO_CODISA,
                                   OBSERVACIONES,  t2.descripcion,  t1.ESTATUS, FECHA_ESTATUS
                                 order by ID desc
                            """.format(filter = data['filter'], pCliente = data['pCliente'] ))
        list = []
        for row in c:
            aux = {}
            aux = {
                    'no_cia':row[0],
                    'grupo':row[1],
                    'no_cliente':row[2],
                    'fecha':row[3],
                    'no_factu':row[4],
                    # 'no_arti':row[4],
                    'observacion':row[5],
                    'estatus':row[6],
                    'precio':row[7],
                    'cantidad':row[8],
                    'ID':row[9],
                    'estatus_id':row[10],
                    'fecha_estatus':row[11]


              }
            list.append(aux)


        return response.json({"data":list},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

@app.route('/get/pedidosV2',["POST","GET"])
@jwt_required
async def pedidosV2 (request , token: Token):
    try:
        data = request.json

        if not 'pNoCia' in data :
            data['pNoCia'] = '01'
        else:
            data['pNoCia'] = "'"+data['pNoCia']+"'"

        if not 'pNoGrupo' in data :
            data['pNoGrupo'] = '01'
        else:
            data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"

        if not 'pCliente' in data :
            data['pCliente'] = 'null'
        else:
            data['pCliente'] = "'"+data['pCliente']+"'"


        list = await procedure_pedidos(data['pNoCia'],data['pNoGrupo'],data['pCliente'])



        return response.json({"data":list},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

async def procedure_detalle_pedidos(idPedido):
    try:

        db = get_db()
        c = db.cursor()
        c.callproc("dbms_output.enable")
        c.execute("""DECLARE

                        l_cursor  SYS_REFCURSOR;

                        v_id_pedido number;
                        v_cod_producto varchar2(15);
                        v_nombre_producto varchar2(80);
                        v_princ_activo varchar2(200);
                        v_unidades NUMBER;
                        v_precio_bruto_bs number;
                        v_precio_bruto_usd varchar2(10);
                        v_precio_neto_bs number;
                        v_iva_bs number;
                        v_precio_neto_usd varchar2(10);
                        v_iva_usd varchar2(10);
                        v_fecha_vence varchar2(10);
                        v_tipo_prod_emp varchar2(12);


                      BEGIN


                          PROCESOSPW.detalle_pedidos_cargados (l_cursor ,{idPedido});


                        LOOP

                          FETCH l_cursor into

                                  v_id_pedido ,
                                  v_cod_producto ,
                                  v_nombre_producto ,
                                  v_princ_activo ,
                                  v_unidades ,
                                  v_precio_bruto_bs ,
                                  v_precio_bruto_usd ,
                                  v_precio_neto_bs ,
                                  v_iva_bs ,
                                  v_precio_neto_usd ,
                                  v_iva_usd,
                                  v_fecha_vence,
                                  v_tipo_prod_emp;

                          EXIT WHEN l_cursor%NOTFOUND;

                          dbms_output.put_line

                            (


                                  v_id_pedido|| '|'||
                                  v_cod_producto|| '|'||
                                  v_nombre_producto|| '|'||
                                  v_princ_activo|| '|'||
                                  v_unidades|| '|'||
                                  v_precio_bruto_bs|| '|'||
                                  v_precio_bruto_usd|| '|'||
                                  v_precio_neto_bs|| '|'||
                                  v_iva_bs|| '|'||
                                  v_precio_neto_usd|| '|'||
                                  v_iva_usd|| '|'||
                                  v_fecha_vence|| '|'||
                                  v_tipo_prod_emp


                            );



                        END LOOP;


                        CLOSE l_cursor;


  END;""".format(idPedido = idPedido))
        textVar = c.var(str)
        statusVar = c.var(int)
        list = []
        while True:
            c.callproc("dbms_output.get_line", (textVar, statusVar))
            if statusVar.getvalue() != 0:
                break
            arr = str(textVar.getvalue()).split("|")
            obj = {
                  'id_pedido': arr[0],
                  'COD_PRODUCTO': arr[1],
                  'nombre_producto': arr[2],
                  'princ_activo': arr[3],
                  'CANTIDAD': arr[4],
                  'precio_bruto_bs' : formatFloatDdo(arr[5]),
                  'precio_bruto_usd' : formatFloatDdo(arr[6]),
                  'precio_neto_bs': formatFloatDdo(arr[7]),
                  'PRECIO': formatFloatDdo(arr[5]),
                  'iva_bs': formatFloatDdo(arr[8]),
                  'precio_neto_usd': formatFloatDdo(arr[9]),
                  'iva_usd': formatFloatDdo(arr[10]),
                  'fecha_vence': arr[11],
                  'tipo_prod_emp': arr[12]
            }
            list.append(obj)

        return list
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

async def procedure_pedidos(cia,grupo,cliente):
    try:

        db = get_db()
        c = db.cursor()
        c.callproc("dbms_output.enable")
        c.execute("""DECLARE

                        l_cursor  SYS_REFCURSOR;
                        v_id_pedido number;
                        v_nombre_cliente varchar2(40);
                        v_direccion_cliente varchar2(200);
                        v_fecha_creacion DATE;
                        v_cod_estatus number;
                        v_estatus varchar2(80);
                        v_fecha_estatus DATE;
                        v_tipo_pedido varchar2(15);
                        pNoCia varchar2(10) DEFAULT '01';
                        pNoGrupo varchar2(10) DEFAULT '01';
                        pCliente varchar2(50) DEFAULT null;

                      BEGIN

                          pNoCia := {pNoCia};
                          pNoGrupo := {pNoGrupo};
                          pCliente := {pCliente};

                          PROCESOSPW.pedidos_cargados (l_cursor ,pNoCia, pNoGrupo,pCliente);

                        LOOP

                          FETCH l_cursor into
                                  v_id_pedido,
                                  v_nombre_cliente,
                                  v_direccion_cliente,
                                  v_fecha_creacion,
                                  v_cod_estatus,
                                  v_estatus,
                                  v_fecha_estatus,
                                  v_tipo_pedido
                                  ;

                          EXIT WHEN l_cursor%NOTFOUND;

                          dbms_output.put_line

                            (
                                  v_id_pedido|| '|'||

                                  v_nombre_cliente|| '|'||

                                  v_direccion_cliente|| '|'||

                                  v_fecha_creacion|| '|'||

                                  v_cod_estatus|| '|'||

                                  v_estatus|| '|'||

                                  v_fecha_estatus|| '|'||

                                  v_tipo_pedido


                            );

                        END LOOP;


                        CLOSE l_cursor;


                      END;
        """.format(
                    pNoCia = cia,
                    pNoGrupo = grupo,
                    pCliente = cliente
                )
            )
        textVar = c.var(str)
        statusVar = c.var(int)
        list = []
        while True:
            c.callproc("dbms_output.get_line", (textVar, statusVar))
            if statusVar.getvalue() != 0:
                break
            arr = str(textVar.getvalue()).split("|")
            obj = {
                  'ID': arr[0],
                  'nombre_cliente': arr[1],
                  'direccion_cliente': arr[2],
                  'fecha_creacion': arr[3],
                  'cod_estatus': arr[4],
                  'estatus': arr[5],
                  'fecha_estatus': arr[6],
                  'tipo_pedido': arr[7]
            }
            list.append(obj)

        return list
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)

@app.route('/get/pedido',["POST","GET"])
@jwt_required
async def pedido (request , token: Token):
    try:
        data = request.json

        if not 'idPedido' in data or data['idPedido'] == 0 :
            return response.json({"msg": "Missing ID parameter"}, status=400)

        # mongodb = get_mongo_db()

        db = get_db()
        c = db.cursor()

        pedidos = await procedure_detalle_pedidos(int(data['idPedido']))
        totales = await totales_pedido(int(data['idPedido']))
        errores = await log_errores(int(data['idPedido']))
        c.execute("""SELECT
                         COD_CIA, GRUPO_CLIENTE,
                        COD_CLIENTE, TO_CHAR(FECHA_CARGA, 'DD-MM-YYYY'), NO_PEDIDO_CODISA,
                        OBSERVACIONES, t2.descripcion, ESTATUS, TIPO_PEDIDO
                        FROM PAGINAWEB.PEDIDO t1
                        join PAGINAWEB.ESTATUS t2
                            on t1.ESTATUS = t2.CODIGO
                        WHERE ID = {idPedido}
                            """.format( idPedido = data['idPedido'] ))
        list = []
        for row in c:
            aux = {}
            aux = {
                    'no_cia':row[0],
                    'grupo':row[1],
                    'no_cliente':row[2],
                    'fecha':row[3],
                    'no_factu':row[4],
                    'observacion':row[5],
                    'estatus':row[6],
                    'estatus_id':row[7],
                    'tipo_pedido':row[8],
                    'pedido': pedidos,
                    'errores':errores,
                    'totales':totales,
              }

            list.append(aux)

        return response.json({"msj": "OK", "obj": list}, 200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR",400)



async def log_errores(idPedido):
    try:

        db = get_db()
        c = db.cursor()

        c.execute("""SELECT
                         COD_PRODUCTO, TO_CHAR(FECHA, 'DD-MM-YYYY'),
                           t2.DESCRIPCION
                        FROM PAGINAWEB.REGISTRO_ERROR t1
                        JOIN TIPO_ERROR t2 on t1.COD_ERROR = t2.CODIGO
                        WHERE t1.ID_PEDIDO = {idPedido}
                        """.format( idPedido = idPedido ))
        list = []
        for row in c:
            aux = {}
            aux = {
                    'COD_PRODUCTO':row[0],
                    'FECHA':row[1],
                    'DESCRIPCION':row[2]
              }

            list.append(aux)

        return list
    except Exception as e:
        logger.debug(e)
        return e

@app.route('/get/categorias', ["POST", "GET"])
async def filtros(request):
    try:

        db = get_db()
        c = db.cursor()

        c.execute("""SELECT
                    CODIGO, NOMBRE
                    FROM PAGINAWEB.FILTRO_CATEGORIA_PRODUCTO
                        """)
        list = []
        for row in c:
            aux = {}
            aux = {
                    'CODIGO':row[0],
                    'NOMBRE':row[1]
              }

            list.append(aux)

        return response.json({"msj": "OK", "obj": list}, 200)
    except Exception as e:
        logger.debug(e)
        return e

@app.route('/get/proveedores', ["POST", "GET"])
async def procedure_prove(request):

    # data = request.json

    db = get_db()
    c = db.cursor()

    c.callproc("dbms_output.enable")
    c.execute("""
                DECLARE
                        l_cursor  SYS_REFCURSOR;
                        output number DEFAULT 1000000;

                        v_cod_proveedor varchar2(20);
                        v_nom_proveedor varchar2(50);
                    BEGIN


                        dbms_output.enable(output);
                        PROCESOSPW.proveedores (l_cursor);

                    LOOP
                    FETCH l_cursor into

                        v_cod_proveedor,
                        v_nom_proveedor;
                        EXIT WHEN l_cursor%NOTFOUND;
                    dbms_output.put_line
                        (
                        v_cod_proveedor|| '|'||
                        v_nom_proveedor
                        );
                    END LOOP;
                    CLOSE l_cursor;
                END;
            """)
    textVar = c.var(str)
    statusVar = c.var(int)
    list = []
    while True:
        c.callproc("dbms_output.get_line", (textVar, statusVar))
        if statusVar.getvalue() != 0:
            break
        arr = str(textVar.getvalue()).split("|")
        obj = {
        'cod_proveedor' : arr[0],
        'nombre_proveedor': arr[1]
        }
        list.append(obj)
    return response.json({"msj": "OK", "obj": list}, 200)


@app.route('/totales_pedido', ["POST", "GET"])
@jwt_required
async def totales(request, token: Token):

    data = request.json
    list = await totales_pedido(int(data['idPedido']))
    return response.json({"msj": "OK", "totales": list}, 200)

async def totales_pedido(idPedido):

    try:
        db = get_db()
        c = db.cursor()

        c.callproc("dbms_output.enable")
        c.execute("""
                    DECLARE

                    output number DEFAULT 1000000;

                    v_total_bruto varchar(20);
                    v_desc_volumen varchar(20);
                    v_otros_descuentos varchar(20);
                    v_desc_adicional varchar(20);
                    v_desc_dpp varchar(20);
                    v_sub_total varchar(20);
                    v_impuesto varchar(20);
                    v_total varchar(20);
                    v_totalGravado varchar(20);
                    v_totalExento  varchar(20);
                    v_descImpuesto varchar(20);
                    v_totalNetoUSD varchar(20);
                    v_tipoCambio  varchar(20);

                BEGIN


                    dbms_output.enable(output);

                    PROCESOSPW.totales_pedido ({idPedido},v_total_bruto,v_desc_volumen, v_otros_descuentos, v_desc_adicional, v_desc_dpp, v_sub_total, v_impuesto, v_total,v_totalGravado, v_totalExento, v_descImpuesto, v_totalNetoUSD,v_tipoCambio  );

                    dbms_output.put_line(
                                v_total_bruto|| '|'||
                                v_desc_volumen|| '|'||
                                v_otros_descuentos|| '|'||
                                v_desc_adicional|| '|'||
                                v_desc_dpp|| '|'||
                                v_sub_total|| '|'||
                                v_impuesto|| '|'||
                                v_total|| '|'||
                                v_totalGravado|| '|'||
                                v_totalExento|| '|'||
                                v_descImpuesto|| '|'||
                                v_totalNetoUSD|| '|'||
                                v_tipoCambio);
            END;
                """.format( idPedido = idPedido ))
        textVar = c.var(str)
        statusVar = c.var(int)
        list = {}
        while True:
            c.callproc("dbms_output.get_line", (textVar, statusVar))
            if statusVar.getvalue() != 0:
                break

            if textVar.getvalue() == None:
                break

            arr = str(textVar.getvalue()).split("|")
            obj = {
                    'total_bruto' :  formatFloatDdo(arr[0]),
                    'desc_volumen' :  formatFloatDdo(arr[1]),
                    'otros_descuentos' :  formatFloatDdo(arr[2]),
                    'desc_adicional' :  formatFloatDdo(arr[3]),
                    'desc_dpp' :  formatFloatDdo(arr[4]),
                    'sub_total' :  formatFloatDdo(arr[5]),
                    'impuesto' :  formatFloatDdo(arr[6]),
                    'total' :  formatFloatDdo(arr[7]),
                    'totalGravado' :  formatFloatDdo(arr[8]),
                    'totalExento' :  formatFloatDdo(arr[9]),
                    'descImpuesto' :  arr[10],
                    'totalNetoUSD' :  formatFloatDdo(arr[11]),
                    'tipoCambio' :  formatFloatDdo(arr[12]),
            }
            list = obj
        return list
    except Exception as e:
        logger.debug(e)
        return e



async def logAudit(user, module, accion, context):

    db = get_mongo_db()

    log = dict(
        username = user,
        module = module,
        accion = accion,
        context = context,
    )

    await db.audit.insert_one(log)

    return

def formatFloatDdo(value):

    if len(value) > 0:
    	x = value.replace(",", ".")
    	x = float(x)
    else:
    	x = float(0)

    return x



# app.run(host='0.0.0.0', port = port, debug = True)
ssl = {'cert': 'conf/ssl.crt/server.crt', 'key': 'conf/ssl.key/sever.key'}
app.run(host='0.0.0.0', port= port, debug = False, workers=4)
