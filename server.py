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
import mysql.connector
from datetime import datetime, timedelta, date
from sanic import Sanic
from sanic import response
from sanic_cors import CORS, cross_origin
from sanic.handlers import ErrorHandler
from sanic.exceptions import SanicException
from sanic.log import logger
from sanic_jwt_extended import (JWT, jwt_required)
from sanic_jwt_extended.exceptions import JWTExtendedException
from sanic_jwt_extended.tokens import Token
from motor.motor_asyncio import AsyncIOMotorClient
from sanic.exceptions import ServerError
from sanic_openapi import swagger_blueprint
from sanic_openapi import doc
from sanic_gzip import Compress
from sanic_jinja2 import SanicJinja2
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr
from pprint import pprint

import http.client
import json
import multiprocessing
workers = multiprocessing.cpu_count()

class CustomHandler(ErrorHandler):
    def default(self, request, exception):
        return response.json(str(exception), 501)


app = Sanic(__name__)
# Compress(app)
port = 3500

compress = Compress()
sio = socketio.AsyncServer(async_mode='sanic')
sio.attach(app)

with JWT.initialize(app) as manager:
    manager.config.secret_key = "ef8f6025-ec38-4bf3-b40c-29642ccd63128995"
    # manager.config.jwt_access_token_expires = timedelta(minutes=1)
    manager.config.jwt_access_token_expires = False
    manager.config.rbac_enable = True

app.blueprint(swagger_blueprint)
CORS(app, automatic_options=True)
# Compress(app)
jinja = SanicJinja2(app)



def get_mysql_db():
    connection  = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="portal_ddo"
        )
    return connection

def generate_session_pool():
    dsn_tns = cx_Oracle.makedsn(
        '192.168.168.218', '1521', service_name='DELOESTE')

    return cx_Oracle.SessionPool(user=r'APLPAGWEB', password='4P1P4GWE3', dsn=dsn_tns, min=2,
                            max=5, increment=1, encoding="UTF-8")

pool = generate_session_pool()

def get_oracle_db():
    connection = pool.acquire()
    return connection


@app.middleware('request')
async def print_on_request(request):
    db = get_mysql_db()
    if  'authorization' in request.headers:
        access_token = request.headers['authorization'][7:]
        session = await getSessionTokenBySession(db, access_token)

        if not session:
            return response.json({"msg": "Sin sesion activa"}, status=401)

        present = datetime.now()
        expired_at = (present + timedelta(minutes = 15))
        await udpSessionExpiredAt(db, access_token, expired_at )



#@app.middleware('response')
async def print_on_response(request, response):
    if response.status == 401:
        db = get_mongo_db()
        db.session_token.delete_many({'access_token':request.headers['authorization'][7:] })


@app.route('/resetPass', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
async def availableUser(request):
    data = request.json
    db = get_mysql_db()
    # username = data.get("username", None)
    user = None
    #print(data)

    if 'username' in data:
        user = await db.user.find_one({'username': data['username']}, {'_id': 0})

    elif user == None:
        user = await db.user.find_one({'email': data['email']}, {'_id': 0})

    else:
        response.json({"msg": "Missing username parameter"}, status=400)

    if user == None:
        response.json({"msg": "Missing username parameter"}, status=400)

    await db.user.update_one({'username': data['username']}, {"$set": {'password': data["password"]}})

    emailData = dict(
        template="",
        to=user['email'],
        subject="Reinicio de Password",
        user=user,
        newpass=data.get("newpass", None),
        password=data.get("password", None),
        type="reset"
    )
    ##print(emailData)
    smtp = http.client.HTTPConnection('127.0.0.1', 8888)
    result = smtp.request("POST", "/put",
                          json.dumps(emailData)
                          )

    # await prepareMail(emailData)

    return response.json(result, 200)

async def getUserbyUserName(db, username ):
    c = db.cursor() 
    sql = """SELECT `id_usuarios`,
                `role`,
                `name`,
                `email`,
                `username`,
                `password`,
                `COD_CIA`,
                `GRUPO_CLIENTE`,
                `COD_CLIENTE`,
                `permisos`,
                `estatus`
            FROM usuarios WHERE username = \'{username}\' """.format(username=username)
    c.execute(sql,username)
    result = c.fetchone()
    print(result)
    if result:
        user = {
            "id_user":result[0],
            "role":result[1],
            "name" : result[2],
            "email" : result[3],
            "username" : result[4],
            "password" : result[5],
            "COD_CIA" : result[6],
            "GRUPO_CLIENTE" : result[7],
            "COD_CLIENTE" : result[8],
            "permisos" : json.loads(result[9]),
            "estatus" : result[10]
        }
    else:

        return None

    return user

async def getSessionToken(db, username ):
    c = db.cursor() 
    sql = """SELECT * FROM session_token WHERE username =  \'{username}\' """.format(username=username)
    print(sql)
    c.execute(sql)
    user = c.fetchone()
    return user

async def getSessionTokenBySession(db, access_token ):
    c = db.cursor() 
    sql = """SELECT * FROM session_token WHERE access_token =  \'{access_token}\' """.format(access_token=access_token)
    c.execute(sql)
    session_token = c.fetchone()
    return session_token

async def udpSessionExpiredAt(db, access_token, expired_at ):
    c = db.cursor()
    sql = """UPDATE `session_token` SET `expired_at` = \'{expired_at}\', `renovated` = 1 WHERE `access_token` = \'{access_token}\';""".format(access_token=access_token, expired_at=expired_at)
    c.execute(sql)
    db.commit()
    return 

async def insertSessionToken(db, access_token, username, expired_at):
    c = db.cursor() 
    sql = """INSERT INTO `portal_ddo`.`session_token`
            (`access_token`,
            `username`,
            `expired_at`,create_at)
            VALUES
            (\'{access_token}\',\'{username}\',\'{expired_at}\', NOW())
            """.format(access_token=access_token,
            username=username,expired_at=expired_at)
    c.execute(sql)
    db.commit()
     
@app.route("/login", ["POST"])
async def login(request):
    data = request.json
    username = data.get("username", None)
    password = data.get("password", None)
    if not username:
        return response.json({"msg": "Missing username parameter"}, status=400)
    if not password:
        return response.json({"msg": "Missing password parameter"}, status=400)

    db = get_mysql_db()
    dbOracle = get_oracle_db()
    # await validaSession(db)
    user = await getUserbyUserName(db,username)

    if user:

        if user['password'] == password:

            if user['estatus'] != "Activo":
                return response.json({"msg": "Usuario inactivo"}, status=430)

            session_activa = await getSessionToken(db,username)

            if session_activa:
                return response.json({"msg": "Usuario ya se encuentra conectado por favor cierre todas las sesiones"}, status=435)

            access_token = JWT.create_access_token(identity=username)

            expired_at = (datetime.now() + timedelta(minutes = 15))

            await insertSessionToken(db, access_token, username, expired_at)

            if user['role'] == 'root' or user['role'] == 'sisAdm' or user['role'] == 'seller' :
                return response.json({'access_token': access_token, 'user': user}, 200)
            else:
                disponible_cli = await disponible_cliente(dbOracle,user['COD_CIA'],user['GRUPO_CLIENTE'],user['COD_CLIENTE'])
                data ={
                "pNoCia":user['COD_CIA'],
                "pNoGrupo":user['GRUPO_CLIENTE'],
                "pCliente":user['COD_CLIENTE']
                }
                client = await clientes(dbOracle, data)
                return response.json({'access_token': access_token, 'user': user, 'cliente':client, 'disponible_cliente':disponible_cli}, 200)
        else:
            return response.json({"msg": "Contraseña invalida"}, status=403)
    else:

        return response.json({"msg": "Usuario no existe"}, status=403)


# @app.route("/logout", ["POST", "GET"])
# # @compress.compress
# @doc.exclude(True)
# async def logout(request):
#     db = get_mysql_db()
#     data = request.json
#     await db.session_token.delete_many({'access_token': data.get("token", None)})
#     return response.json({"msg":"success"}, status=200)

@app.route("/logout", ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
async def logout(request):
    data = request.json
    db = get_mysql_db()
    c = db.cursor() 
    sql = """DELETE FROM `session_token`
	    WHERE access_token =  \'{access_token}\' """.format(access_token=data.get("token", None))
    c.execute(sql)
    db.commit()
    return response.json({"msg":"success"}, status=200)

@app.route("/validaSession", ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
async def validaSession(request):
    db = get_mysql_db()
    now = datetime.now()
    list=await  db.session_token.find({'expired_at': {'$gte': datetime.timestamp(now) }}).to_list(length=None)
    pprint(list)
    return response.json({"msg":"success"}, status=200)


@app.route("/refresh_token", ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def refresh_token(request):
    refresh_token = JWT.create_refresh_token(identity=str(uuid.uuid4()))
    return response.json({'refresh_token': refresh_token}, status=200)


@app.route("/validate_token", ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
async def validate_token(request): # token: Token):
    ##print("valido")
    return response.json({'msg': "OK"}, status=200)

async def getUserByEmail(db, email ):
    c = db.cursor() 
    sql = """SELECT * FROM user WHERE email =  \'{email}\' """.format(email=email)
    c.execute(sql)
    user = c.fetchone()
    return user

async def getUserByUsername(db, username ):
    c = db.cursor() 
    sql = """SELECT * FROM user WHERE username =  \'{username}\' """.format(username=username)
    c.execute(sql)
    user = c.fetchone()
    return user

# async def insertSessionToken(db, user):
#     c = db.cursor() 
#     sql = """INSERT INTO `usuarios`( `role`, `name`, `email`, `username`, `password`, `COD_CIA`, `GRUPO_CLIENTE`, `COD_CLIENTE`, `permisos`, `estatus`)
#             VALUES
#             #TODO (\'{access_token}\',\'{username}\',\'{expired_at}\', NOW())
#             """.format(access_token=access_token,
#             username=username,expired_at=expired_at)
#     c.execute(sql)
#     db.commit()

@app.route('/add/user', ["POST", "GET"])
# @compress.compress
# @doc.exclude(True)
# #@jwt_required
async def addUser(request): # token: Token):
    user = request.json
    db = get_mysql_db()

    # userEmail = await db.user.find_one({'email': user.get("email", None)}, {'_id': 0})
    userEmail = await getUserByEmail(db,user.get("email", None) )

    if userEmail != None:
        return response.json({"msg": "Email no disponible"}, status=400)

    # username = await db.user.find_one({'username': user.get("username", None)}, {'_id': 0})
    username = await getUserByUsername(db, user.get("username", None) )
    if username != None:
        return response.json({"msg": "Username no disponible"}, status=400)

    emailData = dict(
        to=user['email'],
        subject="Bienvenido",
        user=user['username'],
        type="welcome"
    )
    ##print(emailData)
    smtp = http.client.HTTPConnection('127.0.0.1', 8888)
    result = smtp.request("POST", "/put",
                          json.dumps(emailData)
                          )

    await db.user.insert_one(user)

    return response.json("OK", 200)


@app.route('/upd/user', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def addUser(request): # token: Token):
    user = request.json
    db = get_mysql_db()

    # await db.user.insert_one(user)

    await db.user.replace_one({'username': user.get("username", None)}, user)

    return response.json("OK", 200)


@app.route('/upd/user_pass', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def user_pass(request): # token: Token):
    user = request.json
    db = get_mysql_db()

    await db.user.update_one({'username': user.get("username", None)}, {"$set": {'password': user.get("password", None)}})

    return response.json("OK", 200)


@app.route('/del/user', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def addUser(request): # token: Token):
    user = request.json
    db = get_mysql_db()

    # await db.user.insert_one(user)

    await db.user.delete_one({'username': user.get("username", None)})

    return response.json("OK", 200)


@app.route('/get/users', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def listUser(request): # token: Token):
    data = request.json
    db = get_mysql_db()

    users = []
    ##print(data)
    if 'role' in data:

        if data['role'] == "root":

            users = await db.user.find({'role': {'$in': ['root', 'sisAdm', 'seller']}}, {'_id': 0}).to_list(length=None)

        if data['role'] == "sisAdm":

            users = await db.user.find({'role': {'$in': ['sisAdm', 'seller']}}, {'_id': 0}).to_list(length=None)

    else:
        users = await db.user.find({'COD_CLIENTE': data['pCliente']}, {'_id': 0}).to_list(length=None)

    return response.json(users, 200)


@app.route('/get/user', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def availableUser(request): # token: Token):
    data = request.json
    db = get_mysql_db()
    # username = data.get("username", None)
    users = await db.user.find_one({'username': data.get("username", None)}, {'_id': 0})

    return response.json(users, 200)


@app.route('/available/user', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def availableUser(request): # token: Token):
    data = request.json
    db = get_mysql_db()
    # username = data.get("username", None)
    users = await db.user.find_one({'username': data.get("username", None)}, {'_id': 0})

    return response.json(users, 200)


@app.route('/available/email', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def availableUser(request): # token: Token):
    data = request.json
    db = get_mysql_db()
    # username = data.get("username", None)
    users = await db.user.find_one({'email': data.get("email", None)}, {'_id': 0})

    return response.json(users, 200)


@app.route('/disponible_cliente', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
async def procedure(request):

    data = request.json

    db = get_oracle_db()
    c = db.cursor()

    if not 'pCliente' in data:
        return response.json({"msg": "Missing parameter"}, status=400)

    if not 'pNoCia' in data:
        return response.json({"msg": "Missing parameter"}, status=400)

    if not 'pNoGrupo' in data:
        return response.json({"msg": "Missing parameter"}, status=400)


    vdisp_bs = c.var(float)
    vdisp_usd = c.var(float)
    l_result = c.callproc("""PROCESOSPW.disponible_cliente""",[
        vdisp_bs,
        vdisp_usd,
        data['pNoCia'],
        data['pNoGrupo'],
        data['pCliente']
        ])[0]

    obj = {
        'disp_bs': vdisp_bs.getvalue(),
        'disp_usd': vdisp_usd.getvalue()
    }


    return response.json({"msj": "OK", "obj": obj}, 200)

async def disponible_cliente(db, cia, grp, cli):
    c = db.cursor()
    vdisp_bs = c.var(float)
    vdisp_usd = c.var(float)
    l_result = c.callproc("""PROCESOSPW.disponible_cliente""",[
        vdisp_bs,
        vdisp_usd,
        cia,
        grp,
        cli
        ])[0]

    obj = {
        'disp_bs': vdisp_bs.getvalue(),
        'disp_usd': vdisp_usd.getvalue()
    }

    return obj

@app.route('/procedure_clientes', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
async def procedure(request):

    data = request.json

    db = get_oracle_db()

    list = await clientes(db, data)

    return response.json({"msj": "OK", "obj": list}, 200)

async def clientes(db,data ):

    c = db.cursor()

    if not 'pTotReg' in data or data['pTotReg'] == 0:
        data['pTotReg'] = 100

    if not 'pTotPaginas' in data or data['pTotPaginas'] == 0:
        data['pTotPaginas'] = 100

    if not 'pPagina' in data:
        # data['pPagina'] = 'null'
        data['pPagina'] = None

    if not 'pLineas' in data or data['pLineas'] == 0:
        data['pLineas'] = 100

    if not 'pDireccion' in data:
        # data['pDireccion'] = 'null'
        data['pDireccion'] = None
    # else:
    #     data['pDireccion'] = "'"+data['pDireccion']+"'"

    if not 'pNoCia' in data:
        data['pNoCia'] = 'null'
    # else:
    #     data['pNoCia'] = "'"+data['pNoCia']+"'"

    if not 'pNoGrupo' in data:
        data['pNoGrupo'] = None
        # data['pNoGrupo'] = 'null'
    # else:
    #     data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"
    if not 'pCliente' in data:
        # data['pCliente'] = 'null'
        data['pCliente'] = None
    # else:
    #     data['pCliente'] = "'"+data['pCliente']+"'"

    if not 'pNombre' in data:
        # data['pNombre'] = 'null'
        data['pNombre'] = None
    # else:
    #     data['pNombre'] = "'"+data['pNombre']+"'"

    #print(data)
    l_cur = c.var(cx_Oracle.CURSOR)
    l_result = c.callproc("""PROCESOSPW.clientes""",[
        l_cur,
        data['pTotReg'],
        data['pTotPaginas'],
        data['pPagina'],
        data['pLineas'],
        data['pNoCia'],
        data['pNoGrupo'],
        data['pCliente'],
        data['pNombre'],
        data['pDireccion']
        ])[0]
    list = []
    for arr in l_result:
        obj = {
                'cod_cia': arr[0],
                'nombre_cia': arr[1],
                'grupo_cliente': arr[2],
                'nom_grupo_cliente': arr[3],
                'cod_cliente': arr[4],
                'nombre_cliente': arr[5],
                'direccion_cliente': arr[6],
                'direccion_entrega_cliente': arr[7],
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
                'monto_minimo': arr[21],
                'tipo_venta': arr[22],
                'limite_credito': arr[23],
                'vendedor': arr[24],
                'max_unid_med_emp': arr[25],
                'max_unid_misc_emp': arr[26],
                'unid_fact_med_emp': arr[27],
                'unid_fact_misc_emp': arr[28],
                'unid_disp_med_emp': arr[29],
                'unid_disp_misc_emp': arr[30],
                'monto_min_pick': arr[31],
                'ind_emp_nolim': arr[32],
                'email_vendedor': arr[33],
                'email_persona_cyc': arr[34],
                'pagina': arr[35],
                'linea': arr[36]
            }
        list.append(obj)

    return list


@app.route('/procedure_deudas', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def procedure(request): # token: Token):
# async def procedure(request):

    data = request.json

    if not 'pTotReg' in data or data['pTotReg'] == 0:
        data['pTotReg'] = 100

    if not 'pTotPaginas' in data or data['pTotPaginas'] == 0:
        data['pTotPaginas'] = 100

    if not 'pPagina' in data or data['pPagina'] == 0:
        data['pPagina'] = 1

    if not 'pLineas' in data or data['pLineas'] == 0:
        data['pLineas'] = 100

    if not 'pDeuda' in data:
        data['pDeuda'] =None

    if not 'pNoCia' in data:
        data['pNoCia'] = '01'
    # else:
    #     data['pNoCia'] = "'"+data['pNoCia']+"'"

    if not 'pNoGrupo' in data:
        data['pNoGrupo'] = '01'
    # else:
    #     data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"

    if not 'pCLiente' in data:
        data['pCLiente'] =None
    # else:
    #     data['pCLiente'] = "'"+data['pCLiente']+"'"

    if not 'pNombre' in data:
        data['pNombre'] =None
    # else:
    #     data['pNombre'] = "'"+data['pNombre']+"'"

    if not 'pTipo' in data:
        data['pTipo'] =None
    # else:
    #     data['pTipo'] = "'"+data['pTipo']+"'"

    if not 'pEstatus' in data:
        data['pEstatus'] =None
    # else:
    #     data['pEstatus'] = "'"+data['pEstatus']+"'"

    db = get_oracle_db()
    c = db.cursor()
    # l_cursor, pTotReg ,pTotPaginas, pPagina, pLineas, pNoCia, pNoGrupo, pCLiente);
    l_cur = c.var(cx_Oracle.CURSOR)
    l_result = c.callproc("""PROCESOSPW.deudas""",[
        l_cur,
        data['pTotReg'],
        data['pTotPaginas'],
        data['pPagina'],
        data['pLineas'],
        data['pNoCia'],
        data['pNoGrupo'],
        data['pCLiente'],
        ])[0]
    list = []
    for arr in l_result:

        obj = {
                'no_fisico': arr[0],
                'codigo_cliente': arr[1],
                'nombre_cliente': arr[2],
                'tipo_venta': arr[3],
                'fecha_vencimiento': dateByResponse(arr[3]),
                'monto_inicial': arr[5],
                'monto_actual': arr[6],
                'monto_inicial_usd': arr[7],
                'monto_actual_usd': arr[8],
                'fecha_ultimo_pago': dateByResponse(arr[9]),
                'monto_ultimo_pago': arr[10],
                'estatus_deuda': arr[11],
                'codigo_tipo_doc': arr[12],
                'nombre_tipo_doc': arr[13],
                'cia': arr[14],
                'grupo': arr[15],
                'tipo_cambio': arr[16],
                'fecha_aviso':  dateByResponse(arr[17]),
                'docu_aviso': arr[18],
                'serie_fisico': arr[19],
                'fecha_documento': dateByResponse(arr[20]),
                'aplica_corte': arr[21],
                'fecha_entrega':  dateByResponse(arr[22])
            }
        list.append(obj)

    return response.json({"msj": "OK", "obj": list}, 200)


@app.route('/procedure_productos', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
async def procedure(request):

    data = request.json

    ##print(data)

    if not 'pTotReg' in data or data['pTotReg'] == 0:
        data['pTotReg'] = 100

    if not 'pTotPaginas' in data or data['pTotPaginas'] == 0:
        data['pTotPaginas'] = 100

    if not 'pPagina' in data or data['pPagina'] == 0:
        data['pPagina'] = 1

    if not 'pLineas' in data or data['pLineas'] == 0:
        data['pLineas'] = 100

    if not 'pNoCia' in data:
        return response.json({"msg": "Missing username parameter"}, status=400)

    if not 'pNoGrupo' in data:
        return response.json({"msg": "Missing username parameter"}, status=400)

    if not 'pCliente' in data:
        # data['pCliente'] = 'null'
        data['pCliente'] = None

    if not 'pBusqueda' in data or data['pBusqueda'] == None:
        # data['pBusqueda'] = 'null'
        data['pBusqueda'] = None

    if not 'pComponente' in data:
        # data['pComponente'] = 'null'
        data['pComponente'] = None

    if not 'pArticulo' in data:
        data['pArticulo'] = None

    if not 'pCodProveedor' in data or data['pCodProveedor'] == None:
        data['pCodProveedor'] = None

    if not 'pFiltroCategoria' in data or data['pFiltroCategoria'] == None:
        data['pFiltroCategoria'] = None

    if not 'pExistencia' in data or data['pExistencia'] == None:
        data['pExistencia'] = None


    ##print(data)
    db = get_oracle_db()
    c = db.cursor()
    # #print(data)

    print(data)
    l_cur = c.var(cx_Oracle.CURSOR)


    l_result = c.callproc("""PROCESOSPW.productos""",[
                l_cur,
                0, #data['pTotReg'],
                0, #data['pTotPaginas'],
                None, #data['pPagina'],
                10, #data['pLineas'],
                data['pNoCia'],
                data['pNoGrupo'],
                data['pCliente'],
                data['pBusqueda'],
                data['pComponente'],
                data['pArticulo'],
                data['pFiltroCategoria'],
                data['pCodProveedor'],
                None,#data['pExistencia']
                ])[0]
    list = []
    for arr in l_result:
        obj = {
            'cod_producto': arr[0],
            'nombre_producto': arr[1],
            'princ_activo': arr[2],
            'ind_regulado': arr[3],
            'ind_impuesto': arr[4],
            'ind_psicotropico': arr[5],
            'fecha_vence': arr[6],
            'existencia': arr[7],
            'precio_bruto_bs': arr[8],
            'precio_neto_bs': arr[9],
            'iva_bs': arr[10],
            'precio_neto_usd': arr[11],
            'iva_usd': arr[12],
            'tipo_cambio': arr[13],
            'proveedor': arr[14],
            'bodega': arr[15],
            'categoria': arr[16],
            'descuento1': arr[17],
            'descuento2': arr[18],
            'tipo_prod_emp': arr[19],
            'disp_prod_emp': arr[20],
            'dir_imagen': arr[21],
            'division': arr[22],
            'unidad_manejo':arr[23],
            'pagina': arr[24],
            'linea': arr[25]
            }
        list.append(obj)

    return response.json({"msg": "OK", "obj": list}, 200)

@app.route('/procedure_productosOLD', ["POST", "GET"])
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
    db = get_oracle_db()
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
        if not row["nro_factura"] in list:
            list[int(row["nro_factura"])] = []

    for row in arreglo:
        list[int(row["nro_factura"])].append(row)

    return list


@app.route('/procedure_facturacion', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
async def procedure(request):

    data = request.json
    ##print(data)
    if not 'pTotReg' in data or data['pTotReg'] == 0:
        data['pTotReg'] = 100

    if not 'pTotPaginas' in data or data['pTotPaginas'] == 0:
        data['pTotPaginas'] = 100

    if not 'pPagina' in data or data['pPagina'] == 0:
        data['pPagina'] = 1

    if not 'pLineas' in data or data['pLineas'] == 0:
        data['pLineas'] = 100

    if not 'pDeuda' in data:
        data['pDeuda'] = 'null'
    else:
        data['pDeuda'] = "'"+data['pDeuda']+"'"

    if not 'pNoCia' in data:
        data['pNoCia'] = 'null'
    # else:
    #     data['pNoCia'] = "'"+data['pNoCia']+"'"

    if not 'pNoCia' in data:
        data['pNoCia'] = '01'
    # else:
    #     data['pNoCia'] = "'"+data['pNoCia']+"'"

    if not 'pNoGrupo' in data:
        data['pNoGrupo'] = '01'
    # else:
    #     data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"

    if not 'pCliente' in data:
        data['pCliente'] = 'null'
    # else:
    #     data['pCliente'] = "'"+data['pCliente']+"'"

    if not 'pNombre' in data:
        data['pNombre'] = 'null'
    # else:
    #     data['pNombre'] = "'"+data['pNombre']+"'"

    if not 'pFechaFactura' in data:
        data['pFechaFactura'] = 'null'

    if not 'pFechaPedido' in data:
        data['pFechaPedido'] = 'null'

    ##print(data)
    db = get_oracle_db()
    c = db.cursor()
    l_cur = c.var(cx_Oracle.CURSOR)
    l_result = c.callproc("""PROCESOSPW.pedidos_facturados""",[
        l_cur,
        data['pTotReg'],
        data['pTotPaginas'],
        None,
        data['pLineas'],
        None,
        data['pNoCia'],
        data['pNoGrupo'],
        data['pCliente'],
        None
        ])[0]
    list = []
    for arr in l_result:

        obj = {
            'nro_factura': arr[0],
            'fecha_factura': dateByResponse(arr[1]),
            'cod_cliente': arr[2],
            'cod_vendedor': arr[3],
            'nombre_vendedor': arr[4],
            'email_vendedor': arr[5],
            'no_linea': arr[6],
            'no_arti': arr[7],
            'nombre_arti': arr[8],
            'unidades_pedido': arr[9],
            'unidades_facturadas': arr[10],
            'total_producto':  arr[11],
            'total_producto_usd': arr[12],
            'codigo_compani': arr[13],
            'grupo': arr[14],
            'tipo_pedido': arr[15],
            'fecha_entrega':  dateByResponse(arr[16])
            }
        list.append(obj)


    return response.json({"msj": "OK", "obj": agrupar_facturas(list)}, 200)


@app.route('/valida/client', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def valida_client(request): # token: Token):
    try:
        data = request.json

        if not 'pNoCia' in data:
            return response.json({"msg": "Missing password parameter cia"}, status=400)
        # else:
        #     data['pNoCia'] = "'"+data['pNoCia']+"'"

        if not 'pNoGrupo' in data:
            return response.json({"msg": "Missing password parameter grupo"}, status=400)
        # else:
        #     data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"

        if not 'pCliente' in data:
            return response.json({"msg": "Missing password parameter cliente"}, status=400)
        # else:
        #     data['pCliente'] = "'"+data['pCliente']+"'"

        if not 'pMoneda' in data:
               data['pMoneda'] = '\'P\''
        # else:
        #     data['pMoneda'] = "'"+data['pMoneda']+"'"

        db = get_oracle_db()
        c = db.cursor()
        sql = """select t2.DESCRIPCION
                        from dual
                    join TIPO_ERROR t2 on PAGINAWEB.PROCESOSPW.valida_cliente(\'{pNoCia}\',\'{pNoGrupo}\',\'{pCliente}\',{pMoneda},0) = t2.CODIGO""".format(
            pNoCia=data['pNoCia'],
            pNoGrupo=data['pNoGrupo'],
            pCliente=data['pCliente'],
            pMoneda=data['pMoneda']
            )
        #print(sql)
        c.execute(sql)
        row = c.fetchone()
        # #print("==============================================================row")
        # #print(row)
        if row == None:
            return response.json({"msg": "success"}, 200)

        return response.json({"data": row}, 450)

    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)


async def crear_pedido(request):
    try:
        data = request.json

        db = get_oracle_db()
        c = db.cursor()

        sql = """SELECT
                COUNT(ID)
                FROM PAGINAWEB.PEDIDO WHERE COD_CLIENTE = :COD_CLIENTE and ESTATUS in(0,1,2)"""
        c.execute(sql, [data['COD_CLIENTE']])
        count = c.fetchone()

        if int(count[0]) > 0:
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

        statusVar = c.var(cx_Oracle.NUMBER)
        lineVar = c.var(cx_Oracle.STRING)
        ID = None
        while True:
          c.callproc("dbms_output.get_line", (lineVar, statusVar))
          if lineVar.getvalue() == None:
              break

          ID = lineVar.getvalue()

          if statusVar.getvalue() != 0:
            break
        db.commit()
        pool.release(db)
        return ID
    except Exception as e:
        logger.debug(e)


async def update_detalle_pedido(db, detalle, ID, pCia, pGrupo, pCliente):
    try:
        # #print("====================update_detalle_pedido=====================")
        # db = get_oracle_db()
        c = db.cursor()
        ##print(detalle)
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

        # #print("actualizo con cero")

        cantidad = 0
        respuesta = await valida_art(db,pCia, detalle['COD_PRODUCTO'], pGrupo, pCliente, detalle['CANTIDAD'], float(str(detalle['precio_bruto_bs']).replace(',', '.')), int(ID))
        # #print("paso valida art")
        if respuesta != 1:
            return respuesta
        # #print("sigue")
        disponible = detalle['CANTIDAD']

        c.execute("""UPDATE PAGINAWEB.DETALLE_PEDIDO
                        SET
                                CANTIDAD     = :CANTIDAD,
                                PRECIO_BRUTO = :PRECIO_BRUTO
                        WHERE  ID_PEDIDO    = :ID_PEDIDO
                        AND    COD_PRODUCTO = :COD_PRODUCTO""",
                    [
                            int(disponible),
                            float(
                                str(detalle['precio_bruto_bs']).replace(',', '.')),
                            ID,
                            detalle['COD_PRODUCTO']
                        ])
        db.commit()
        #print("ejecuto todo")
        return disponible

    except Exception as e:
        logger.debug(e)


@app.route('/upd/detalle_producto', ["POST", "GET"])
@doc.exclude(True)
#@jwt_required
async def upd_detalle_producto_serv(request): # token: Token):
        data = request.json
        db = get_oracle_db()
        pedidoValido = await validate_Pedido(db,data['ID'])

        if not pedidoValido:
            return response.json({"msg": "NO PUEDE EDITAR ESTE PEDIDO"}, status=410)

        #print(data)
        reservado = await update_detalle_pedido(db,data['pedido'], data['ID'], data['pNoCia'], data['pNoGrupo'], data['pCliente'])
        #print("======================reservado===================")
        if isinstance(reservado, str):
            return response.json({"msg": respuesta}, 480)

        msg = 0

        totales = await totales_pedido(db, int(data['ID']))

        await upd_estatus_pedido(db,6, data['ID'])


        return response.json({"msg": msg, "reserved": reservado, "totales": totales}, 200)



async def crear_detalle_pedido(db, detalle, ID, pCia, pGrupo, pCliente):

    try:

        cantidad = 0

        disponible = await existencia_disponible(db, pCia, detalle['COD_PRODUCTO'], detalle['CANTIDAD'])

        if disponible == -1:
            return "No se pudo completar por favor verifique la disponibilidad del producto"

        respuesta = await valida_art(db, pCia, detalle['COD_PRODUCTO'], pGrupo, pCliente, disponible, float(str(detalle['precio_bruto_bs']).replace(',', '.')), int(ID))

        if respuesta != 1:
            return respuesta

        c = db.cursor()

        sql = """INSERT INTO DETALLE_PEDIDO ( ID_PEDIDO, COD_PRODUCTO, CANTIDAD, PRECIO_BRUTO, TIPO_CAMBIO, BODEGA)
                        VALUES ( {ID_PEDIDO}, \'{COD_PRODUCTO}\' ,  {CANTIDAD} ,  {PRECIO} , {TIPO_CAMBIO}, \'{BODEGA}\' )""".format(
            ID_PEDIDO=int(ID),
            COD_PRODUCTO=str(detalle['COD_PRODUCTO']),
            CANTIDAD=int(disponible),
                                        PRECIO=float(
                                            str(detalle['precio_bruto_bs']).replace(',', '.')),
                                        TIPO_CAMBIO=float(
                                            str(detalle['tipo_cambio']).replace(',', '.')),
            BODEGA=detalle['bodega']
            )

        c.execute(sql)

        db.commit()

        return disponible

    except Exception as e:
        logger.debug(e)


async def upd_estatus_pedido(db, estatus, ID):
    #print("upd_estatus")
    # db = get_oracle_db()
    c = db.cursor()

    sql = """
                UPDATE PAGINAWEB.PEDIDO
                SET
                    ESTATUS          = {ESTATUS}
                WHERE  ID               = {ID}

        """.format(  ESTATUS = estatus, ID = int(ID))
    #print(sql)
    c.execute(sql)
    #print("ejeuto query")
    db.commit()
    ##print("============================ejecuto======================")
    sql = """select descripcion
                    from ESTATUS where codigo = :estatus"""
    c.execute(sql, [estatus])
    row = c.fetchone()
    return row[0]

async def upd_tipo_pedido(db,ID, tipoPedido = "N"):
        #print("upd_tipo_pedido")
        c = db.cursor()

        sql = """
                    UPDATE PAGINAWEB.PEDIDO
                    SET
                        TIPO_PEDIDO      = :TIPO_PEDIDO
                    WHERE  ID               = :ID

            """

        c.execute(sql, [tipoPedido, ID])
        #print("ejecuto query")

        db.commit()

        return


async def validate_Pedido(db, ID ):

    try:

        # db = get_oracle_db()
        c = db.cursor()

        sql = """
                    SELECT ESTATUS FROM PAGINAWEB.PEDIDO
                    WHERE  ID  = :ID

            """

        c.execute(sql, [ID])

        row = c.fetchone()

        db.commit()

        if row[0] < 2 or row[0] == 6:
            return True
        else:
            return False

    except Exception as e:
        logger.debug(e)


async def tiempo_resta_pedido(db,pIdPedido):
    try:

        # db = get_oracle_db()
        c = db.cursor()
        respuesta = None

        sql = """SELECT PROCESOSPW.tiempo_resta_pedido ({pIdPedido}) from dual""".format(
            pIdPedido=pIdPedido
        )
        # #print(sql)
        c.execute(sql)

        row = c.fetchone()
        return row[0]

    except Exception as e:
        logger.debug(e)


@app.route('/tiempo_resta_pedido/articulo', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def tiempo_resta_pedido(request): # token: Token):
    try:
        data = request.json
        db = get_oracle_db()
        c = db.cursor()

        sql = """SELECT PROCESOSPW.tiempo_resta_pedido ({pIdPedido}) from dual""".format(
            pIdPedido=data['pIdPedido']
        )

        c.execute(sql)

        row = c.fetchone()

        if row != None and row[0] != None:
            return response.json({"time": row[0]},200)
        pool.release(db)
        return response.json({"time": row[0]},407)

    except Exception as e:
        logger.debug(e)


async def valida_art(db,pCia, pNoArti, pGrupo,pCliente,pCantidad,pPrecio,pIdPedido):
    try:

        # db = get_oracle_db()
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
        # #print(sql)
        c.execute(sql)

        row = c.fetchone()
        #print("RESUTADO VALIDA ARTICULO")
        if row != None and row[0] != None:
            #print(row[0])
            return row[0]

        return 1
    except Exception as e:
        logger.debug(e)


async def existencia_disponible(db, pCia, pNoArti, pCantidad ):
    try:

        c = db.cursor()
        respuesta = None
        disponible = 0
        sql = """SELECT PROCESOSPW.existencia_disponible (\'{pCia}\',\'{pNoArti}\') from dual""".format(
            pCia=pCia,
            pNoArti=pNoArti,
        )
        c.execute(sql)

        row = c.fetchone()

        if row != None and row[0] != None and row[0] > 0:
            if int(pCantidad) > int(row[0]):
                return row[0]
            else:
                return int(pCantidad)

        return -1
    except Exception as e:
        logger.debug(e)


@app.route('/valida/articulo', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def valida_articulo(request): # token: Token):
    try:
        data = request.json

        if not 'articulo' in data:
            return response.json({"msg": "Missing username parameter"}, status=480)

        articulo = data['articulo']
        db = get_oracle_db()
        respuesta = await valida_art(db,data['pNoCia'], articulo['COD_PRODUCTO'], data['pNoGrupo'],data['pCliente'],articulo['CANTIDAD'],float(str(articulo['precio_bruto_bs']).replace(',','.')),int(data['idPedido']))

        if respuesta != 1:
            return response.json({"msg": respuesta}, status=480)
        pool.release(db)
        return response.json({"data": row},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)


@app.route('/finalizar_pedido', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def finaliza_pedido(request): # token: Token):
    try:
        data = request.json
        db = get_oracle_db()
        await upd_tipo_pedido(db,data['ID'], data['tipoPedido'])
        await upd_estatus_pedido(db,2, data['ID'])
        pool.release(db)
        return response.json("success", 200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)


@app.route('/editar_pedido', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def editar_pedido(request): # token: Token):
    try:
        data = request.json
        db = get_oracle_db()
        estatus = await upd_estatus_pedido(db,6, data['ID'])
        pool.release(db)
        return response.json({"estatus" : estatus}, 200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)


@app.route('/posponer_pedido', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def editar_pedido(request): # token: Token):
    try:
        data = request.json
        db = get_oracle_db()
        estatus = await upd_estatus_pedido(db,data['estatus'], data['ID'])
        pool.release(db)
        return response.json({"estatus" : estatus}, 200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)


@app.route('/cancel_pedido', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def editar_pedido(request): # token: Token):
    try:
        data = request.json
        db = get_oracle_db
        estatus = await upd_estatus_pedido(db,5, data['ID'])
        pool.release(db)
        return response.json({"estatus" : estatus}, 200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)

@app.route('/add/pedido', ["POST","GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def add_pedido(request): # token: Token):
    # async def procedure(request):
    try:
        data = request.json

        ID = await crear_pedido(request)

        iva_list = []

        for pedido in data['pedido']:
            row = await crear_detalle_pedido(pedido, ID)
            iva_list.append(row)

        mongodb = get_mysql_db()
        totales = dict(
            id_pedido= int(ID),
            productos= iva_list
        )

        await mongodb.order.insert_one(totales)

        return response.json("SUCCESS", 200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)

@app.route('/add/pedidoV2', ["POST","GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def add_pedidoV2(request): # token: Token):
    # async def procedure(request):
    try:
        data = request.json

        ID = await crear_pedido(request)

        if ID == None:
            response.json("ERROR", 400)

        # await logAudit(data['username'], 'pedido', 'add', ID)
        return response.json({"ID": ID},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)

@app.route('/add/detalle_producto', ["POST","GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def add_detalle_producto(request): # token: Token):
    # async def procedure(request):
    try:
        data = request.json
        db = get_oracle_db()
        pedidoValido = False

        pedidoValido = await validate_Pedido(db,data['ID'])

        if not pedidoValido:
            return response.json({"msg": "NO PUEDE EDITAR ESTE PEDIDO"}, status=410)

        respuesta = await crear_detalle_pedido( db,data['pedido'], data['ID'], data['pNoCia'], data['pNoGrupo'], data['pCliente'])

        if isinstance(respuesta, str):
            return response.json({"msg": respuesta  }, 480)

        totales = await totales_pedido(db, int(data['ID']))

        msg = 0

        if data['pedido']['CANTIDAD'] < respuesta:
            msg = 1

        # await upd_estatus_pedido(db,6, data['ID'])

        # await logAudit(data['username'], 'pedido', 'upd', int(data['ID']))

        return response.json({"msg": msg, "reserved": respuesta, "totales": totales },200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)
@app.route('/del/detalle_producto', ["POST","GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def del_detalle_producto(request): # token: Token):
    # async def procedure(request):
    try:
        data = request.json
        db = get_oracle_db()
        pedidoValido = await validate_Pedido(db,data['id_pedido'])

        if not pedidoValido:
            return response.json({"msg": "NO PUEDE EDITAR ESTE PEDIDO"}, status=410)

        await upd_estatus_pedido(db, 6, data['id_pedido'])


        c = db.cursor()

        c.execute("""DELETE FROM DETALLE_PEDIDO WHERE ID_PEDIDO = :ID AND COD_PRODUCTO = :COD_PRODUCTO""",
                  [
                data['id_pedido'],
                data['COD_PRODUCTO']
            ])
        db.commit()

        totales = await totales_pedido(db, int(data['id_pedido']))

        # await logAudit(data['username'], 'pedido', 'del', int(data['id_pedido']))
        # pool.release(db)
        return response.json({"totales": totales},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)

@app.route('/del/pedido', ["POST","GET"])
# @compress.compress
@doc.exclude(True)
# #@jwt_required
# async def update_pedido(request): # token: Token):
async def procedure(request):
    try:
        data = request.json
        ##print(data)

        db = get_oracle_db()
        c = db.cursor()

        c.execute("""DELETE FROM DETALLE_PEDIDO WHERE ID_PEDIDO = :ID""", [data['ID']])

        c.execute("""DELETE FROM PEDIDO WHERE ID = :ID""", [data['ID']])

        db.commit()

        pool.release(db)
        return response.json("SUCCESS", 200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)


@app.route('/get/pedidos', ["POST","GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def pedidos (request): # token: Token):
    try:
        data = request.json

        if not 'pCliente' in data:
            data['pCliente'] = 'null'
            data['filter'] = '--'
        else:
            data['pCliente'] = "'"+data['pCliente']+"'"
            data['filter'] = ''

        db = get_oracle_db()
        c = db.cursor()

        query = """SELECT COD_CIA, GRUPO_CLIENTE,
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
                            """.format(filter = data['filter'], pCliente = data['pCliente'])
        #print(query)
        c.execute(query)
        list = []
        for row in c:
            aux = {}
            aux = {
                    'no_cia': row[0],
                    'grupo': row[1],
                    'no_cliente': row[2],
                    'fecha': row[3],
                    'no_factu': row[4],
                    # 'no_arti':row[4],
                    'observacion': row[5],
                    'estatus': row[6],
                    'precio': row[7],
                    'cantidad': row[8],
                    'ID': row[9],
                    'estatus_id': row[10],
                    'fecha_estatus': row[11]

                }
            list.append(aux)


        return response.json({"data": list},200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)


@app.route('/get/pedidosV2', ["POST","GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def pedidosV2 (request): # token: Token):
    try:
        data = request.json

        if not 'pNoCia' in data:
            data['pNoCia'] = '01'
        # else:
        #     data['pNoCia'] = "'"+data['pNoCia']+"'"

        if not 'pNoGrupo' in data:
            data['pNoGrupo'] = '01'
        # else:
        #     data['pNoGrupo'] = "'"+data['pNoGrupo']+"'"

        if not 'pCliente' in data:
            data['pCliente'] = 'null'
        # else:
        #     data['pCliente'] = "'"+data['pCliente']+"'"
        db = get_oracle_db()

        list = await procedure_pedidos(db, data['pNoCia'], data['pNoGrupo'],data['pCliente'])

        pool.release(db)
        return response.json({"data": list},200)

    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)


async def procedure_detalle_pedidos(db,idPedido):

        c = db.cursor()

        l_cur = c.var(cx_Oracle.CURSOR)
        l_result = c.callproc("""PROCESOSPW.detalle_pedidos_cargados""",[l_cur, idPedido])[0]

        list = []

        for arr in l_result:
            # #print(arr)
            aux = {}
            aux = {
                'id_pedido': arr[0],
                  'COD_PRODUCTO': arr[1],
                  'nombre_producto': arr[2],
                  'princ_activo': arr[3],
                  'CANTIDAD': arr[4],
                  'precio_bruto_bs': arr[5],
                  'precio_bruto_usd': arr[6],
                  'precio_neto_bs': arr[7],
                  'PRECIO': arr[5],
                  'iva_bs': arr[8],
                  'precio_neto_usd': arr[9],
                  'iva_usd': arr[10],
                  'fecha_vence': arr[11],
                  'tipo_prod_emp': arr[12]

                }
            list.append(aux)

        return list

async def procedure_pedidos(db, cia, grupo,cliente):
    try:

        c = db.cursor()

        l_cur = c.var(cx_Oracle.CURSOR)
        l_result = c.callproc("""PROCESOSPW.pedidos_cargados""",[l_cur,cia,grupo,cliente])[0]
        list = []

        for arr in l_result:

            aux = {
                'ID': arr[0],
                  'nombre_cliente': arr[1],
                  'direccion_cliente': arr[2],
                  'fecha_creacion':  dateByResponse(arr[3]),
                  'cod_estatus': arr[4],
                  'estatus': arr[5],
                  'fecha_estatus': dateByResponse(arr[6]),
                  'tipo_pedido': arr[7]

                }
            list.append(aux)


        return list
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)


@app.route('/get/pedido', ["POST","GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def pedido (request): # token: Token):
# async def pedido (request):
    try:
        data = request.json

        if not 'idPedido' in data or data['idPedido'] == 0:
            return response.json({"msg": "Missing ID parameter"}, status=400)

        # mongodb = get_mysql_db()

        db = get_oracle_db()
        c = db.cursor()

        pedidos = await procedure_detalle_pedidos(db, int(data['idPedido']))
        totales = await totales_pedido(db, int(data['idPedido']))
        errores = await log_errores(db, int(data['idPedido']))
        query = """SELECT
                         COD_CIA, GRUPO_CLIENTE,
                        COD_CLIENTE, TO_CHAR(FECHA_CARGA, 'DD-MM-YYYY'), NO_PEDIDO_CODISA,
                        OBSERVACIONES, t2.descripcion, ESTATUS, TIPO_PEDIDO
                        FROM PAGINAWEB.PEDIDO t1
                        join PAGINAWEB.ESTATUS t2
                            on t1.ESTATUS = t2.CODIGO
                        WHERE ID = {idPedido}
                            """.format(idPedido = data['idPedido'] )
        c.execute(query)

        list = []
        for row in c:
            aux = {
                    'no_cia': row[0],
                    'grupo': row[1],
                    'no_cliente': row[2],
                    'fecha': row[3],
                    'no_factu': row[4],
                    'observacion': row[5],
                    'estatus': row[6],
                    'estatus_id': row[7],
                    'tipo_pedido': row[8],
                    'pedido': pedidos,
                    'errores': errores,
                    'totales': totales,
                }

            list.append(aux)

        return response.json({"msj": "OK", "obj": list}, 200)
    except Exception as e:
        logger.debug(e)
        return response.json("ERROR", 400)


async def log_errores(db,idPedido):
    try:

        # db = get_oracle_db()
        c = db.cursor()

        c.execute("""SELECT
                         COD_PRODUCTO, TO_CHAR(FECHA, 'DD-MM-YYYY'),
                           t2.DESCRIPCION
                        FROM PAGINAWEB.REGISTRO_ERROR t1
                        JOIN TIPO_ERROR t2 on t1.COD_ERROR = t2.CODIGO
                        WHERE t1.ID_PEDIDO = {idPedido}
                        """.format(idPedido = idPedido ))
        list = []
        for row in c:
            aux = {}
            aux = {
                    'COD_PRODUCTO': row[0],
                    'FECHA': row[1],
                    'DESCRIPCION': row[2]
                }

            list.append(aux)

        return list
    except Exception as e:
        logger.debug(e)
        return e


@app.route('/get/categorias', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
#@jwt_required
async def filtros(request):#, token:Token):
    try:

        db = get_oracle_db()
        c = db.cursor()

        c.execute("""SELECT
                    CODIGO, NOMBRE
                    FROM PAGINAWEB.FILTRO_CATEGORIA_PRODUCTO
                        """)
        list = []
        for row in c:
            aux = {}
            aux = {
                    'CODIGO': row[0],
                    'NOMBRE': row[1]
                }

            list.append(aux)

        return response.json({"msj": "OK", "obj": list}, 200)
    except Exception as e:
        logger.debug(e)
        return e


@app.route('/get/proveedores', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
async def procedure_prove(request):

    db = get_oracle_db()
    c = db.cursor()

    l_cur = c.var(cx_Oracle.CURSOR)
    l_result = c.callproc("""PROCESOSPW.proveedores""",[
        l_cur
        ])[0]
    list = []
    for arr in l_result:
        obj = {
            'cod_proveedor' : arr[0],
            'nombre_proveedor': arr[1]
            }
        list.append(obj)

    pool.release(db)

    return response.json({"msj": "OK", "obj": list}, 200)


@app.route('/totales_pedido', ["POST", "GET"])
# @compress.compress
@doc.exclude(True)
# #@jwt_required
# async def totales(request): # token: Token):
# #@jwt_required
async def totales(request):

    data = request.json
    db = get_oracle_db()
    list = await totales_pedido(db, int(data['idPedido']))
    pool.release(db)
    return response.json({"msj": "OK", "totales": list}, 200)


async def totales_pedido(db, idPedido):

    try:
        # db = get_oracle_db()
        c = db.cursor()

        total_bruto=c.var(int)
        desc_volumen=c.var(int)
        otros_descuentos=c.var(int)
        desc_adicional=c.var(int)
        desc_dpp=c.var(int)
        sub_total=c.var(int)
        impuesto=c.var(int)
        total=c.var(int)
        totalGravado=c.var(int)
        totalExento =c.var(int)
        descImpuesto=c.var(str)
        totalNetoUSD=c.var(int)
        tipoCambio =c.var(int)
        descPreEmpaque =c.var(int)
        procVol =c.var(int)
        procPP =c.var(int)
    
        l_result = c.callproc("""PROCESOSPW.totales_pedido""",[
            int(idPedido),
            total_bruto,
            desc_volumen,
            otros_descuentos,
            desc_adicional,
            desc_dpp,
            sub_total,
            impuesto,
            total,
            totalGravado,
            totalExento ,
            descImpuesto,
            totalNetoUSD,
            tipoCambio,
            descPreEmpaque,
            procVol,
            procPP
            ])[0]

        obj = {
                'total_bruto':total_bruto.getvalue(),
                'desc_volumen':desc_volumen.getvalue(),
                'otros_descuentos':otros_descuentos.getvalue(),
                'desc_adicional':desc_adicional.getvalue(),
                'desc_dpp':desc_dpp.getvalue(),
                'sub_total':sub_total.getvalue(),
                'impuesto':impuesto.getvalue(),
                'total':total.getvalue(),
                'totalGravado':totalGravado.getvalue(),
                'totalExento':totalExento.getvalue(),
                'descImpuesto':descImpuesto.getvalue(),
                'totalNetoUSD':totalNetoUSD.getvalue(),
                'tipoCambio':tipoCambio.getvalue(),
                'descPreEmpaque': descPreEmpaque.getvalue(),
                'procVol': procVol.getvalue(),
                'procPP': procPP.getvalue()
        }

        return obj
    except Exception as e:
        logger.debug(e)
        return e


async def logAudit(user, module, accion, context):

    db = get_mysql_db()

    log = dict(
        username= user,
        module= module,
        accion= accion,
        context= context,
    )

    await db.audit.insert_one(log)

    return

def dateByResponse(value):
    if isinstance(value, (datetime, date)):
        return value.strftime("%d/%m/%Y")
    else:
        return value

def formatFloatDdo(value):

    if len(value) > 0:
    	x = value.replace(",", ".")
    	x = float(x)
    else:
    	x = float(0)

    return x

ssl = {'cert': 'conf/ssl.crt/server.crt', 'key': 'conf/ssl.key/sever.key'}
app.run(host='0.0.0.0', port= port, debug = True, workers=workers)
# app.run(host='0.0.0.0', port = port, debug = False, ssl=ssl)
