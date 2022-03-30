# -*- coding: utf-8 -*-
import hashlib
import os


        

with open('usuarios.csv', 'r') as file1:
    array = file1.readlines()
    array = [row.split('\n') for row in array]

file = open("insert usuarios.txt", "w")

for line in array:
    value = line[0].strip().split(';')
    password = hashlib.md5(value[2].encode())
    # print(password.hexdigest())
    # print(value)
    sql = """INSERT INTO usuarios
            (
            role,
            name,
            email,
            username,
            password,
            COD_CIA,
            GRUPO_CLIENTE,
            COD_CLIENTE,
            estatus,
            permisos
            )
            VALUES
            (
            \'{role}\',
            \'{name}\',
            \'{email}\',
            \'{username}\',
            \'{password}\',
            \'{COD_CIA}\',
            \'{GRUPO_CLIENTE}\',
            \'{COD_CLIENTE}\',
            \'{estatus}\',
            \'{permisos}\'
            );
        """.format(
            role = 'generic',
            name = value[1],
            email = "noemail@email.com",
            username = value[0],
            password = password.hexdigest(),
            COD_CIA = "01",
            GRUPO_CLIENTE = "01",
            COD_CLIENTE = value[0],
            permisos = """{"pedido": {"ver": true, "crear": true, "editar": true, "eliminar": false},"perfil": {"ver": true}}""",
            estatus = "Activo"
            )
    file.write(sql + os.linesep)
file.close()
            
    