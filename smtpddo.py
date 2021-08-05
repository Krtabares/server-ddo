
import asyncio
import requests
from sanic import Sanic
from sanic import request
from sanic.response import html, json
# import aiosmtplib
from random import randint
from sanic_jinja2 import SanicJinja2
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from motor.motor_asyncio import AsyncIOMotorClient
from email.utils import formataddr
from datetime import datetime
from sanic import response
#!/usr/bin/env python3

import ssl,smtplib

from email.message import EmailMessage
from email.headerregistry import Address
from email.utils import make_msgid

from sanic.handlers import ErrorHandler
app = Sanic(__name__)
jinja = SanicJinja2(app)

# class CustomHandler(ErrorHandler):
#     def default(self, request, exception):
#         # print("[EXCEPTION] "+str(exception))
#         return response.json(str(exception),501)
# handler = CustomHandler()
# app.error_handler = handler

def get_mongo_db():
    mongo_uri = "mongodb://127.0.0.1:27017/ddo"
    client = AsyncIOMotorClient(mongo_uri)
    db = client['ddo']
    return db

@app.route('/put', methods=["POST"])
async def check(request):
    data = request.json
    print(data)
    # await user_pass(data['user'], data["password"])
    if 'newpass' in data:
        await _send_email2(data, data["newpass"])
    elif 'user' in data:
        await _send_email3(data, data["user"])

    return json(f"SUCCESS!")




async def _send_email2(data, mypass):
    sender_email = "ddoesteinfo@gmail.com"
    receiver_email = data.get("to", None)
    message_type = data.get("type", None)
    password = "Caracas2020$"
    # mypass = "ddo.2017"
    message = MIMEMultipart("alternative")
    message["Subject"] = data.get("subject", None)
    message["From"] = sender_email
    message["To"] = receiver_email

    print(message)
    # Create the plain-text and HTML version of your message
    text = """\
    Hi,
    How are you?
    Real Python has many great tutorials:
    www.realpython.com"""

    html = """
        <!doctype html>
        <html lang="en-US">

        <head>
            <meta content="text/html; charset=utf-8" http-equiv="Content-Type" />
            <title>Reset Password Email Template</title>
            <meta name="description" content="Reset Password Email Template.">
            <style type="text/css">
                a:hover {text-decoration: underline !important;}
            </style>
        </head>

        <body marginheight="0" topmargin="0" marginwidth="0" style="margin: 0px; background-color: #f2f3f8;" leftmargin="0">

            <table cellspacing="0" border="0" cellpadding="0" width="100%" bgcolor="#f2f3f8"
                style="@import url(https://fonts.googleapis.com/css?family=Rubik:300,400,500,700|Open+Sans:300,400,600,700); font-family: 'Open Sans', sans-serif;">
                <tr>
                    <td>
                        <table style="background-color: #f2f3f8; max-width:670px;  margin:0 auto;" width="100%" border="0"
                            align="center" cellpadding="0" cellspacing="0">
                            <tr>
                                <td style="height:80px;">&nbsp;</td>
                            </tr>
                            <tr>
                                <td style="text-align:center;">
                                <a href="https://rakeshmandal.com" title="logo" target="_blank">
                                    <img src="http://www.del-oeste.com/wp-content/uploads/2017/11/drogueria-del-oeste-bur-sin-rif-v3.png" title="logo" alt="logo">
                                </a>
                                </td>
                            </tr>
                            <tr>
                                <td style="height:20px;">&nbsp;</td>
                            </tr>
                            <tr>
                                <td>
                                    <table width="95%" border="0" align="center" cellpadding="0" cellspacing="0"
                                        style="max-width:670px;background:#fff; border-radius:3px; text-align:center;-webkit-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);-moz-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);box-shadow:0 6px 18px 0 rgba(0,0,0,.06);">
                                        <tr>
                                            <td style="height:40px;">&nbsp;</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:0 35px;">
                                                <h1 style="color:#1e1e2d; font-weight:500; margin:0;font-size:32px;font-family:'Rubik',sans-serif;">
                                                    Ha solicitado restablecer su contraseña</h1>
                                                <span
                                                    style="display:inline-block; vertical-align:middle; margin:29px 0 26px; border-bottom:1px solid #cecece; width:100px;"></span>
                                                <p style="color:#455056; font-size:15px;line-height:24px; margin:0;">

                                                    No podemos simplemente enviarle su contraseña anterior. Se ha generado una contraseña para usted. Su nueva contraseña de inicio de sesión es la siguiente:
                                                </p>
                                                <a href="javascript:void(0);"
                                                    style="background:#20e277;text-decoration:none !important; font-weight:500; margin-top:35px; color:#fff; font-size:14px;padding:10px 24px;display:inline-block;border-radius:50px;">
                                                    """ + mypass + """</a>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td style="height:40px;">&nbsp;</td>
                                        </tr>
                                    </table>
                                </td>
                            <tr>
                                <td style="height:20px;">&nbsp;</td>
                            </tr>
                            <tr>
                                <td style="text-align:center;">
                                    <p style="font-size:14px; color:rgba(69, 80, 86, 0.7411764705882353); line-height:18px; margin:0 0 0;">&copy; <strong>http://www.del-oeste.com</strong></p>
                                </td>
                            </tr>
                            <tr>
                                <td style="height:80px;">&nbsp;</td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>

        </body>

        </html>
    """

    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    print(message_type)
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(
            sender_email, receiver_email, message.as_string()
        )
    return "SUCCESS"

async def _send_email3(data, username):
    sender_email = "ddoesteinfo@gmail.com"
    receiver_email = data.get("to", None)
    message_type = data.get("type", None)
    password = "Caracas2020$"
    # mypass = "ddo.2017"
    message = MIMEMultipart("alternative")
    message["Subject"] =  data.get("subject", None)
    message["From"] = sender_email
    message["To"] = receiver_email

    print(message)
    # Create the plain-text and HTML version of your message
    text = """\
    Hi,
    How are you?
    Real Python has many great tutorials:
    www.realpython.com"""

    htmlWelcome = """<p>&lt;!doctype html&gt;</p>
        <p></p>
        <table style="@import url(https: //fonts.googleapis.com/css?family=Rubik:300,400,500,700|Open+Sans:300,400,600,700); font-family: 'Open Sans', sans-serif;" border="0" width="100%" cellspacing="0" cellpadding="0" bgcolor="#f2f3f8">
        <tbody>
        <tr>
        <td>
        <table style="background-color: #f2f3f8; max-width: 670px; margin: 0 auto;" border="0" width="100%" cellspacing="0" cellpadding="0" align="center">
        <tbody>
        <tr>
        <td style="height: 80px;">&nbsp;</td>
        </tr>
        <tr>
        <td style="text-align: center;"><a title="logo" href="https://rakeshmandal.com" target="_blank"> <img title="logo" src="http://www.del-oeste.com/wp-content/uploads/2017/11/drogueria-del-oeste-bur-sin-rif-v3.png" alt="logo" /> </a></td>
        </tr>
        <tr>
        <td style="height: 20px;">&nbsp;</td>
        </tr>
        <tr>
        <td>
        <table style="max-width: 670px; background: #fff; border-radius: 3px; text-align: center; -webkit-box-shadow: 0 6px 18px 0 rgba(0,0,0,.06); -moz-box-shadow: 0 6px 18px 0 rgba(0,0,0,.06); box-shadow: 0 6px 18px 0 rgba(0,0,0,.06);" border="0" width="95%" cellspacing="0" cellpadding="0" align="center">
        <tbody>
        <tr>
        <td style="height: 40px;">&nbsp;</td>
        </tr>
        <tr>
        <td style="padding: 0 35px;">
        <h1 style="color: #1e1e2d; font-weight: 500; margin: 0; font-size: 32px; font-family: 'Rubik',sans-serif;">Bienvenido</h1>
        <p style="color: #455056; font-size: 15px; line-height: 24px; margin: 0;">Te damos la bienvenida al sistema de Droguería del Oeste. A continuación te mostramos tus credenciales de acceso al sistema.</p>
        <a style="background: #20e277; text-decoration: none !important; font-weight: 500; margin-top: 35px; color: #fff; font-size: 14px; padding: 10px 24px; display: inline-block; border-radius: 50px;">Nombre de usuario: <strong> """ + username + """</strong></a>
        <a style="background: #20e277; text-decoration: none !important; font-weight: 500; margin-top: 35px; color: #fff; font-size: 14px; padding: 10px 24px; display: inline-block; border-radius: 50px;">Contraseña:  <strong>ddo.2017</strong></a>
          <p style="color: red; font-size: 15px; line-height: 24px; margin-top: 5;">Nota:Te recordamos que una vez hayas iniciado sesión, debes cambiar tu contraseña directamente en la opción de Perfil.</p>
        </td>

        </tr>
        <tr>
        <td style="height: 40px;">&nbsp;</td>
        </tr>
        </tbody>
        </table>
        </td>
        </tr>
        <tr>
        <td style="height: 20px;">&nbsp;</td>
        </tr>
        <tr>
        <td style="text-align: center;">
        <p style="font-size: 14px; color: rgba(69, 80, 86, 0.7411764705882353); line-height: 18px; margin: 0 0 0;">&copy; <strong>http://www.del-oeste.com</strong></p>
        </td>
        </tr>
        <tr>
        <td style="height: 80px;">&nbsp;</td>
        </tr>
        </tbody>
        </table>
        </td>
        </tr>
        </tbody>
        </table>"""

    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    print(message_type)
    part2 = MIMEText(htmlWelcome, "html")


    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(
            sender_email, receiver_email, message.as_string()
        )
    return "SUCCESS"

app.run(port=8888, debug=True, auto_reload=True)
