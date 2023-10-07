"""
*********************************************************************************************************************************************
Author  : Hedi FEZAI
Date    : 20231005
Desc    : Squelette pour initier un developpement quelconque. Gère les fichiers logs, l'envoi de mails logs HTML et  les connexions MSSQL.
*********************************************************************************************************************************************
"""


from os import path, rename, makedirs, environ
from datetime import datetime
from email.mime.text import MIMEText as text
from email.mime.multipart import MIMEMultipart
import smtplib, ssl
import sqlalchemy as sa
import pandas as pd

# Additional Libraries


def initLogFile():
    # création du dossier et recyclage des fichiers logs. renvoie le chemin du fichier log
    if not path.isdir(LogItems['logFolder']):
        makedirs(LogItems['logFolder'])
    old_log_file=''
    log_file = LogItems['logFolder'] + '/' + LogItems['filePrefix'] + '.txt'
    log_sizeKB = LogItems['MaxFileSizeKB']
    if path.exists(log_file):
        if datetime.now().strftime('%Y%m%d') != datetime.fromtimestamp(path.getctime(log_file)).strftime('%Y%m%d') or path.getsize(log_file)/1024 > log_sizeKB:
            old_log_file = LogItems['logFolder'] + '/' + LogItems['filePrefix'] + '_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.txt'
            rename(log_file, old_log_file)
    with open(log_file,'a',encoding = 'utf-8') as f:
        f.write ('\n')
    return log_file

def logToFile (log_file, level = 1, isError = False, message = ''):
    # fonction qui gère l'écriture dans les fichiers logs. Level : gère l'indentation dans les logs pour plus de lisibilité
    fileSep = level * '\t'
    htmlSep = level * '&emsp;'
    with open(log_file,'a',encoding = 'utf-8') as f:
        if isError:
            f.write  (datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' |E|' +  fileSep + message + '\n')
            log_list.append ('<font color="red">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + htmlSep + message +'</font>')
        else:
            f.write  (datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' | |' +  fileSep + message + '\n')
            log_list.append ('<font color="black">' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + htmlSep + message +'</font>')

def sendMail (hasError = False, useTLS = False):
    if MailItems['status'] == 0:
        sendMail = False
    else:
        if MailItems['level'] == 'info':
            sendMail = True
        elif MailItems['level'] == 'error' and hasError == True:
            sendMail = True
        else:
            sendMail = False
    if sendMail == True:
        port = MailItems['port']
        smtp_server = MailItems['smtp_server']
        sender_email = MailItems['sender_email']
        receiver_email = MailItems['receiver_email']
        login_email = MailItems['login_email']
        m = MIMEMultipart()
        try:
            password = MailItems['password']
        except Exception as pwd:
            logToFile (logfile, 1, True, 'Error decrypting SMTP password : ' + str(pwd))
        if hasError == False:
            prefix_subject = '[INF]'
        else:
            prefix_subject = '[ERR]'
            m['X-Priority'] = '2'
        message= """
                <html>
                    <body>
                        <style type='text/css'>
                            .tg  {border-collapse:collapse;border-spacing:0;}
                            .tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px; overflow:hidden;padding:3px 10px;word-break:normal;}
                            .tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:3px 10px;word-break:normal;}
                            .tg .tg-eqdz{font-family:'Lucida Sans Unicode', 'Lucida Grande', sans-serif !important;font-size:12px;font-weight:bold;text-align:center;vertical-align:middle}
                            .tg .tg-j7mi{font-family:'Lucida Sans Unicode', 'Lucida Grande', sans-serif !important;font-size:12px;text-align:center;vertical-align:middle}
                            .tg .tg-vwq0{font-family:'Lucida Sans Unicode', 'Lucida Grande', sans-serif !important;font-size:12px;text-align:left;vertical-align:middle}
                        </style>
                        <table class='tg'>
                            <thead>
                                <tr>
                                    <th class='tg-eqdz'>DateTime</th>
                                    <th class='tg-eqdz'>Text</th>
                                </tr>
                            </thead>
                            <tbody>
        """
        for lig in log_list[1:]:
            message += '<tr>' + '<td class="tg-j7mi">' + lig[:lig.find('20')+19] +'</td><td class="tg-vwq0">' + lig[lig.find('20')+19:] +'</th>' +'</tr>'
        message = message + '</table></body></html>'
        m['Subject'] = prefix_subject + '[' + environ['COMPUTERNAME'] + '] ' + programName + ' ' + programSubCategory
        m['From'] = sender_email
        m['To'] = ' ,'.join(receiver_email)

        m.attach(text(message, 'html'))
        try:
            context = ssl.create_default_context()
            if useTLS:
                with smtplib.SMTP(smtp_server, port) as server:
                    server.ehlo()
                    server.starttls(context=context)
                    server.ehlo()
                    server.login(login_email, password)
                    server.sendmail(sender_email, receiver_email, m.as_string())
                    server.quit()
                    logToFile (logfile, level = 1, isError = False, message = 'Email Notification sent successfully to : ' + ', '.join(receiver_email))
            else:
                with smtplib.SMTP_SSL(smtp_server, port) as server:
                    server.ehlo()
                    server.login(login_email, password)
                    server.sendmail(sender_email, receiver_email, m.as_string())
                    server.quit()
                    logToFile (logfile, level = 1, isError = False, message = 'Email Notification sent successfully to : ' + ', '.join(receiver_email))
        except Exception as sendErr:
            logToFile (logfile, level = 1, isError = True, message = 'Failed to send Email Notification : ' + str(sendErr))
class DatabaseConnector:
    def __init__(self, server):
        self.server = server
    def create_connection(self):
        engine = sa.create_engine(f"mssql+pyodbc://{self.server},1433/master?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server&encrypt=no", echo = False)
        connection = engine.connect()
        return connection
        # Usage :
            # cnx = DatabaseConnector("SERVERNAME").create_connection()
            # query = f"SELECT  * FROM [DATABASE].[schema].[Table]"
            # df = pd.read_sql_query(query, cnx,dtype=str)

#########################################################################################################

programCategory     = 'Projet X'
programSubCategory  = 'Phase Y'
programName         = 'SquelettePython'

MailItems = {
            'smtp_server'   :   'smtp.server.com',                  # Servur SMTP
            'port'          :   465,                                # Port SMTP
            'useTLS'        :   False,                              # TLS True or False
            'sender_email'  :   'myaddress@mail.com',               # Emetteur
            'receiver_email':   ['hisaddress@mail.com'],            # Destinataire du mail
            'login_email'   :   'myaddress@mail.com',               # Login SMTP
            'password'      :   'password',                         # Mot de Passe SMTP
            'level'         :   'info',            #info/error      # info : mails envoyés après chaque exécution, erreur : mails envoyés en cas d'erreur (l'objet sera modifié en si présence d'erreur)
            'status'        :   1
}

LogItems = {
            'logFolder'     :   'D:/Logs/' + programCategory + '/' + programSubCategory + '/' + programName,
            'filePrefix'    :   programName,                        # Préfixe pour les fichiers Logs
            'MaxFileSizeKB' :   512                                 # Taille limite souhaitée d'un fichier log
}
log_list=[] # Pour le mail en HTML
if __name__ == '__main__':
    logfile = initLogFile()
    hasError = False
    logToFile(logfile, level = 1, isError = False, message = '########################################## BEGIN CYCLE ##########################################')
    # Start Work Here




    # End Work Here
    sendMail(hasError, useTLS = MailItems['useTLS'])
    logToFile(logfile, level = 1, isError = False, message = '########################################### END CYCLE ###########################################')
