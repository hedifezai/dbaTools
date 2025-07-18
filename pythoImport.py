"""
=============================================================================
Project Name        : PythoImport Version 6.6
Created by          : Hedi FEZAI
Date Creation       : 2022-11-10
Date Modification   : 2025-07-15
-----------------------------------------------------------------------------
Changelog :

V1.0 Initial Build (2022-11-10)
	v   Added Support For Mailing
	v   Added Log Cycling
	v   Added Support for Dynamic SQL Table Creation
	v   Added lookup function for files matching mask inside Zip Files
	v   Added Support for network shared folders
	v   Improved logging

V2.0 (2022-11-14)
	v   Added support for local SQL server testing
	v   Added Exec SP after Importing File and catch Return Value
	v   Added automatic switch for mail profile on FRPARDMT01
	v   Corrected a bug in treating complex FileMasks
	v   Added support for Recursive Folders Creation
	v   Switched to HTML mails with Errors colors in Red and text formatted into a table

V3.0 (2022-11-15)
	v   Added Dynamic Mask Support fo Files
	v   Added Dynamic Mask Support for Zips
	v   Added Truncate table before import when Fixed Mode is enabled and we need to keep Table Structure
	v   Added Recursive Folders search in Zips for Files matching Mask

V4.0 (2022-12-08)
	v   Added OwnCloud Support
	v   Added Support for selecting specific columns from source file and using other names for them
	v   Bug Fixes for SP Exec and moving files to errorFolder
	v   Minor Improvements in logging process
	v   Added possiblity to set column types default for df_to_sql table creation
	v   Enhanced Visual of Errors in Logging File
	v   Added ability to override Pandas field detection and force all fields to NVARCHAR with a predefined lenght instead of MAX
	v   Added mutiple receivers in Mail Notification
	v   Ignore Error if Truncate doesn't find the table. Table will be created
	v   Added multiMask multiTable multiSP for the same Import
	v   Bug Fixes and Improved Visuals
V4.1 (2023-07-05)
	v ignore Mail if no file has been processed (download or import)
	v corrected a bug with filehandle not closing in time
V4.2 (2023-07-18)
	v Ignoring quotechar = None
V4.3 (2023-11-05)
	v Replacing spaces in tablenames with underscores
V4.4 (2023-11-08)
	v RetroCompatibility with older versions of settings.py
	v Usage of path.join instead of joining manually paths and filenames
V5.0 (2023-11-09)
    v Rebuilt entirely using the new skeleton file (optimizing file logging and log recycling + optimizing pyodbc and sqlalchemy connexions)
    v FileMask, SQLTable and Stored Procedures can be either single value each or a list each having the same number of elements
    v Automatic detection of SQL Drivers installed and using the latest one automatically for pyodbc and sqlalchemy
    v This version is fully compatible with all older versions of PythoImport and Settings files
V5.1 (2023-12-09)
    v Passwords are now encrtypted with Fernet from Cryptograpghy Module
    v Remove line from dataFrame when a line has no value on a defined column
v5.2 (2024-02-14)
    v Bug fix mail subject not switching to ERR in some error cases
    v RetroCompatibility with older versions of settings.py password Encryptions
    v Add mail switch "action" that sends mail only if there were action while executing script
    v Add supprot for json files
v5.3 (2024-02-24)
    v Add support for recursive file list in sftp based on list of remote paths
    v Apply retention to [LogItems['logFolder'], TsfItems['archiveFolder'], TsfItems['errorFolder'], TsfItems['localFolder']]
    v Bug fix mail subject not switching to ERR in some error cases
v5.4 (2024-03-08)
    v Add missing columns automatically to destination SQL table
    v Changed mail subject to Warning when this is the case
v5.5 (2024-04-01)
    v Corrected bug from v5.3 related to folder retention variable not set correctly
    v Corrected bug relative to localfolder not liking being a list
    v Corrected bug in truncate table when table has spaces in it's name
v6.0 (2024-04-16)
    v Add option for using BCP command in order to transfer Data to SQL table faster using BulkInsert
    v Various optimizations relatied to using BCP if some columns are not present in one side or the other
v6.1 (2024-04-27)
    v Bug fix when trying to truncate or drop inexistant table
v6.2 (2024-10-01)
    v Bug fix with mask sent to SP not changing when having multiple imports in the same job
    v Add .lower() to all control variables coming from settings
    v Add support for custom encoding and separator for BCP file
v6.3 (2024-11-09)
    v Correct Bug when BCP runs into an error and file is moved to Processed Folder instead of InError Folder
    v Removing print spResult when procedure returns 0
v6.4 (2025-04-15)
    v Add support for FTP servers (Port 21)
v6.5 (2025-05-07)
    v Exclude folders from automatic retention
v6.6 (2025-07-16)
    v Fixed bug with fnmatch cheking absolute path instead of filename
WishList
	ToDo :
	?   Add Support for TarBalls (tar, tar.gz, tgz)
	?   Add API Support
	?   Add SQL/MySQL as Source
	?   Add Export to SFTP
	?   Add Export to MAIL
"""
import warnings
warnings.simplefilter("ignore", category=UserWarning)
from os import path, rename, makedirs, listdir, remove, environ, getenv
from datetime import datetime, timedelta
from email.mime.text import MIMEText as text
from email.mime.multipart import MIMEMultipart
from cryptography.fernet import Fernet
import smtplib, ssl
import sqlalchemy as sa
import pyodbc
import pandas as pd
import base64
import json
import shutil
import pysftp
import fnmatch
import zipfile
import subprocess
import csv
import ftplib
from settings import *

def initLogFile():
    # création du dossier et recyclage des fichiers logs. renvoie le chemin du fichier log
    if not path.isdir(LogItems['logFolder']):
        makedirs(LogItems['logFolder'])
    old_log_file=''
    log_file = LogItems['logFolder'] + '/' + LogItems['filePrefix'] + '.txt'
    log_sizeKB = LogItems['MaxFileSizeKB']
    if 'retentionDays' not in LogItems:
        retentionDays = 30
    else:
        retentionDays = LogItems['retentionDays']

    # Creations des dossiers de travail s'ils n'existent pas
    if not path.isdir(rootFolder):
        makedirs(rootFolder)
    if not path.isdir(LogItems['logFolder']):
        makedirs(LogItems['logFolder'])
    if not path.isdir(TsfItems['localFolder']):
        makedirs(TsfItems['localFolder'])
    if not path.isdir(TsfItems['archiveFolder']):
        makedirs(TsfItems['archiveFolder'])
    if not path.isdir(TsfItems['errorFolder']):
        makedirs(TsfItems['errorFolder'])

    # traitement des rétentions sur les fichier de logs et les fichiers de données
    for folder in [LogItems['logFolder'], TsfItems['archiveFolder'], TsfItems['errorFolder'], TsfItems['localFolder']]:
        listFiles = [filename for filename in listdir(folder) if int(datetime.fromtimestamp(path.getctime(folder + '/' +filename)).date().strftime('%Y%m%d')) < int((datetime.now().date() - timedelta(days = retentionDays)).strftime('%Y%m%d'))]
        for filename in listFiles:
            if path.isfile(filename):
                remove(folder + '/' + filename)
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
        if MailItems['level'].lower() == 'info':
            sendMail = True
        elif MailItems['level'].lower() == 'error' and hasError == True:
            sendMail = True
        elif MailItems['level'].lower() == 'action' and (hasAction == True or hasError == True):
            sendMail = True
        else:
            sendMail = False
    prefix_subject = ""
    if sendMail == True:
        port = MailItems['port']
        smtp_server = MailItems['smtp_server']
        sender_email = MailItems['sender_email']
        receiver_email = MailItems['receiver_email']
        login_email = MailItems['login_email']
        m = MIMEMultipart()
        try:
            password = Fernet(getenv('FERNET_KEY')).decrypt(MailItems['password']).decode()
        except:
            try:
                password = base64.b64decode(MailItems['password'].encode("ascii")).decode("ascii").replace('\n', '')
            except Exception as pwd:
                logToFile (logfile, 1, True, 'Error decrypting SMTP password : ' + str(pwd))
        if hasError == False:
            if hasWarn == False:
                prefix_subject = '[INF]'
            else:
                prefix_subject = '[WRN]'
                m['X-Priority'] = '2'
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
        m['Subject'] = prefix_subject + '[' + environ["COMPUTERNAME"] + ']'+ 'PythoImport ' + piItems['Client'] + '_' + piItems['Campagne']
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
                    logToFile (logfile, level = 1, isError = False, message = prefix_subject + ' Email Notification sent successfully to : ' + ', '.join(receiver_email))
            else:
                with smtplib.SMTP_SSL(smtp_server, port) as server:
                    server.ehlo()
                    server.login(login_email, password)
                    server.sendmail(sender_email, receiver_email, m.as_string())
                    server.quit()
                    logToFile (logfile, level = 1, isError = False, message = prefix_subject + ' Email Notification sent successfully to : ' + ', '.join(receiver_email))
        except Exception as sendErr:
            logToFile (logfile, level = 1, isError = True, message = prefix_subject + ' Failed to send Email Notification : ' + str(sendErr))
    elif MailItems['status'] == 1:
        logToFile (logfile, level = 1, isError = False, message = prefix_subject + ' Email setting set to "' + MailItems['level'] + '" and there were none. Skipping...')

def getAllFiles(sftp, listRemotePath):
    if isinstance(listRemotePath,list) == False:
        listRemotePath = [listRemotePath]
    for remotePath in listRemotePath:
        for item in sftp.listdir_attr(remotePath):
            itemPath = remotePath.rstrip("/") + "/" + item.filename
            if sftp.isfile(itemPath):
                if itemPath not in listFiles:
                    listFiles.append(itemPath)
            elif sftp.isdir(itemPath) and TsfItems.get('recursiveMode') == True:
                getAllFiles(sftp, itemPath)
    return listFiles

class DatabaseEngine:
    def __init__(self, server, database):
        self.server = server
        self.database = database
    def create_connection(self):
        drivers = [driver for driver in pyodbc.drivers() if 'ODBC' in driver]
        driverSA = drivers[-1].replace (" ","+")
        engine = sa.create_engine(f"mssql+pyodbc://{self.server},1320/{self.database}?trusted_connection=yes&driver={driverSA}&encrypt=no", echo = False)
        connection = engine.connect()
        return connection

class DatabaseConnector:
    def __init__(self, server,database):
        self.server = server
        self.database = database
    def create_connection(self):
        drivers = [driver for driver in pyodbc.drivers() if 'ODBC' in driver]
        driverPO = "{" + drivers[-1] + "}"
        connection = pyodbc.connect(f"DRIVER={driverPO};SERVER={self.server},1320;initial_catalog={self.database};Trusted_Connection=yes;Encrypt=no;MARS_Connection=Yes;")
        return connection


if __name__ == '__main__':
    logfile = initLogFile()
    log_list=[] # Pour le mail en HTML
    hasError = False
    hasWarn = False
    logToFile(logfile, level = 1, isError = False, message = f"{' BEGIN CYCLE '.center(150, '#')}")

    # Start Work Here
    # Force Column Types/Sizes when creating tables whin Pandas and SQLAlchemy
    def sqlcol(dfparam):
        dtypedict = {}
        if TsfItems['forceAlltoNVARCHAR'] == False:
            for i,j in zip(dfparam.columns, dfparam.dtypes):
                if "object" in str(j):
                    dtypedict.update({i: sa.types.NVARCHAR(length=TsfItems['nvarcharLength'])})
                if "datetime" in str(j):
                    dtypedict.update({i: sa.types.DateTime()})
                if "float" in str(j):
                    dtypedict.update({i: sa.types.Float(precision=3, asdecimal=True)})
                if "int" in str(j):
                    dtypedict.update({i: sa.types.INT()})
        else:
            for i,j in zip(dfparam.columns, dfparam.dtypes):
                dtypedict.update({i: sa.types.NVARCHAR(length=TsfItems['nvarcharLength'])})

        return dtypedict

    hasErrorGlobal = False
    hasError = False
    hasAction = False
    sftpStatus=sftpItems.pop('status')

    if isinstance(TsfItems['remoteFolder'], list) == False:
        TsfItems['remoteFolder']=[TsfItems['remoteFolder']]
    if isinstance(TsfItems['fileMask'], list) == False:
        TsfItems['fileMask']=[TsfItems['fileMask']]
    if isinstance(SqlItems['sqlTable'], list) == False:
        SqlItems['sqlTable']=[SqlItems['sqlTable']]
    if isinstance(SqlItems['spExec'], list) == False:
        SqlItems['spExec']=[SqlItems['spExec']]


    if (len(TsfItems['fileMask']) == len(SqlItems['sqlTable']) == len(SqlItems['spExec'])) or (len(TsfItems['fileMask']) == len(SqlItems['spExec']) and SqlItems['sqlTableMode'].lower() == 'auto') :
        #Gestion des Masques des Fichiers à Télécharger
        listComputedMask=[]
        preFix = datetime.now().strftime("%Y%m%d_%H%M%S") + '_' if TsfItems.get('addTimeStamp') == True else ''
        lookupDay = datetime.today() + timedelta(days = TsfItems['lookUpDay'])
        computedZipMask = TsfItems['zipMask']
        lookupDay = datetime.today() + timedelta(days = TsfItems['lookUpDay'])
        computedZipMask = computedZipMask.replace('§yyyy§',lookupDay.strftime("%Y"))
        computedZipMask = computedZipMask.replace('§yy§',lookupDay.strftime("%y"))
        computedZipMask = computedZipMask.replace('§MM§',lookupDay.strftime("%m"))
        computedZipMask = computedZipMask.replace('§ww§',lookupDay.strftime("%W"))
        computedZipMask = computedZipMask.replace('§dd§',lookupDay.strftime("%d"))
        computedZipMask = computedZipMask.replace('§hh§',lookupDay.strftime("%H"))
        computedZipMask = computedZipMask.replace('§mm§',lookupDay.strftime("%M"))
        computedZipMask = computedZipMask.replace('§ss§',lookupDay.strftime("%S"))
        for index in range (len(TsfItems['fileMask'])):
            computedMask = TsfItems['fileMask'][index]
            computedMask = computedMask.replace('§yyyy§',lookupDay.strftime("%Y"))
            computedMask = computedMask.replace('§yy§',lookupDay.strftime("%y"))
            computedMask = computedMask.replace('§MM§',lookupDay.strftime("%m"))
            computedMask = computedMask.replace('§ww§',lookupDay.strftime("%W"))
            computedMask = computedMask.replace('§dd§',lookupDay.strftime("%d"))
            computedMask = computedMask.replace('§hh§',lookupDay.strftime("%H"))
            computedMask = computedMask.replace('§mm§',lookupDay.strftime("%M"))
            computedMask = computedMask.replace('§ss§',lookupDay.strftime("%S"))
            listComputedMask.append(computedMask)
        if sftpStatus == 'enabled' or sftpStatus == 1 or sftpStatus == '1':
            # Bloc SFTP
            try:
                sftp = None
                if sftpItems['port'] == 22:
                    cnopts = pysftp.CnOpts()
                    cnopts.hostkeys = None
                try:
                    sftpItems['password'] = Fernet(getenv('FERNET_KEY')).decrypt(sftpItems['password']).decode()
                except:
                    try:
                        sftpItems['password'] = base64.b64decode(sftpItems['password'].encode("ascii")).decode("ascii").replace('\n', '')
                    except Exception as pwd:
                        logToFile(logfile, level = 1, isError = False, message = f"Error decrypting SFTP password : {str(pwd)}")
                        hasError = True
                logToFile(logfile, level = 1, isError = False, message = "BEGIN transfert")
                if sftp:
                    sftp.close
                if sftpItems['port'] == 22:
                    sftp = pysftp.Connection(**sftpItems, cnopts = cnopts)
                else:
                    sftp = ftplib.FTP_TLS(timeout=10)
                    sftp.connect(sftpItems['host'], sftpItems['port'])
                    sftp.auth()
                    sftp.prot_p()
                    sftp.login(sftpItems['username'], sftpItems['password'])
                logToFile(logfile, level = 2, isError = False, message = 'Connected to : ' + sftpItems['host'] +' with Username : ' + sftpItems['username'])
            except Exception as cnx:
                logToFile(logfile, level = 2, isError = True, message =  str(cnx) + ' Server : ' + sftpItems['host'] +'. Username : ' + sftpItems['username'])
                hasError = True
            else:
                try:
                    logToFile(logfile, level = 2, isError = False, message = 'Accessing remote folders : ' + str(TsfItems['remoteFolder']))
                    listFiles = []
                    cleanlist = []
                    listZip = []
                    if sftpItems['port'] == 22:
                        sftp.cd('/')
                        listFiles=getAllFiles(sftp, TsfItems['remoteFolder'])
                    else:
                        sftp.cwd(TsfItems['remoteFolder'][0])
                        listFiles=sftp.nlst()
                    for index in range (len(listComputedMask)):
                        cleanlist += [file for file in listFiles if fnmatch.fnmatch(path.basename(file), listComputedMask[index])]
                    if TsfItems['lookForZip']:
                        listZip = [file for file in listFiles if fnmatch.fnmatch(path.basename(file), computedZipMask)]
                    logToFile(logfile, level = 2, isError = False, message = 'Total Files and Folders Found : ' + str(len(listFiles)))
                    logToFile(logfile, level = 2, isError = False, message = '-> Total Matching Files Found : ' + str(len(cleanlist) + len (listZip)))
                    if TsfItems['lookForZip']:
                        listZip = []
                        listZip = [file for file in listFiles if fnmatch.fnmatch(path.basename(file), computedZipMask)]
                        logToFile(logfile, level = 2, isError = False, message = 'ZipFile mask : ' + computedZipMask + '\t' + 'Files Found : ' + str(len(listZip)))
                        #Zip
                        for file in listZip:
                            cleanlistZip = []
                            try:
                                zipName = preFix + file.replace(':','')
                                logToFile(logfile, level = 2, isError = False, message = 'Downloading ' + file + ' as '+ zipName)
                                zipFile = path.join(TsfItems['localFolder'], zipName).replace("\\","/")
                                if sftpItems['port'] == 22:
                                    sftp.get (file, zipFile)
                                else:
                                    sftp.retrbinary('RETR %s' % file, open(zipFile, 'wb').write)
                                #Decompress Zip
                                logToFile(logfile, level = 2, isError = False, message =  'Decompressing ' + file)
                                with zipfile.ZipFile(zipFile, 'r') as myzip:
                                    listZip=myzip.infolist()
                                    for zipIndex in range(len(listComputedMask)):
                                        cleanlistZip += [zip_info for zip_info in listZip if (fnmatch.fnmatch(zip_info.filename, listComputedMask[zipIndex]) and zip_info.filename[-1] != '/')]
                                        logToFile(logfile, level = 2, isError = False, message = 'Current mask : ' + listComputedMask[zipIndex] + '\t' + 'Cumulative Files Found : ' + str(len(cleanlistZip)))
                                    for fileZipped in cleanlistZip:
                                        try:
                                            fileZipped.filename = path.basename(fileZipped.filename)
                                            myzip.extract(fileZipped,TsfItems['localFolder'])
                                            newFileZipped = preFix + fileZipped.filename
                                            shutil.move(path.join(TsfItems['localFolder'],fileZipped.filename).replace("\\","/"), path.join(TsfItems['localFolder'],newFileZipped).replace("\\","/"))
                                            logToFile(logfile, level = 3, isError = False, message = 'File extracted : ' + fileZipped.filename + ' as :' + newFileZipped)
                                        except Exception as extr:
                                            logToFile(logfile, level = 3, isError = True, message = 'Unable to extract ' + fileZipped.filename + ' from zip : ' + str(extr))
                                            hasError = True
                                try:
                                    shutil.move(zipFile,TsfItems['archiveFolder'])
                                except Exception as mvz:
                                    logToFile(logfile, level = 2, isError = True, message = 'Unable to move ' + zipFile + ' to archive : ' + str(mvz))
                                    hasError = True
                                if TsfItems['deleteAfter'] == True:
                                    try:
                                        if sftpItems['port'] == 22:
                                            sftp.remove (file)
                                        else:
                                            sftp.delete(file)
                                    except Exception as rmv:
                                        logToFile(logfile, level = 2, isError = True, message = 'Unable to delete file from SFTP : ' + str(rmv))
                                        hasError = True
                            except Exception as dwnld:
                                logToFile(logfile, level = 2, isError = True, message = 'An ERROR occured during Download : ' + str(dwnld))
                                hasError = True
                    for index in range (len(listComputedMask)):
                        cleanlist = []
                        hasError = False
                        cleanlist = [file for file in listFiles if fnmatch.fnmatch(path.basename(file), listComputedMask[index])]
                        logToFile(logfile, level = 2, isError = False, message = 'Current mask : ' + listComputedMask[index] + '\t' + 'Files Found : ' + str(len(cleanlist)))
                        for file in cleanlist:
                            try:
                                hasAction = True
                                fileName = preFix + path.basename(file.replace(':',''))
                                logToFile(logfile, level = 2, isError = False, message = 'Downloading ' + file + ' as '+ fileName)
                                if sftpItems['port'] == 22:
                                    sftp.get(file, TsfItems['localFolder'] + '/' + fileName)
                                else:
                                    with open(TsfItems['localFolder'] + '/' + fileName, 'wb') as f:
                                        sftp.retrbinary('RETR %s' % file, f.write)
                                if TsfItems['deleteAfter'] == True:
                                    try:
                                        if sftpItems['port'] == 22:
                                            sftp.remove (file)
                                        else:
                                            sftp.delete(file)
                                    except Exception as rmv:
                                        logToFile(logfile, level = 2, isError = True, message = 'Unable to delete file from SFTP : ' + str(rmv))
                                        hasError = True
                            except Exception as dwnld:
                                logToFile(logfile, level = 2, isError = True, message = 'An ERROR occured during Download : ' + str(dwnld))
                                hasError = True
                except Exception as rep:
                    logToFile(logfile, level = 2, isError = True, message = 'An ERROR occured during access to Path : ' + str(rep))
                    hasError = True
                logToFile(logfile, level = 1, isError = False, message = 'END Transfert')
                if sftp:
                    sftp.close
                    sftp = None
        elif owncldItems['status'] == 'enabled' or owncldItems['status']  == 1 or owncldItems['status']  == '1':
            # Bloc de transfert OwnCloud
            try:
                try:
                    owncldItems['password'] = Fernet(getenv('FERNET_KEY')).decrypt(owncldItems['password']).decode()
                except Exception as pwd:
                    logToFile(logfile, level = 2, isError = True, message = 'Error decrypting OwnCloud password : ' + str(pwd))
                    hasError = True
                logToFile(logfile, level = 1, isError = False, message = 'BEGIN transfert')
                oc = owncloud.Client(owncldItems['host'])
                oc.login(owncldItems['username'], owncldItems['password'])
                logToFile(logfile, level = 2, isError = False, message = 'Connected to : ' + owncldItems['host'] +' with Username : ' + owncldItems['username'])
            except Exception as cnx:
                logToFile(logfile, level = 2, isError = True, message = str(cnx) + ' Server : ' + owncldItems['host'] +'. Username : ' + owncldItems['username'])
                hasError = True
            else:
                try:
                    logToFile(logfile, level = 2, isError = False, message = 'Accessing remote folder : ' + TsfItems['remoteFolder'])
                    with oc.cd(TsfItems['remoteFolder']):
                        listFiles = []
                        cleanlist = []
                        listZip = []
                        listFiles = oc.list(TsfItems['remoteFolder'], depth = 1)
                        for index in range (len(listComputedMask)):
                            cleanlist += [file for file in listFiles if fnmatch.fnmatch(path.basename(file), listComputedMask[index])]
                        if TsfItems['lookForZip']:
                            listZip = [file for file in listFiles if fnmatch.fnmatch(path.basename(file), computedZipMask)]
                        logToFile(logfile, level = 2, isError = False, message = 'Total Files and Folders Found : ' + str(len(listFiles)))
                        logToFile(logfile, level = 1, isError = False, message = '->\t' +'Total Matching Files Found : ' + str(len(cleanlist) + len (listZip)))
                        if TsfItems['lookForZip']:
                            listZip = []
                            listZip = [file for file in listFiles if fnmatch.fnmatch(path.basename(file), computedZipMask)]
                            logToFile(logfile, level = 2, isError = False, message = 'ZipFile mask : ' + computedZipMask + '\t' + 'Files Found : ' + str(len(listZip)))
                            #Zip
                            for file in listZip:
                                cleanlistZip = []
                                try:
                                    zipName = preFix + file
                                    logToFile(logfile, level = 2, isError = False, message = 'Downloading ' + file + ' as '+ zipName)
                                    zipFile = path.join(TsfItems['localFolder'],zipName).replace("\\","/")
                                    oc.get_file (path.join(TsfItems['remoteFolder'],file).replace("\\","/"), zipFile)
                                    #Decompress Zip
                                    logToFile(logfile, level = 2, isError = False, message = 'Decompressing ' + file)
                                    with zipfile.ZipFile(zipFile, 'r') as myzip:
                                        listZip=myzip.infolist()
                                        for zipIndex in range(len(listComputedMask)):
                                            cleanlistZip += [zip_info for zip_info in listZip if (fnmatch.fnmatch(zip_info.filename, listComputedMask[zipIndex]) and zip_info.filename[-1] != '/')]
                                            logToFile(logfile, level = 2, isError = False, message = 'Current mask : ' + listComputedMask[zipIndex] + '\t' + 'Cumulative Files Found : ' + str(len(cleanlistZip)))
                                        for fileZipped in cleanlistZip:
                                            try:
                                                fileZipped.filename = path.basename(fileZipped.filename)
                                                myzip.extract(fileZipped,TsfItems['localFolder'])
                                                newFileZipped = preFix + fileZipped.filename
                                                shutil.move(path.join(TsfItems['localFolder'],fileZipped.filename).replace("\\","/"), path.join(TsfItems['localFolder'],newFileZipped).replace("\\","/"))
                                                logToFile(logfile, level = 3, isError = False, message = 'File extracted : ' + fileZipped.filename + ' as :' + newFileZipped)
                                            except Exception as extr:
                                                logToFile(logfile, level = 3, isError = True, message = 'Unable to extract ' + fileZipped.filename + ' from zip : ' + str(extr))
                                                hasError = True
                                    try:
                                        shutil.move(zipFile,TsfItems['archiveFolder'])
                                    except Exception as mvz:
                                        logToFile(logfile, level = 2, isError = True, message = 'Unable to move ' + zipFile + ' to archive : ' + str(mvz))
                                        hasError = True
                                    if TsfItems['deleteAfter'] == True:
                                        try:
                                            oc.delete (path.join(TsfItems['remoteFolder'],file).replace("\\","/"))
                                        except Exception as rmv:
                                            logToFile(logfile, level = 2, isError = True, message = 'Unable to delete file from SFTP : ' + str(rmv))
                                            hasError = True
                                except Exception as dwnld:
                                    logToFile(logfile, level = 2, isError = True, message = 'An ERROR occured during Download : ' + str(dwnld))
                                    hasError = True
                        for index in range (len(listComputedMask)):
                            cleanlist = []
                            hasError = False
                            cleanlist = [file for file in listFiles if fnmatch.fnmatch(path.basename(file), listComputedMask[index])]
                            logToFile(logfile, level = 2, isError = False, message = 'Current mask : ' + listComputedMask[index] + '\t' + 'Files Found : ' + str(len(cleanlist)))
                            for file in cleanlist:
                                try:
                                    hasAction = True
                                    fileName = preFix + file
                                    logToFile(logfile, level = 2, isError = False, message = 'Downloading ' + file + ' as '+ fileName)
                                    oc.get_file(path.join(TsfItems['remoteFolder'],file).replace("\\","/"), path.join(TsfItems['localFolder'],fileName).replace("\\","/"))
                                    if TsfItems['deleteAfter'] == True:
                                        try:
                                            oc.delete (path.join(TsfItems['remoteFolder'],file).replace("\\","/"))
                                        except Exception as rmv:
                                            logToFile(logfile, level = 2, isError = True, message = 'Unable to delete file from SFTP : ' + str(rmv))
                                            hasError = True
                                except Exception as dwnld:
                                    logToFile(logfile, level = 2, isError = True, message = 'An ERROR occured during Download : ' + str(dwnld))
                                    hasError = True
                except Exception as rep:
                    logToFile(logfile, level = 2, isError = True, message = 'An ERROR occured during access to Path : ' + str(rep))
                    hasError = True
                logToFile(logfile, level = 1, isError = False, message = 'END Transfert')
        elif apiItems['status']== 'enabled' or apiItems['status']  == 1 or apiItems['status']  == '1':
            pass
        else:
            # Bloc tranfert depuis local ou filer réseau+
            for actualFolder in TsfItems['remoteFolder']:
                if (path.isdir(actualFolder)):
                    logToFile(logfile, level = 1, isError = False, message = 'BEGIN transfert')
                    logToFile(logfile, level = 2, isError = False, message = 'Accessing remote folder : ' + actualFolder)
                    listFiles = []
                    cleanlist = []
                    listZip = []
                    listFiles = listdir(actualFolder)
                    for index in range (len(listComputedMask)):
                        cleanlist += [file for file in listFiles if fnmatch.fnmatch(path.basename(file), listComputedMask[index])]
                    if TsfItems['lookForZip']:
                        listZip = [file for file in listFiles if fnmatch.fnmatch(path.basename(file), computedZipMask)]
                    logToFile(logfile, level = 2, isError = False, message = 'Total Files and Folders Found : ' + str(len(listFiles)))
                    logToFile(logfile, level = 1, isError = False, message = '->\t' +'Total Matching Files Found : ' + str(len(cleanlist) + len (listZip)))
                    if TsfItems['lookForZip']:
                        listZip = []
                        listZip = [file for file in listFiles if fnmatch.fnmatch(path.basename(file), computedZipMask)]
                        logToFile(logfile, level = 2, isError = False, message = 'ZipFile mask : ' + computedZipMask + '\t' + 'Files Found : ' + str(len(listZip)))
                        #Zip
                        for file in listZip:
                            cleanlistZip = []
                            try:
                                zipName = preFix + file
                                logToFile(logfile, level = 2, isError = False, message = 'Downloading ' + file + ' as '+ zipName)
                                zipFile = path.join(TsfItems['localFolder'],zipName).replace("\\","/")
                                if TsfItems['deleteAfter'] == True:
                                    try:
                                        shutil.move (path.join(actualFolder,file).replace("\\","/"), path.join(TsfItems['localFolder'],zipName).replace("\\","/"))
                                    except Exception as mv:
                                        logToFile(logfile, level = 2, isError = True, message = 'Unable to delete file from remote folder : ' + str(mv))
                                        hasError = True
                                else:
                                    try:
                                        shutil.copyfile(path.join(actualFolder,file).replace("\\","/"), path.join(TsfItems['localFolder'],zipName).replace("\\","/"))
                                    except Exception as cp:
                                        logToFile(logfile, level = 2, isError = True, message = 'Unable to copy file from remote folder : ' + str(cp))
                                        hasError = True
                                #Decompress Zip
                                logToFile(logfile, level = 2, isError = False, message = 'Decompressing ' + file)
                                with zipfile.ZipFile(zipFile, 'r') as myzip:
                                    listZip=myzip.infolist()
                                    for zipIndex in range(len(listComputedMask)):
                                        cleanlistZip += [zip_info for zip_info in listZip if (fnmatch.fnmatch(zip_info.filename, listComputedMask[zipIndex]) and zip_info.filename[-1] != '/')]
                                        logToFile(logfile, level = 2, isError = False, message = 'Current mask : ' + listComputedMask[zipIndex] + '\t' + 'Cumulative Files Found : ' + str(len(cleanlistZip)))
                                    for fileZipped in cleanlistZip:
                                        try:
                                            fileZipped.filename = path.basename(fileZipped.filename)
                                            myzip.extract(fileZipped,TsfItems['localFolder'])
                                            newFileZipped = preFix + fileZipped.filename
                                            shutil.move(path.join(TsfItems['localFolder'],fileZipped.filename).replace("\\","/"), path.join(TsfItems['localFolder'],newFileZipped).replace("\\","/"))
                                            logToFile(logfile, level = 3, isError = False, message = 'File extracted : ' + fileZipped.filename + ' as :' + newFileZipped)
                                        except Exception as extr:
                                            logToFile(logfile, level = 3, isError = True, message = 'Unable to extract ' + fileZipped.filename + ' from zip : ' + str(extr))
                                            hasError = True
                                try:
                                    shutil.move(zipFile,TsfItems['archiveFolder'])
                                except Exception as mvz:
                                    logToFile(logfile, level = 2, isError = True, message = 'Unable to move ' + zipFile + ' to archive : ' + str(mvz))
                                    hasError = True
                            except Exception as dwnld:
                                logToFile(logfile, level = 2, isError = True, message = 'An ERROR occured during Download : ' + str(dwnld))
                                hasError = True
                    for index in range (len(listComputedMask)):
                        cleanlist = []
                        hasError = False
                        cleanlist = [file for file in listFiles if fnmatch.fnmatch(path.basename(file), listComputedMask[index])]
                        logToFile(logfile, level = 2, isError = False, message = 'Current mask : ' + listComputedMask[index] + '\t' + 'Files Found : ' + str(len(cleanlist)))
                        for file in cleanlist:
                            try:
                                hasAction = True
                                fileName = preFix + file
                                logToFile(logfile, level = 2, isError = False, message = 'Downloading ' + file + ' as '+ fileName)
                                if TsfItems['deleteAfter'] == True:
                                    try:
                                        shutil.move (path.join(actualFolder,file).replace("\\","/"), path.join(TsfItems['localFolder'],fileName).replace("\\","/"))
                                    except Exception as mv:
                                        logToFile(logfile, level = 2, isError = True, message = 'Unable to move file from remote folder : ' + str(mv))
                                        hasError = True
                                else:
                                    try:
                                        shutil.copyfile(path.join(actualFolder,file).replace("\\","/"), path.join(TsfItems['localFolder'],fileName).replace("\\","/"))
                                    except Exception as cp:
                                        logToFile(logfile, level = 2, isError = True, message = 'Unable to copy file from remote folder : ' + str(cp))
                                        hasError = True
                            except Exception as dwnld:
                                logToFile(logfile, level = 2, isError = True, message = 'An ERROR occured during Download : ' + str(dwnld))
                                hasError = True
                    logToFile(logfile, level = 1, isError = False, message = 'END Transfert')
                else:
                    logToFile(logfile, level = 1, isError = True, message = 'Unable to locate remote folder : ' + actualFolder)
                    hasError = True
        # Bloc Import
        if (SqlItems['status'] == 1 or SqlItems['status'] == '1' or SqlItems['status'] == 'enabled') :
            listFiles = []
            cleanlistAll = []
            listFiles = listdir(TsfItems['localFolder'])
            for index in range (len(listComputedMask)):
                cleanlistAll += [file for file in listFiles if fnmatch.fnmatch(path.basename(file), '*' + listComputedMask[index])]
            if len(cleanlistAll)!=0:
                hasAction = True
                logToFile(logfile, level = 1, isError = False, message = 'BEGIN Import')
                try:
                    logToFile(logfile, level = 2, isError = False, message = 'Connecting to server : ' + SqlItems['sqlServer'] + ',' + str(SqlItems['sqlPort']) + ' DataBase : ' +  SqlItems['sqlDataBase'])
                    conn = DatabaseConnector(SqlItems['sqlServer'], SqlItems['sqlDataBase'] ).create_connection()
                    engine = DatabaseEngine(SqlItems['sqlServer'], SqlItems['sqlDataBase'] ).create_connection()
                except Exception as sql:
                    logToFile(logfile, level = 2, isError = True, message = 'An ERROR occured during connecion to SQL Server : ' + str(sql))
                    hasError = True
                else:
                    cleanlist=[]
                    for index in range (len(listComputedMask)):
                        cleanlist = [file for file in listFiles if fnmatch.fnmatch(path.basename(file), '*' + listComputedMask[index])]
                        for file in cleanlist:
                            df=None
                            if hasErrorGlobal == False:
                                hasErrorGlobal = hasError
                            hasError = False
                            filepath = path.join(TsfItems['localFolder'],file).replace("\\","/")
                            archivepath = path.join(TsfItems['archiveFolder'],file).replace("\\","/")
                            errpath = path.join(TsfItems['errorFolder'],file).replace("\\","/")
                            try:
                                file_name, file_extension = path.splitext(file)
                                if file_extension in ['.csv', '.txt', '.ows']:
                                    decimal = TsfItems.get('decimal') if TsfItems.get('decimal') else '.'
                                    if 'dtypes' not in locals() and 'dtypes' not in globals():
                                        dtypes = 'str'
                                    if TsfItems['useFileColumns'] == True:
                                        with open(filepath, errors='replace',encoding=TsfItems['encoding']) as filehandle:
                                            if TsfItems['quotechar'] == None:
                                                df = pd.read_csv(filehandle, encoding=TsfItems['encoding'], delimiter=TsfItems['separator'],engine = 'python', decimal = decimal, dtype = dtypes)
                                            else:
                                                df = pd.read_csv(filehandle, quotechar=TsfItems['quotechar'], encoding=TsfItems['encoding'], delimiter=TsfItems['separator'],engine = 'python', decimal = decimal, dtype = dtypes)
                                    else:
                                        if 'skiprows' not in locals() and 'skiprows' not in globals():
                                            skiprows=0
                                        if 'skipfooter' not in locals() and 'skipfooter' not in globals():
                                            skipfooter = 0
                                        with open(filepath, errors='replace',encoding=TsfItems['encoding']) as filehandle:
                                            if TsfItems['quotechar'] == None:
                                                df = pd.read_csv(filehandle, encoding=TsfItems['encoding'], delimiter=TsfItems['separator'], skiprows = skiprows, skipfooter = skipfooter, header = None, names= columnNames.values(), usecols=columnNames.keys(), engine = 'python' , dtype = dtypes)
                                            else:
                                                df = pd.read_csv(filehandle, quotechar=TsfItems['quotechar'], encoding=TsfItems['encoding'], delimiter=TsfItems['separator'], skiprows = skiprows, skipfooter = skipfooter, header = None, names= columnNames.values(), usecols=columnNames.keys(), engine = 'python' , dtype = dtypes)
                                elif file_extension in ['.xls', '.xlsx', '.xlsm','.xlsb']:
                                    if 'skiprows' not in locals() and 'skiprows' not in globals():
                                        skiprows = 1
                                    if 'skipfooter' not in locals() and 'skipfooter' not in globals():
                                        skipfooter = 0
                                    if TsfItems['useFileColumns'] == True:
                                        df = pd.read_excel(filepath, skiprows = skiprows, skipfooter=skipfooter)
                                    else:
                                        df = pd.read_excel(filepath, skiprows = skiprows, skipfooter=skipfooter, header = None, names= columnNames.values(),usecols=columnNames.keys())
                                elif file_extension in ['.json']:
                                    if 'dropNACol' not in locals() and 'dropNACol' not in globals():
                                        dropNACol = ""
                                    if 'addParent' not in locals() and 'addParent' not in globals():
                                        addParent = False
                                    if 'addJsonText' not in locals() and 'addJsonText' not in globals():
                                        addJsonText = False
                                    def flattenData(data, parent ="", row ={}, rows = []):
                                        fieldSep = "."
                                        if type(data) == list:
                                            for _, item in enumerate(data):
                                                flattenData(item, parent, row, rows)
                                        else :
                                            for key, value in data.items():
                                                if parent != "" and addParent == True :
                                                    child = f"{parent}{fieldSep}{key}"
                                                else:
                                                    child = f"{key}"
                                                if type (value) == dict or type(value) == list:
                                                    flattenData(value, child, row, rows)
                                                else:
                                                    row = {**row, child: str(value)}
                                            if addJsonText == True:
                                                row = {**row, "jsonText" :  str(row) }
                                            rows.append(row)
                                        return rows
                                    with open(filepath, 'r') as fileHandler:
                                        data = json.load(fileHandler)
                                    flattened_data=flattenData(data)
                                    df = pd.DataFrame(flattened_data)
                                    if 'dropNACol' in locals() or 'dropNACol' in globals():
                                        if dropNACol != "":
                                            df.dropna(subset=[dropNACol],inplace=True)
                                else:
                                    logToFile(logfile, level = 2, isError = True, message = 'Insupported file format : ' + file_extension)
                                    shutil.move(filepath,errpath)
                                    logToFile(logfile, level = 2, isError = True, message = 'File in Error : ' + file)
                                    hasError = True
                                    continue
                                try:
                                    isNew = False
                                    if SqlItems['sqlTableMode'].lower() == 'fixed':
                                        sqlTable = SqlItems['sqlTable'][index]
                                    else:
                                        startPos = SqlItems['sqlStartPos']
                                        stopStr =  SqlItems['sqlStopStr']
                                        tablePrefix = SqlItems['sqlTablePrefix']
                                        if file[startPos:].find(stopStr) >= 0:
                                            sqlTable =  tablePrefix + file[startPos:file[startPos:].find(stopStr) + startPos].lower()
                                        else:
                                            sqlTable =  tablePrefix + file[startPos:file[startPos:].find('.') + startPos].lower()
                                        sqlTable = sqlTable.replace (' ','_')
                                    importMode = SqlItems['importMode']
                                    if importMode.lower() == 'truncate':
                                        importMode = 'append'
                                        # TRUNCATE SQL
                                        try:
                                            res=conn.execute ("SELECT name FROM " + SqlItems['sqlDataBase'] + ".sys.tables WHERE name = '" + sqlTable + "'")
                                            if len (res.fetchall()) != 0: #Check if table exists
                                                sql = f"TRUNCATE TABLE [{SqlItems['sqlDataBase']}].[{SqlItems['sqlSchema']}].[{sqlTable}]"
                                                engine.execute(sa.text(sql))
                                                engine.commit()
                                        except Exception as trnc:
                                            logToFile(logfile, level = 2, isError = True, message = 'Unable to truncate table : ' + SqlItems['sqlDataBase'] + '.' + SqlItems['sqlSchema']+ '.[' + sqlTable + ']' +  ' Error :' + str(trnc))
                                            hasError = True
                                    elif importMode == 'replace':
                                        importMode = 'append'
                                        # DROP SQL
                                        try:
                                            res=conn.execute ("SELECT name FROM " + SqlItems['sqlDataBase'] + ".sys.tables WHERE name = '" + sqlTable + "'")
                                            if len (res.fetchall()) != 0: #Check if table exists
                                                sql = f"DROP TABLE [{SqlItems['sqlDataBase']}].[{SqlItems['sqlSchema']}].[{sqlTable}]"
                                                engine.execute(sa.text(sql))
                                                engine.commit()
                                        except Exception as trnc:
                                            logToFile(logfile, level = 2, isError = True, message = 'Unable to drop table : ' + SqlItems['sqlDataBase'] + '.' + SqlItems['sqlSchema']+ '.[' + sqlTable + ']' +  ' Error :' + str(trnc))
                                            hasError = True
                                    if hasError == False:
                                        takeAction = False
                                        outputdict = sqlcol(df)
                                        logToFile(logfile, level = 1, isError = False, message = '-> Mask : ' + listComputedMask[index] + '\tTable : ' +  sqlTable  + '\tSP : ' +  SqlItems['spExec'][index])
                                        if 'dropNACol' in locals() or 'dropNACol' in globals():
                                            if dropNACol != "" :
                                                if dropNACol in df.columns:
                                                    df.dropna(subset=[dropNACol],inplace=True)
                                                else:
                                                    logToFile(logfile, level = 2, isError = False, message = 'DropNACol (' + dropNACol + ') not found in file : ' + file + '. Ignoring...' )
                                        try:
                                            def cleanStr(inputStr):
                                                cleanedStr = inputStr
                                                cleanedStr = cleanedStr.lower()
                                                cleanedStr = cleanedStr.replace(" ", "").replace("_", "").replace(".", "").replace("#", "").replace("-", "")
                                                return cleanedStr
                                            if SqlItems.get('bcpEncoding') == None:
                                                bcpEncoding = "utf-16le"
                                            else:
                                                bcpEncoding = SqlItems.get('bcpEncoding')
                                            if SqlItems.get('bcpSeparator') == None:
                                                bcpSeparator = "¤"
                                            else:
                                                bcpSeparator = SqlItems.get('bcpSeparator')
                                            res=conn.execute ("SELECT name FROM " + SqlItems['sqlDataBase'] + ".sys.tables WHERE name = '" + sqlTable + "'")
                                            if len (res.fetchall()) != 0: #Check if table exists
                                                listOld = []
                                                dfSQL = pd.read_sql(f"SELECT TOP 1 * FROM {SqlItems['sqlDataBase']}.{SqlItems['sqlSchema']}.[{sqlTable}]",con=engine)
                                                listOld = dfSQL.columns.to_list()
                                                listNew = df.columns.to_list()
                                                listNewRenamed= []
                                                for itemNew in listNew:
                                                    found=0
                                                    for itemOld in listOld:
                                                        if cleanStr(itemNew) ==cleanStr(itemOld):
                                                            listNewRenamed.append(itemOld)
                                                            found=1
                                                    if found == 0:
                                                        listNewRenamed.append(itemNew)
                                                listNewClean = [item for item in listNewRenamed]
                                                listOldClean = [item for item in listOld]
                                                listDelta= [item for item in listNewClean if item not in listOldClean]      #New Columns
                                                listDelta2 = [item for item in listOldClean if item not in listNewRenamed]  #Missing Columns
                                                if len(listDelta) > 0:
                                                    logToFile(logfile, level = 1, isError = False, message = f"!!! Table structure has changed. New Columns : {listDelta}")
                                                    if SqlItems.get('autoAddColumns') == True:
                                                        try:
                                                            logToFile(logfile, level = 1, isError = False, message = f"Adding missing columns automatically.")
                                                            sql = f"ALTER TABLE [{SqlItems['sqlDataBase']}].[{SqlItems['sqlSchema']}].[{sqlTable}] ADD "
                                                            for item in listDelta:
                                                                sql= sql +  f"[{item}] NVARCHAR ({TsfItems['nvarcharLength']}), "
                                                            sql = sql[:-2]
                                                            engine.execute(sa.text(sql))
                                                            engine.commit()
                                                            hasWarn = True
                                                            for col in listDelta2:
                                                                df[col] = pd.Series()
                                                            df.rename(columns=dict(zip(listNew, listNewRenamed)), inplace=True)
                                                            takeAction = True
                                                        except:
                                                            logToFile(logfile, level = 1, isError = True, message = f"Unable to add missing columns.")
                                                            hasError = True
                                                            takeAction = False
                                                    else:
                                                        logToFile(logfile, level = 1, isError = False, message = f"AutoAddColumns is set to False")
                                                        takeAction = False
                                                        hasWarn = True
                                                else:
                                                    takeAction = True
                                                if takeAction == True:
                                                    if SqlItems.get('useBCP') == True:
                                                        listNewBCP = listOld + listDelta
                                                        for col in listDelta2:
                                                            df[col] = pd.Series()
                                                        df.rename(columns=dict(zip(listNew, listNewRenamed)), inplace=True)
                                                        dfBCP = df[listNewBCP].copy()
                                                        fileBCP = path.join(TsfItems['localFolder'],"fileBCP.csv").replace("\\","/")
                                                        dfBCP.to_csv(fileBCP, encoding=bcpEncoding, sep=bcpSeparator, index=False)
                                                        bcpCommand=f'BCP [{SqlItems["sqlDataBase"]}].[{SqlItems["sqlSchema"]}].[{sqlTable}] IN "{fileBCP}" -S {SqlItems["sqlServer"]},{SqlItems["sqlPort"]} -T -w -t "{bcpSeparator}" -C 65001 -F {SqlItems["firstDataRow"]}'
                                                        logToFile(logfile, level = 1, isError = False, message = f"Using BCP command : {bcpCommand}")
                                                        try:
                                                            res=subprocess.run(bcpCommand, shell=True, check=True, capture_output=True, text=True)
                                                            logToFile(logfile, level = 1, isError = False, message = f"Command executed successfully : {res.stdout.splitlines()[-1]}")
                                                            logToFile(logfile, level = 2, isError = False, message = file + " ==> " + sqlTable +' (' + str(len(df)) +' rows) with BCP')
                                                            remove(fileBCP)
                                                        except subprocess.CalledProcessError as e:
                                                            logToFile(logfile, level = 1, isError = True, message = f"Command ran into an error : {str(e)}")
                                                            try:
                                                                shutil.move(filepath,errpath)
                                                                logToFile(logfile, level = 2, isError = False, message = "File : " + file + ' moved to '+ TsfItems['errorFolder'])
                                                            except Exception as moveToError:
                                                                logToFile(logfile, level = 2, isError = True, message = "Error moving file to InError : " + file + '. Error Text :' + str(moveToError))
                                                                hasError = True
                                                    else:
                                                        df.to_sql(name = sqlTable ,schema = SqlItems['sqlSchema'], con=engine, if_exists=importMode, index=False, dtype = outputdict)
                                                        engine.commit()
                                                        logToFile(logfile, level = 2, isError = False, message = file + " ==> " + sqlTable +' (' + str(len(df)) +' rows) with ' + SqlItems['importMode'])
                                                else:
                                                    logToFile(logfile, level = 1, isError = False, message = f"skipping import")
                                            else: #Table doesn't exist
                                                if SqlItems.get('useBCP') == True:
                                                    lineToCopy = df.iloc[1:2]
                                                    dfCreator = lineToCopy.copy()
                                                    dfCreator.to_sql(name = sqlTable ,schema = SqlItems['sqlSchema'], con=engine, if_exists=importMode, index=False, dtype = outputdict)
                                                    sql = f"TRUNCATE TABLE [{SqlItems['sqlDataBase']}].[{SqlItems['sqlSchema']}].[{sqlTable}]"
                                                    engine.execute(sa.text(sql))
                                                    engine.commit()
                                                    logToFile(logfile, level = 1, isError = False, message = f"Table created with one line and emptied")
                                                    dfBCP = df.copy()
                                                    fileBCP = path.join(TsfItems['localFolder'],"fileBCP.csv").replace("\\","/")
                                                    dfBCP.to_csv(fileBCP, encoding=bcpEncoding, sep=bcpSeparator, index=False)
                                                    bcpCommand=f'BCP [{SqlItems["sqlDataBase"]}].[{SqlItems["sqlSchema"]}].[{sqlTable}] IN "{fileBCP}" -S {SqlItems["sqlServer"]},{SqlItems["sqlPort"]} -T -w -t "{bcpSeparator}" -C65001 -F {SqlItems["firstDataRow"]}'
                                                    logToFile(logfile, level = 1, isError = False, message = f"Using BCP command : {bcpCommand}")
                                                    try:
                                                        res=subprocess.run(bcpCommand, shell=True, check=True, capture_output=True, text=True)
                                                        logToFile(logfile, level = 1, isError = False, message = f"Command executed successfully : {res.stdout.splitlines()[-1]}")
                                                        logToFile(logfile, level = 2, isError = False, message = file + " ==> " + sqlTable +' (' + str(len(df)) +' rows) with BCP')
                                                        remove(fileBCP)
                                                    except subprocess.CalledProcessError as e:
                                                        logToFile(logfile, level = 1, isError = True, message = f"Command ran into an error : {str(e)}")
                                                        try:
                                                            shutil.move(filepath,errpath)
                                                            logToFile(logfile, level = 2, isError = False, message = "File : " + file + ' moved to '+ TsfItems['errorFolder'])
                                                        except Exception as moveToError:
                                                            logToFile(logfile, level = 2, isError = True, message = "Error moving file to InError : " + file + '. Error Text :' + str(moveToError))
                                                            hasError = True
                                                else:
                                                    df.to_sql(name = sqlTable ,schema = SqlItems['sqlSchema'], con=engine, if_exists=importMode, index=False, dtype = outputdict)
                                                    engine.commit()
                                                    logToFile(logfile, level = 2, isError = False, message = file + " ==> " + sqlTable +' (' + str(len(df)) +' rows) with ' + SqlItems['importMode'])
                                        except Exception as imp:
                                            logToFile(logfile, level = 2, isError = True, message = "Error importing data to SQL Table. Error Text :" + str(imp))
                                            hasError = True
                                            try:
                                                shutil.move(filepath,errpath)
                                                logToFile(logfile, level = 2, isError = False, message = "File : " + file + ' moved to '+ TsfItems['errorFolder'])
                                            except Exception as moveToError:
                                                logToFile(logfile, level = 2, isError = True, message = "Error moving file to InError : " + file + '. Error Text :' + str(moveToError))
                                                hasError = True
                                        else:
                                        #Exec SP
                                            if SqlItems['spExec'][index] != '' and SqlItems['spExec'][index][:2] != '--' and hasError == False:
                                                logToFile(logfile, level = 2, isError = False, message = "Executing Stored Procedure : " + SqlItems['spExec'][index])
                                                try:
                                                    cursor =conn.cursor()
                                                    result=cursor.execute("SET NOCOUNT ON; DECLARE @ret int; EXEC @ret = " + SqlItems['sqlDataBase'] + '.' + SqlItems['sqlSchema'] + '.' + SqlItems['spExec'][index] + " ?, ?, ?, ?, ?, ?, ?, ?; SELECT 'RETURN_VALUE' = @ret",
                                                        (piItems['Provenance'],
                                                        piItems['Client'],
                                                        piItems['Campagne'],
                                                        piItems['Sufixe'],
                                                        listComputedMask[index],
                                                        file,
                                                        SqlItems['sqlDataBase'],
                                                        sqlTable)
                                                        ).fetchall()
                                                    spResult = (result[0][0])
                                                    conn.commit()
                                                    if spResult != 0:
                                                        logToFile(logfile, level = 3, isError = True, message = "Return Value = " + str(spResult))
                                                        hasError = True
                                                        try:
                                                            shutil.move(filepath,errpath)
                                                            logToFile(logfile, level = 2, isError = False, message = "File : " + file + ' moved to '+ TsfItems['errorFolder'])
                                                        except Exception as moveToError:
                                                            logToFile(logfile, level = 2, isError = True, message = "Error moving file to InError : " + file + '. Error Text :' + str(moveToError))
                                                            hasError = True
                                                    else:
                                                        #logToFile(logfile, level = 3, isError = False, message = "Return Value = " + str(spResult))
                                                        try:
                                                            shutil.move(filepath,archivepath)
                                                            logToFile(logfile, level = 2, isError = False, message = "File Moved to Archive : " + archivepath)
                                                        except Exception as moveToArchive:
                                                            logToFile(logfile, level = 2, isError = True, message = "Error moving file to Archive : " + file + '. Error Text :' + str(moveToArchive))
                                                            hasError = True
                                                            try:
                                                                shutil.move(filepath,errpath)
                                                                logToFile(logfile, level = 2, isError = False, message = "File : " + file + ' moved to '+ TsfItems['errorFolder'])
                                                            except Exception as moveToError:
                                                                logToFile(logfile, level = 2, isError = True, message = "Error moving file to InError : " + file + '. Error Text :' + str(moveToError))
                                                                hasError = True
                                                except Exception as exsp:
                                                    logToFile(logfile, level = 3, isError = True, message = "An Error occured. Error Text :" + str(exsp))
                                                    hasError = True
                                                    try:
                                                        shutil.move(filepath,errpath)
                                                        logToFile(logfile, level = 2, isError = False, message = "File : " + file + ' moved to '+ TsfItems['errorFolder'])
                                                    except Exception as moveToError:
                                                        logToFile(logfile, level = 2, isError = True, message = "Error moving file to InError : " + file + '. Error Text :' + str(moveToError))
                                                        hasError = True
                                                finally:
                                                    cursor = None
                                            else:
                                                try:
                                                    shutil.move(filepath,archivepath)
                                                    logToFile(logfile, level = 2, isError = False, message = "File Moved to Archive : " + archivepath)
                                                except Exception as moveToArchive:
                                                    logToFile(logfile, level = 2, isError = True, message = "Error moving file to Archive : " + file + '. Error Text :' + str(moveToArchive))
                                                    hasError = True
                                    else:
                                        try:
                                            shutil.move(filepath,errpath)
                                            logToFile(logfile, level = 2, isError = False, message = "File : " + file + ' moved to '+ TsfItems['errorFolder'])
                                        except Exception as moveToError:
                                            logToFile(logfile, level = 2, isError = True, message = "Error moving file to InError : " + file + '. Error Text :' + str(moveToError))
                                            hasError = True
                                except Exception as err:
                                    logToFile(logfile, level = 2, isError = True, message = "Failed to import : " + file + '. Error Text :' + str(err))
                                    hasError = True
                                    try:
                                        shutil.move(filepath,errpath)
                                        logToFile(logfile, level = 2, isError = False, message = "File : " + file + ' moved to '+ TsfItems['errorFolder'])
                                    except Exception as moveToError:
                                        logToFile(logfile, level = 2, isError = True, message = "Error moving file to InError : " + file + '. Error Text :' + str(moveToError))
                                        hasError = True
                                        continue
                            except Exception as read:
                                logToFile(logfile, level = 2, isError = True, message = "Failed to import : " + file + '. Error Text :' + str(read))
                                hasError = True
                                try:
                                    shutil.move(filepath,errpath)
                                    logToFile(logfile, level = 2, isError = False, message = "File : " + file + ' moved to '+ TsfItems['errorFolder'])
                                except Exception as moveToError:
                                    logToFile(logfile, level = 2, isError = True, message = "Error moving file to InError : " + file + '. Error Text :' + str(moveToError))
                                    hasError = True
                                if hasErrorGlobal == False:
                                    hasErrorGlobal = hasError
                    logToFile(logfile, level = 1, isError = False, message = 'END Import')
            else:
                logToFile(logfile, level = 1, isError = False, message = "No files to import ")
        if hasErrorGlobal == False:
            hasErrorGlobal = hasError
    else:
        logToFile(logfile, level = 1, isError = True, message = '!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Items Number Do Not Match !!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    # End Work Here
    if 'useTLS' not in MailItems:
        useTLS = True
    else:
        useTLS = MailItems['useTLS']
    sendMail(hasErrorGlobal, useTLS = useTLS)
    logToFile(logfile, level = 1, isError = False, message = f"{' END CYCLE '.center(150, '#')}")
