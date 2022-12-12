"""
=============================================================================
Project Name        : PythoImport Version 4.0
Created by          : Hedi FEZAI
Date Creation       : 10/11/2022
Date Modification   : 08/12/2022
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

 WishList
    ToDo :
    ?   Add Support for TarBalls (tar, tar.gz, tgz)
    ?   Add API Support
    ?   Add SQL/MySQL as Source
    ?   Add Export to SFTP
    ?   Add Export to MAIL

"""
from os import path,makedirs,listdir,rename,environ
import shutil
import pandas as pd
import sqlalchemy as sa
import pysftp
import fnmatch
import base64
from datetime import datetime,timedelta
import smtplib, ssl
from email.mime.text import MIMEText as text
from email.mime.multipart import MIMEMultipart
import zipfile
import owncloud
import pyodbc
from settings import *


# Let's Go

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

# Gestion des fichiers logs
def logfile (log_file, message):
    with open(log_file,'a',encoding = 'utf-8') as f:
        f.write  (message)
old_log_file=''
log_file = LogItems['logFolder'] + '/' + LogItems['filePrefix'] +'.txt'
log_sizeKB = LogItems['MaxFileSizeKB']
if path.exists(log_file):
    if datetime.now().strftime("%Y%m%d") != datetime.fromtimestamp(path.getctime(log_file)).strftime("%Y%m%d") or path.getsize(log_file)/1024 > log_sizeKB:
        old_log_file = LogItems['logFolder'] + '/' + LogItems['filePrefix'] + '_' + datetime.now().strftime("%Y%m%d_%H%M%S") + '.txt'
        rename(log_file, old_log_file)

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
loglist = []
hasErrorGlobal = False
hasError = False
sftpStatus=sftpItems.pop('status')
logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + '########################################## BEGIN CYCLE ##########################################' + '\n')
if len(TsfItems['fileMask']) == len(SqlItems['sqlTable']) == len(SqlItems['spExec']):
    #Gestion des Masques des Fichiers à Télécharger
    listComputedMask=[]
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
    if sftpStatus == 'Enabled' or sftpStatus == 1 or sftpStatus == '1':
        # Bloc SFTP
        try:
            sftp = None
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            try:
                sftpItems['password'] = base64.b64decode(sftpItems['password'].encode("ascii")).decode("ascii").replace('\n', '')
            except Exception as pwd:
                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Error decrypting SFTP password : ' + str(pwd) + '\n')
                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '' + 'Error decrypting SFTP password : ' + str(pwd) +'</font>')
                hasError = True
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + 'BEGIN transfert\n')
            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '' + 'BEGIN transfert')
            if sftp:
                sftp.close
            sftp = pysftp.Connection(**sftpItems, cnopts = cnopts)
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Connected to : ' + sftpItems['host'] +' with Username : ' + sftpItems['username'] + '\n')
            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Connected to : ' + sftpItems['host'] +' with Username : ' + sftpItems['username'])
        except Exception as cnx:
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + str(cnx) + ' Server : ' + sftpItems['host'] +'. Username : ' + sftpItems['username'] + '\n')
            loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + str(cnx) + ' Server : ' + sftpItems['host'] +'. Username : ' + sftpItems['username']+'</font>')
            hasError = True
        else:
            try:
                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Accessing remote folder : ' + TsfItems['remoteFolder'] + '\n')
                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Accessing remote folder : ' + TsfItems['remoteFolder'])
                with sftp.cd(TsfItems['remoteFolder']):
                    listfiles = []
                    cleanlist = []
                    listZip = []
                    listfiles=sftp.listdir()
                    for index in range (len(listComputedMask)):
                        cleanlist += [file for file in listfiles if fnmatch.fnmatch(file, listComputedMask[index])]
                    if TsfItems['lookForZip']:
                        listZip = [file for file in listfiles if fnmatch.fnmatch(file, computedZipMask)]
                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'Total Files and Folders Found : ' + str(len(listfiles)) + '\n')
                    loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'Total Files and Folders Found : ' + str(len(listfiles)))
                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t->\t' +'Total Matching Files Found : ' + str(len(cleanlist) + len (listZip)) + '\n')
                    loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'<b>Total Matching Files Found : ' + str(len(cleanlist) + len (listZip)))
                    if TsfItems['lookForZip']:
                        listZip = []
                        listZip = [file for file in listfiles if fnmatch.fnmatch(file, computedZipMask)]
                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'ZipFile mask : ' + computedZipMask + '\t' + 'Files Found : ' + str(len(listZip)) + '\n')
                        loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'ZipFile mask : ' + computedZipMask + '\t' + 'Files Found : ' + str(len(listZip)))
                        #Zip
                        for file in listZip:
                            cleanlistZip = []
                            try:
                                zipName = datetime.now().strftime("%Y%m%d_%H%M%S") + '_' + file
                                preFix = datetime.now().strftime("%Y%m%d_%H%M%S") + '_'
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Downloading ' + file + ' as '+ zipName + '\n')
                                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Downloading ' + file + ' as '+ zipName)
                                zipFile = TsfItems['localFolder'] + '/' + zipName
                                sftp.get (file, zipFile)
                                #Decompress Zip
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Decompressing ' + file + '\n')
                                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Decompressing ' + file + '\n')
                                with zipfile.ZipFile(zipFile, 'r') as myzip:
                                    listZip=myzip.infolist()
                                    for zipIndex in range(len(listComputedMask)):
                                        cleanlistZip += [zip_info for zip_info in listZip if (fnmatch.fnmatch(zip_info.filename, listComputedMask[zipIndex]) and zip_info.filename[-1] != '/')]
                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'Current    mask : ' + listComputedMask[zipIndex] + '\t' + 'Cumulative Files Found : ' + str(len(cleanlistZip)) + '\n')
                                        loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'Current    mask : ' + listComputedMask[zipIndex] + '\t' + 'Cumulative Files Found : ' + str(len(cleanlistZip)))
                                    for fileZipped in cleanlistZip:
                                        try:
                                            fileZipped.filename = path.basename(fileZipped.filename)
                                            myzip.extract(fileZipped,TsfItems['localFolder'])
                                            newFileZipped = preFix + fileZipped.filename
                                            shutil.move(TsfItems['localFolder'] + '/' + fileZipped.filename, TsfItems['localFolder'] + '/' + newFileZipped)
                                            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t\t' +'File extracted : ' + fileZipped.filename + ' as :' + newFileZipped + '\n')
                                            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp; &emsp;' +'File extracted : ' + fileZipped.filename + ' as :' + newFileZipped)
                                        except Exception as extr:
                                            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t\t' + 'Unable to extract ' + fileZipped.filename + ' from zip : ' + str(extr) + '\n')
                                            loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp; &emsp;' + 'Unable to extract ' + fileZipped.filename + ' from zip : ' + str(extr)+'</font>')
                                            hasError = True
                                try:
                                    shutil.move(zipFile,TsfItems['archiveFolder'])
                                except Exception as mvz:
                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Unable to move ' + zipFile + ' to archive : ' + str(mvz) + '\n')
                                    loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to move ' + zipFile + ' to archive : ' + str(mvz)+'</font>')
                                    hasError = True
                                if TsfItems['deleteAfter'] == True:
                                    try:
                                        sftp.remove (file)
                                    except Exception as rmv:
                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Unable to delete file from SFTP : ' + str(rmv) + '\n')
                                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to delete file from SFTP : ' + str(rmv)+'</font>')
                                        hasError = True
                            except Exception as dwnld:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'An ERROR occured during Download : ' + str(dwnld) + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'An ERROR occured during Download : ' + str(dwnld)+'</font>')
                                hasError = True
                    for index in range (len(listComputedMask)):
                        cleanlist = []
                        hasError = False
                        cleanlist = [file for file in listfiles if fnmatch.fnmatch(file, listComputedMask[index])]
                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'Current    mask : ' + listComputedMask[index] + '\t' + 'Files Found : ' + str(len(cleanlist)) + '\n')
                        loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'Current    mask : ' + listComputedMask[index] + '\t' + 'Files Found : ' + str(len(cleanlist)))
                        for file in cleanlist:
                            try:
                                fileName = datetime.now().strftime("%Y%m%d_%H%M%S") + '_' + file
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Downloading ' + file + ' as '+ fileName + '\n')
                                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Downloading ' + file + ' as '+ fileName)
                                sftp.get(file, TsfItems['localFolder'] + '/' + fileName)
                                if TsfItems['deleteAfter'] == True:
                                    try:
                                        sftp.remove (file)
                                    except Exception as rmv:
                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Unable to delete file from SFTP : ' + str(rmv) + '\n')
                                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to delete file from SFTP : ' + str(rmv)+'</font>')
                                        hasError = True
                            except Exception as dwnld:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'An ERROR occured during Download : ' + str(dwnld) + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'An ERROR occured during Download : ' + str(dwnld)+'</font>')
                                hasError = True
            except Exception as rep:
                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'An ERROR occured during access to Path : ' + str(rep) + '\n')
                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'An ERROR occured during access to Path : ' + str(rep)+'</font>')
                hasError = True
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + 'END Transfert\n')
            loglist.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '\t' + 'END Transfert')
            if sftp:
                sftp.close
                sftp = None
    elif owncldItems['status'] == 'Enabled' or owncldItems['status']  == 1 or owncldItems['status']  == '1':
        # Bloc de transfert OwnCloud
        try:
            try:
                owncldItems['password'] = base64.b64decode(owncldItems['password'].encode("ascii")).decode("ascii").replace('\n', '')
            except Exception as pwd:
                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Error decrypting OwnCloud password : ' + str(pwd) + '\n')
                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '' + 'Error decrypting OwnCloud password : ' + str(pwd) +'</font>')
                hasError = True
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + 'BEGIN transfert\n')
            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '' + 'BEGIN transfert')
            oc = owncloud.Client(owncldItems['host'])
            oc.login(owncldItems['username'], owncldItems['password'])
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Connected to : ' + owncldItems['host'] +' with Username : ' + owncldItems['username'] + '\n')
            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Connected to : ' + owncldItems['host'] +' with Username : ' + owncldItems['username'])
        except Exception as cnx:
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + str(cnx) + ' Server : ' + owncldItems['host'] +'. Username : ' + owncldItems['username'] + '\n')
            loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + str(cnx) + ' Server : ' + owncldItems['host'] +'. Username : ' + owncldItems['username']+'</font>')
            hasError = True
        else:
            try:
                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Accessing remote folder : ' + TsfItems['remoteFolder'] + '\n')
                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Accessing remote folder : ' + TsfItems['remoteFolder'])
                with oc.cd(TsfItems['remoteFolder']):
                    listfiles = []
                    cleanlist = []
                    listZip = []
                    listfiles = oc.list(TsfItems['remoteFolder'], depth = 1)
                    for index in range (len(listComputedMask)):
                        cleanlist += [file for file in listfiles if fnmatch.fnmatch(file, listComputedMask[index])]
                    if TsfItems['lookForZip']:
                        listZip = [file for file in listfiles if fnmatch.fnmatch(file, computedZipMask)]
                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'Total Files and Folders Found : ' + str(len(listfiles)) + '\n')
                    loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'Total Files and Folders Found : ' + str(len(listfiles)))
                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t->\t' +'Total Matching Files Found : ' + str(len(cleanlist) + len (listZip)) + '\n')
                    loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'<b>Total Matching Files Found : ' + str(len(cleanlist) + len (listZip)))
                    if TsfItems['lookForZip']:
                        listZip = []
                        listZip = [file for file in listfiles if fnmatch.fnmatch(file, computedZipMask)]
                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'ZipFile mask : ' + computedZipMask + '\t' + 'Files Found : ' + str(len(listZip)) + '\n')
                        loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'ZipFile mask : ' + computedZipMask + '\t' + 'Files Found : ' + str(len(listZip)))
                        #Zip
                        for file in listZip:
                            cleanlistZip = []
                            try:
                                zipName = datetime.now().strftime("%Y%m%d_%H%M%S") + '_' + file
                                preFix = datetime.now().strftime("%Y%m%d_%H%M%S") + '_'
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Downloading ' + file + ' as '+ zipName + '\n')
                                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Downloading ' + file + ' as '+ zipName)
                                zipFile = TsfItems['localFolder'] + '/' + zipName
                                oc.get_file (TsfItems['remoteFolder']+ '/'+ file, zipFile)
                                #Decompress Zip
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Decompressing ' + file + '\n')
                                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Decompressing ' + file + '\n')
                                with zipfile.ZipFile(zipFile, 'r') as myzip:
                                    listZip=myzip.infolist()
                                    for zipIndex in range(len(listComputedMask)):
                                        cleanlistZip += [zip_info for zip_info in listZip if (fnmatch.fnmatch(zip_info.filename, listComputedMask[zipIndex]) and zip_info.filename[-1] != '/')]
                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'Current    mask : ' + listComputedMask[zipIndex] + '\t' + 'Cumulative Files Found : ' + str(len(cleanlistZip)) + '\n')
                                        loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'Current    mask : ' + listComputedMask[zipIndex] + '\t' + 'Cumulative Files Found : ' + str(len(cleanlistZip)))
                                    for fileZipped in cleanlistZip:
                                        try:
                                            fileZipped.filename = path.basename(fileZipped.filename)
                                            myzip.extract(fileZipped,TsfItems['localFolder'])
                                            newFileZipped = preFix + fileZipped.filename
                                            shutil.move(TsfItems['localFolder'] + '/' + fileZipped.filename, TsfItems['localFolder'] + '/' + newFileZipped)
                                            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t\t' +'File extracted : ' + fileZipped.filename + ' as :' + newFileZipped + '\n')
                                            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp; &emsp;' +'File extracted : ' + fileZipped.filename + ' as :' + newFileZipped)
                                        except Exception as extr:
                                            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t\t' + 'Unable to extract ' + fileZipped.filename + ' from zip : ' + str(extr) + '\n')
                                            loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp; &emsp;' + 'Unable to extract ' + fileZipped.filename + ' from zip : ' + str(extr)+'</font>')
                                            hasError = True
                                try:
                                    shutil.move(zipFile,TsfItems['archiveFolder'])
                                except Exception as mvz:
                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Unable to move ' + zipFile + ' to archive : ' + str(mvz) + '\n')
                                    loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to move ' + zipFile + ' to archive : ' + str(mvz)+'</font>')
                                    hasError = True
                                if TsfItems['deleteAfter'] == True:
                                    try:
                                        oc.delete (TsfItems['remoteFolder']+ '/'+ file)
                                    except Exception as rmv:
                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Unable to delete file from SFTP : ' + str(rmv) + '\n')
                                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to delete file from SFTP : ' + str(rmv)+'</font>')
                                        hasError = True
                            except Exception as dwnld:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'An ERROR occured during Download : ' + str(dwnld) + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'An ERROR occured during Download : ' + str(dwnld)+'</font>')
                                hasError = True
                    for index in range (len(listComputedMask)):
                        cleanlist = []
                        hasError = False
                        cleanlist = [file for file in listfiles if fnmatch.fnmatch(file, listComputedMask[index])]
                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'Current    mask : ' + listComputedMask[index] + '\t' + 'Files Found : ' + str(len(cleanlist)) + '\n')
                        loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'Current    mask : ' + listComputedMask[index] + '\t' + 'Files Found : ' + str(len(cleanlist)))
                        for file in cleanlist:
                            try:
                                fileName = datetime.now().strftime("%Y%m%d_%H%M%S") + '_' + file
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Downloading ' + file + ' as '+ fileName + '\n')
                                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Downloading ' + file + ' as '+ fileName)
                                oc.get_file(TsfItems['remoteFolder']+ '/'+ file, TsfItems['localFolder'] + '/' + fileName)
                                if TsfItems['deleteAfter'] == True:
                                    try:
                                        oc.delete (TsfItems['remoteFolder']+ '/'+ file)
                                    except Exception as rmv:
                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Unable to delete file from SFTP : ' + str(rmv) + '\n')
                                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to delete file from SFTP : ' + str(rmv)+'</font>')
                                        hasError = True
                            except Exception as dwnld:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'An ERROR occured during Download : ' + str(dwnld) + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'An ERROR occured during Download : ' + str(dwnld)+'</font>')
                                hasError = True
            except Exception as rep:
                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'An ERROR occured during access to Path : ' + str(rep) + '\n')
                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'An ERROR occured during access to Path : ' + str(rep)+'</font>')
                hasError = True
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + 'END Transfert\n')
            loglist.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '\t' + 'END Transfert')
    else:
        # Bloc tranfert depuis local ou filer réseau
        if (path.isdir(TsfItems['remoteFolder'])):
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + 'BEGIN transfert\n')
            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '' + 'BEGIN transfert')
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Accessing remote folder : ' + TsfItems['remoteFolder'] + '\n')
            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Accessing remote folder : ' + TsfItems['remoteFolder'])
            listfiles = []
            cleanlist = []
            listZip = []
            listfiles = listdir(TsfItems['remoteFolder'])
            for index in range (len(listComputedMask)):
                cleanlist += [file for file in listfiles if fnmatch.fnmatch(file, listComputedMask[index])]
            if TsfItems['lookForZip']:
                listZip = [file for file in listfiles if fnmatch.fnmatch(file, computedZipMask)]
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'Total Files and Folders Found : ' + str(len(listfiles)) + '\n')
            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'Total Files and Folders Found : ' + str(len(listfiles)))
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t->\t' +'Total Matching Files Found : ' + str(len(cleanlist) + len (listZip)) + '\n')
            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'<b>Total Matching Files Found : ' + str(len(cleanlist) + len (listZip)))
            if TsfItems['lookForZip']:
                listZip = []
                listZip = [file for file in listfiles if fnmatch.fnmatch(file, computedZipMask)]
                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'ZipFile mask : ' + computedZipMask + '\t' + 'Files Found : ' + str(len(listZip)) + '\n')
                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'ZipFile mask : ' + computedZipMask + '\t' + 'Files Found : ' + str(len(listZip)))
                #Zip
                for file in listZip:
                    cleanlistZip = []
                    try:
                        zipName = datetime.now().strftime("%Y%m%d_%H%M%S") + '_' + file
                        preFix = datetime.now().strftime("%Y%m%d_%H%M%S") + '_'
                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Downloading ' + file + ' as '+ zipName + '\n')
                        loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Downloading ' + file + ' as '+ zipName)
                        zipFile = TsfItems['localFolder'] + '/' + zipName
                        if TsfItems['deleteAfter'] == True:
                            try:
                                shutil.move (TsfItems['remoteFolder'] + '/' + file, TsfItems['localFolder'] + '/' + zipName)
                            except Exception as mv:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Unable to delete file from remote folder : ' + str(mv) + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to delete file from remote folder : ' + str(mv)+'</font>')
                                hasError = True
                        else:
                            try:
                                shutil.copyfile(TsfItems['remoteFolder'] + '/' + file, TsfItems['localFolder'] + '/' + zipName)
                            except Exception as cp:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Unable to copy file from remote folder : ' + str(cp) + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to copy file from remote folder : ' + str(cp)+'</font>')
                                hasError = True
                        #Decompress Zip
                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Decompressing ' + file + '\n')
                        loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Decompressing ' + file + '\n')
                        with zipfile.ZipFile(zipFile, 'r') as myzip:
                            listZip=myzip.infolist()
                            for zipIndex in range(len(listComputedMask)):
                                cleanlistZip += [zip_info for zip_info in listZip if (fnmatch.fnmatch(zip_info.filename, listComputedMask[zipIndex]) and zip_info.filename[-1] != '/')]
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'Current    mask : ' + listComputedMask[zipIndex] + '\t' + 'Cumulative Files Found : ' + str(len(cleanlistZip)) + '\n')
                                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'Current    mask : ' + listComputedMask[zipIndex] + '\t' + 'Cumulative Files Found : ' + str(len(cleanlistZip)))
                            for fileZipped in cleanlistZip:
                                try:
                                    fileZipped.filename = path.basename(fileZipped.filename)
                                    myzip.extract(fileZipped,TsfItems['localFolder'])
                                    newFileZipped = preFix + fileZipped.filename
                                    shutil.move(TsfItems['localFolder'] + '/' + fileZipped.filename, TsfItems['localFolder'] + '/' + newFileZipped)
                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t\t' +'File extracted : ' + fileZipped.filename + ' as :' + newFileZipped + '\n')
                                    loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp; &emsp;' +'File extracted : ' + fileZipped.filename + ' as :' + newFileZipped)
                                except Exception as extr:
                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t\t' + 'Unable to extract ' + fileZipped.filename + ' from zip : ' + str(extr) + '\n')
                                    loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp; &emsp;' + 'Unable to extract ' + fileZipped.filename + ' from zip : ' + str(extr)+'</font>')
                                    hasError = True
                        try:
                            shutil.move(zipFile,TsfItems['archiveFolder'])
                        except Exception as mvz:
                            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Unable to move ' + zipFile + ' to archive : ' + str(mvz) + '\n')
                            loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to move ' + zipFile + ' to archive : ' + str(mvz)+'</font>')
                            hasError = True
                    except Exception as dwnld:
                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'An ERROR occured during Download : ' + str(dwnld) + '\n')
                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'An ERROR occured during Download : ' + str(dwnld)+'</font>')
                        hasError = True
            for index in range (len(listComputedMask)):
                cleanlist = []
                hasError = False
                cleanlist = [file for file in listfiles if fnmatch.fnmatch(file, listComputedMask[index])]
                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +'Current    mask : ' + listComputedMask[index] + '\t' + 'Files Found : ' + str(len(cleanlist)) + '\n')
                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +'Current    mask : ' + listComputedMask[index] + '\t' + 'Files Found : ' + str(len(cleanlist)))
                for file in cleanlist:
                    try:
                        fileName = datetime.now().strftime("%Y%m%d_%H%M%S") + '_' + file
                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Downloading ' + file + ' as '+ fileName + '\n')
                        loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Downloading ' + file + ' as '+ fileName)
                        if TsfItems['deleteAfter'] == True:
                            try:
                                shutil.move (TsfItems['remoteFolder'] + '/' + file, TsfItems['localFolder'] + '/' + fileName)
                            except Exception as mv:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Unable to move file from remote folder : ' + str(mv) + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to move file from remote folder : ' + str(mv) +'</font>')
                                hasError = True
                        else:
                            try:
                                shutil.copyfile(TsfItems['remoteFolder'] + '/' + file, TsfItems['localFolder'] + '/' + fileName)
                            except Exception as cp:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Unable to copy file from remote folder : ' + str(cp) + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Unable to copy file from remote folder : ' + str(cp)+'</font>')
                                hasError = True
                    except Exception as dwnld:
                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'An ERROR occured during Download : ' + str(dwnld) + '\n')
                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'An ERROR occured during Download : ' + str(dwnld)+'</font>')
                        hasError = True
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + 'END Transfert\n')
            loglist.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '\t' + 'END Transfert')
        else:
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t' + 'Unable to locate remote folder : ' + TsfItems['remoteFolder'] + '\n')
            loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '' + 'Unable to locate remote folder : ' + TsfItems['remoteFolder']+'</font>')
            hasError = True
    if (SqlItems['status'] == 1 or SqlItems['status'] == '1' or SqlItems['status'] == 'Enabled') :
        # Bloc Import
        listfiles = []
        cleanlistAll = []
        listfiles = listdir(TsfItems['localFolder'])
        for index in range (len(listComputedMask)):
            cleanlistAll += [file for file in listfiles if fnmatch.fnmatch(file, '*' + listComputedMask[index])]
        if len(cleanlistAll)!=0:
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + 'BEGIN Import\n')
            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '' + 'BEGIN Import')
            try:
                if SqlItems['sqlPort'] !=0:
                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Connecting to server : ' + SqlItems['sqlServer'] + ',' + str(SqlItems['sqlPort']) + ' DataBase : ' +  SqlItems['sqlDataBase'] + '\n')
                    loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Connecting to server : ' + SqlItems['sqlServer'] + ',' + str(SqlItems['sqlPort']) + ' DataBase : ' +  SqlItems['sqlDataBase'])
                    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + SqlItems['sqlServer'] + ',' + str(SqlItems['sqlPort'])+ ';initial_catalog=' + SqlItems['sqlDataBase'] +';Trusted_Connection=yes;Encrypt=no;MARS_Connection=Yes;')
                    engine = sa.create_engine('mssql+pyodbc://' + SqlItems['sqlServer'] + ',' + str(SqlItems['sqlPort']) + '/' + SqlItems['sqlDataBase'] +'?trusted_connection=yes&driver=ODBC+Driver+18+for+SQL+Server&encrypt=no', echo = True)
                else:
                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Connecting to server : ' + SqlItems['sqlServer'] + ' DataBase : ' +  SqlItems['sqlDataBase'] + '\n')
                    loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Connecting to server : ' + SqlItems['sqlServer'] + ' DataBase : ' +  SqlItems['sqlDataBase'])
                    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + SqlItems['sqlServer'] + ';initial_catalog=' + SqlItems['sqlDataBase'] +';trusted_connection=yes;Encrypt=no;MARS_Connection=Yes;')
                    engine = sa.create_engine('mssql+pyodbc://' + SqlItems['sqlServer'] + '/' + SqlItems['sqlDataBase'] +'?trusted_connection=yes&driver=ODBC+Driver+18+for+SQL+Server&encrypt=no', echo = True)
            except Exception as sql:
                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'An ERROR occured during connecion to SQL Server : ' + str(sql)  + '\n')
                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'An ERROR occured during connecion to SQL Server : ' + str(sql)+'</font>')
                hasError = True
            else:
                cleanlist=[]
                for index in range (len(listComputedMask)):
                    cleanlist = [file for file in listfiles if fnmatch.fnmatch(file, '*' + listComputedMask[index])]
                    for file in cleanlist:
                        df=None
                        hasError = False
                        filepath = TsfItems['localFolder'] + '/' + file
                        archivepath = TsfItems['archiveFolder'] + '/' + file
                        errpath = TsfItems['errorFolder'] +'/' + file
                        try:
                            file_name, file_extension = path.splitext(file)
                            if file_extension in ['.csv', '.txt', '.ows']:
                                if TsfItems['useFileColumns'] == True:
                                    df = pd.read_csv(filepath, quotechar=TsfItems['quotechar'], encoding=TsfItems['encoding'], delimiter=TsfItems['separator'])
                                else:
                                    df = pd.read_csv(filepath, quotechar=TsfItems['quotechar'], encoding=TsfItems['encoding'], delimiter=TsfItems['separator'], skiprows = 1, header = None, names= columnNames.values(),usecols=columnNames.keys())
                            elif file_extension in ['.xls', '.xlsx', '.xlsm','.xlsb']:
                                if TsfItems['useFileColumns'] == True:
                                    df = pd.read_excel(filepath)
                                else:
                                    df = pd.read_excel(filepath, skiprows = 1, header = None, names= columnNames.values(),usecols=columnNames.keys())
                            else:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Insupported file format : ' + file_extension +'\n')
                                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Insupported file format : ' + file_extension)
                                shutil.move(filepath,errpath)
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +  "File in Error : " + file + '\n')
                                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "File in Error : " + file)
                                hasError = True
                                continue
                            try:
                                if SqlItems['sqlTableMode'] == 'fixed':
                                    sqlTable = SqlItems['sqlTable'][index]
                                else:
                                    startPos = SqlItems['sqlStartPos']
                                    stopStr =  SqlItems['sqlStopStr']
                                    tablePrefix = SqlItems['sqlTablePrefix']
                                    if file[startPos:].find(stopStr) >= 0:
                                        sqlTable =  tablePrefix + file[startPos:file[startPos:].find(stopStr) + startPos].lower()
                                    else:
                                        sqlTable =  tablePrefix + file[startPos:file[startPos:].find('.') + startPos].lower()
                                importMode = SqlItems['importMode']
                                if importMode == 'truncate':
                                    importMode = 'append'
                                    # truncate SQL
                                    try:
                                        res=conn.execute ("SELECT name FROM " + SqlItems['sqlDataBase'] + ".sys.tables WHERE name = '" + sqlTable + "'")         # + SqlItems['sqlDataBase']+'.' + SqlItems['sqlSchema']+'.' + sqlTable)
                                        if len (res.fetchall()) != 0:
                                            conn.execute ("TRUNCATE TABLE " + SqlItems['sqlDataBase']+'.' + SqlItems['sqlSchema']+'.' + sqlTable)
                                            conn.commit()
                                    except Exception as trnc:
                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Unable to truncate table : " + SqlItems['sqlDataBase'] + '.' + SqlItems['sqlSchema'] + '.' +  ' Error :' + str(trnc) + '\n')
                                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Unable to truncate table : " + SqlItems['sqlDataBase'] + '.' + SqlItems['sqlSchema'] + '.'  + ' Error :' + str(trnc) +'</font>')
                                        hasError = True
                                if hasError == False:
                                    outputdict = sqlcol(df)
                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + 'Mask : ' + listComputedMask[index] + '\tTable : ' +  sqlTable  + '\tSP : ' +  SqlItems['spExec'][index] + '\n')
                                    loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + 'Mask : ' + listComputedMask[index] + '&emsp;' + 'Table : ' +  sqlTable  + '&emsp;' + 'SP : ' +  SqlItems['spExec'][index] )
                                    df.to_sql(name = sqlTable ,schema = SqlItems['sqlSchema'], con=engine, if_exists=importMode, index=False, dtype = outputdict)
                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' + file + " ==> " + sqlTable +' (' + str(len(df)) +' rows) with ' + SqlItems['importMode'] + '\n')
                                    loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' + file + " ==> " + sqlTable +' (' + str(len(df)) +' rows) with ' + SqlItems['importMode'])
                                    #Exec SP
                                    if SqlItems['spExec'][index] != '' and SqlItems['spExec'][index][:2] != '--':
                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t'  "Executing Stored Procedure : " + SqlItems['spExec'][index] + '\n')
                                        loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;'  "Executing Stored Procedure : " + SqlItems['spExec'][index])
                                        try:
                                            cursor =conn.cursor()
                                            result=cursor.execute("SET NOCOUNT ON; DECLARE @ret int; EXEC @ret = " + SqlItems['sqlDataBase'] + '.' + SqlItems['sqlSchema'] + '.' + SqlItems['spExec'][index] + " ?, ?, ?, ?, ?, ?, ?, ?; SELECT 'RETURN_VALUE' = @ret",
                                                (piItems['Provenance'],
                                                piItems['Client'],
                                                piItems['Campagne'],
                                                piItems['Sufixe'],
                                                computedMask,
                                                file,
                                                SqlItems['sqlDataBase'],
                                                sqlTable)
                                                ).fetchall()
                                            spResult = (result[0][0])
                                            conn.commit()
                                            if spResult != 0:
                                                hasError = True
                                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t\t' + "Return Value = " + str(spResult) + '\n')
                                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp; &emsp;' + "Return Value = " + str(spResult) + '</font>')
                                                try:
                                                    shutil.move(filepath,errpath)
                                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] + '\n')
                                                    loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] +'</font>')
                                                except Exception as moveToError:
                                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError) + '\n')
                                                    loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError)+'</font>')
                                            else:
                                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t\t' + "Return Value = " + str(spResult) + '\n')
                                                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp; &emsp;' + "Return Value = " + str(spResult))
                                                try:
                                                    shutil.move(filepath,archivepath)
                                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +  "File Moved to Archive : " + archivepath + '\n')
                                                    loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "File Moved to Archive : " + archivepath )
                                                except Exception as moveToArchive:
                                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Error moving file to Archive : " + file + '. Error Text :' + str(moveToArchive) + '\n')
                                                    loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Error moving file to Archive : " + file + '. Error Text :' + str(moveToArchive)+'</font>')
                                                    hasError = True
                                                    try:
                                                        shutil.move(filepath,errpath)
                                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] + '\n')
                                                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] +'</font>')
                                                    except Exception as moveToError:
                                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError) + '\n')
                                                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError)+'</font>')
                                        except Exception as exsp:
                                            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t\t' +  "An Error occured. Error Text :" + str(exsp) + '\n')
                                            loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp; &emsp;' +  "An Error occured. Error Text :" + str(exsp) +'</font>')
                                            hasError = True
                                            try:
                                                shutil.move(filepath,errpath)
                                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] + '\n')
                                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] +'</font>')
                                            except Exception as moveToError:
                                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError) + '\n')
                                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError)+'</font>')
                                        finally:
                                            cursor = None
                                    else:
                                        try:
                                            shutil.move(filepath,archivepath)
                                            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t\t' +  "File Moved to Archive : " + archivepath + '\n')
                                            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "File Moved to Archive : " + archivepath)
                                        except Exception as moveToArchive:
                                            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Error moving file to Archive : " + file + '. Error Text :' + str(moveToArchive) + '\n')
                                            loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Error moving file to Archive : " + file + '. Error Text :' + str(moveToArchive)+'</font>')
                                            hasError = True
                                else:
                                    try:
                                        shutil.move(filepath,errpath)
                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] + '\n')
                                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] +'</font>')
                                    except Exception as moveToError:
                                        logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError) + '\n')
                                        loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError)+'</font>')
                            except Exception as err:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Failed to import : " + file + '. Error Text :' + str(err) + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Failed to import : " + file + '. Error Text :' + str(err)+'</font>')
                                try:
                                    shutil.move(filepath,errpath)
                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] + '\n')
                                    loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] +'</font>')
                                except Exception as moveToError:
                                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError) + '\n')
                                    loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError)+'</font>')
                                    hasError = True
                                    continue
                        except Exception as read:
                            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Failed to import : " + file + '. Error Text :' + str(read) + '\n')
                            loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Failed to import : " + file + '. Error Text :' + str(read)+'</font>')
                            hasError = True
                            try:
                                shutil.move(filepath,errpath)
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "File : " + file + ' moved to '+ TsfItems['errorFolder'] +'</font>')
                            except Exception as moveToError:
                                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError) + '\n')
                                loglist.append ('<font color="red">' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '&emsp;' +  "Error moving file to InError : " + file + '. Error Text :' + str(moveToError)+'</font>')
                            if hasErrorGlobal == False:
                                hasErrorGlobal = hasError
                logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + 'END Import\n')
                loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '' + 'END Import')
        else:
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + "No files to import " + '\n')
            loglist.append (datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '' + "No files to import ")
    if hasErrorGlobal == False:
        hasErrorGlobal = hasError
    # Bloc mail
    if MailItems['status'] == 0:
        sendMail = False
    else:
        if MailItems['level'] == 'info':
            sendMail = True
        elif MailItems['level'] == 'error' and hasErrorGlobal == True:
            sendMail = True
        else:
            sendMail = False
    if sendMail == True:
        message= """
                <html>
                    <body>
                        <style type="text/css">
                            .tg  {border-collapse:collapse;border-spacing:0;}
                            .tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px; overflow:hidden;padding:3px 10px;word-break:normal;}
                            .tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:3px 10px;word-break:normal;}
                            .tg .tg-eqdz{font-family:"Lucida Sans Unicode", "Lucida Grande", sans-serif !important;font-size:12px;font-weight:bold;text-align:center;vertical-align:middle}
                            .tg .tg-j7mi{font-family:"Lucida Sans Unicode", "Lucida Grande", sans-serif !important;font-size:12px;text-align:center;vertical-align:middle}
                            .tg .tg-vwq0{font-family:"Lucida Sans Unicode", "Lucida Grande", sans-serif !important;font-size:12px;text-align:left;vertical-align:middle}
                        </style>
                        <table class="tg">
                            <thead>
                                <tr>
                                    <th class="tg-eqdz">DateTime</th>
                                    <th class="tg-eqdz">Text</th>
                                </tr>
                            </thead>
                            <tbody>
        """
        for lig in loglist:
            message += '<tr>' + '<td class="tg-j7mi">' + lig[:lig.find('20')+19] +'</td><td class="tg-vwq0">' + lig[lig.find('20')+19:] +'</th>' +'</tr>'
        message = message + '</table></body></html>'

        port = MailItems['port']
        smtp_server = MailItems['smtp_server']
        sender_email = MailItems['sender_email']
        receiver_email = MailItems['receiver_email']
        login_email = MailItems['login_email']

        try:
            password = base64.b64decode(MailItems['password'].encode("ascii")).decode("ascii").replace('\n', '')
        except Exception as pwd:
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t\t' + 'Error decrypting SMTP password : ' + str(pwd) + '\n')
        m = MIMEMultipart()
        m.attach(text(message, "html"))
        #m = text(message)
        if hasErrorGlobal == False:
            prefix_subject = '[INF]'
        else:
            prefix_subject = '[ERR]'
            m['X-Priority'] = '2'
        m['Subject'] = prefix_subject + '[' + environ["COMPUTERNAME"] + ']'+ 'PythoImport ' + piItems['Client'] + '_' + piItems['Campagne']
        m['From'] = sender_email
        m['To'] = " ,".join(receiver_email)
        try:
            context = ssl.create_default_context()
            if environ["COMPUTERNAME"] == 'FRPARDMT01':
                with smtplib.SMTP(smtp_server, port) as server:
                    server.ehlo()
                    server.starttls(context=context)
                    server.ehlo()
                    server.login(login_email, password)
                    res = server.sendmail(sender_email, receiver_email, m.as_string())
                    server.quit()
                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + 'Email Notification sent successfully to : ' + ", ".join(receiver_email) + '\n')
            else:
                with smtplib.SMTP_SSL(smtp_server, port) as server:
                    server.ehlo()
                    server.login(login_email, password)
                    res = server.sendmail(sender_email, receiver_email, m.as_string())
                    server.quit()
                    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + 'Email Notification sent successfully to : ' + ", ".join(receiver_email) + '\n')
        except:
            logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + 'E' +'|' +  '\t' + 'Failed to send Email Notification' + '\n')
    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + '########################################### END CYCLE ###########################################' + '\n\n')
else:
    logfile (log_file, datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '|' + ' ' +'|' +  '\t' + '!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Items Number Do Not Match !!!!!!!!!!!!!!!!!!!!!!!!!!!!!' + '\n\n')
