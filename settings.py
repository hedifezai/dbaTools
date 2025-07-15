#################################################################################################
#                           Fichier de paramétrage PythoImport                                  #
#################################################################################################
from os import getenv
from cryptography.fernet import Fernet
from dotenv import load_dotenv
load_dotenv(r'pathToEnv\.env')

# Informations sur l'automate d'import et les dossier à créer
piItems = {
            'Provenance':    'Provenance',
            'Client'    :    'Client',
            'Campagne'  :    'Campagne',
            'Sufixe'    :    ''                  # Inutile, mais rajouté pour de la rétrocompatibilité avec AutoImport
}
# Informations sur la connexion SFTP, mettre status = 0 pour igoner ce bloc si inutie
sftpItems = {
            'host'      :   getenv('SFTP_HOST'),
            'port'      :   int(getenv('SFTP_PORT')),
            'username'  :   getenv('SFTP_USER'),
            'password'  :   getenv('SFTP_PASSWORD'),
            'status'    :   1                       # 1 = Enabled  0 = Disabled
}
# Informations sur la connexion OwnCloud, mettre status = 0 pour igoner ce bloc si inutie
# Pour OwnCloud, il faut créer un "mot de passe d'application" à partir d'un compte AD (se fait en se connectant personnellement à OC)
owncldItems = {
            'host'      :   'https://ownCloudURL',
            'username'  :   'user',
            'password'  :   '*************',
            'status'    :   0                       # 1 = Enabled  0 = Disabled
}
# Informations sur la connexion API, mettre status = 0 pour igoner ce bloc si inutie
# Pas encore implémentée !!!
apiItems = {
            'host'      :   'https://wft-geo-db.p.rapidapi.com/v1/geo',
            'username'  :   'user',
            'password'  :   'c**************',
            'headers'   :   {"X-RapidAPI-Key": "******","X-RapidAPI-Host": "wft-geo-db.p.rapidapi.com"},
            'endpoints' :   ['/countries', '/cities'],
            'status'    :   0                       # 1 = Enabled  0 = Disabled
}
# Hormis remoteFolder, et le répertoire d'application, tous les autres dossiers seront automatiquement créés s'ils n'existent pas
# NB : Les BackSlash "\" sont à remplacer par ForwardSlash "/" dans tous le chemins d'accès SFTP, OC, Filer ou même en local
rootFolder = 'D:/Data/' + piItems['Provenance'] +'/' + piItems['Client'] + '/' + piItems['Campagne'] + '_PythoImport'
LogFolder  = 'D:/Logs/' + piItems['Provenance'] +'/' + piItems['Client'] + '/' + piItems['Campagne'] + '_PythoImport'
# Information sur le process de transfert. Si SFTP et Owncloud sont désactivés, remoteFolfer est considéré comme source : emplacement réseau ou local
# Tags disponibles pour les masques (File ou Zip): §yyyy§ §MM§ §dd§ §W§ §hh§ §mm§ §ss§
TsfItems = {
            'remoteFolder'          :   [
                                         '/DEVDBA/',
                                        ],                # Emplacement Source distant SFTP, OC, Filer ou local
            'fileMask'              :   [
                                         '*.csv',
                                        ],          # Masque pour les fichiers trouvés sur Remote Folder et dans les Zip
            'useFileColumns'        :   True,               # Si False, il faut renseigner la variable columnNames suivante avec les numéros de colonnes à récupérer et leurs nouveaux noms
            'lookUpDay'             :   -1,                 # Décalage en jours par rapport à aujourd'hui pour le calcul des masque de fichiers
            'lookForZip'            :   False,               # Télecharge les Zips qui correspondent au zipMask et vérifie dedans la présence de fichiers avec fileMask
            'recursiveMode'         :   False,              # Mode de recherche recursif en SFTP
            'zipMask'               :   '*.zip',            # Masque pour les fichiers zip trouvés sur RemoteFolder
            'encoding'              :   'utf-8',     # File Encoding 'windows-1252', 'utf-8',...
            'separator'             :   ';',                # Field Separator
            'decimal'               :   '.',
            'quotechar'             :   None,               # Délimiteur de champs
            'forceAlltoNVARCHAR'    :   True,               # Forcer tous les champs à NVARCHAR(255) lors de la création des tables et de l'injection des données (evite les problèmes de conversion)
            'nvarcharLength'        :   4000,                # Taille du champs NVARCHAR
            'localFolder'           :   rootFolder + '/ToProcess',  # Répertoire qui va accueillir les fichiers transférés en attente du traitement
            'archiveFolder'         :   rootFolder + '/Processed',  # Répertoire qui va accueillir les fichiers traités ou les zips téléchargés
            'errorFolder'           :   rootFolder + '/InError',    # Répertoire qui va accueillir les fichiers en erreur suite traitement
            'addTimeStamp'          :   True,                       # Rajoute le timeStamp au nom fu fichier importé
            'deleteAfter'           :   False               # Supprime les fichiers distants après leur récupération
}
# Numéros de colonnes à récupérer du fichier source (commence à 0) avec les nouveaux nom à leur donner
columnNames ={
    2:'Status',
    3:'Nom Prénom',
    7:'Téléphone',
    8:'Email'
}

# dtypes ={    'CLI reçu' : 'string'    }

skiprows    = 0                 # Nombre de lignes à ignorer au début du fichier
skipfooter  = 0                 # Nombre de lignes à ignorer à la fin du fichier
addParent   = False             # Nomme les colonnes Json en parent.child sinon child tout court
addJsonText = True              # Ajoute le texte Brut Json en colonne à lafin de la table
dropNACol   = ""     # Supprime les lignes où cette colonne est vide

# Informations sur le logging des exécutions
LogItems = {
            'logFolder'     :   LogFolder + '/Logs',
            'filePrefix'    :   'PythoImport',       # Préfixe pour les fichiers Logs
            'MaxFileSizeKB' :   512,                 # Taille limite souhaitée d'un fichier log
            'retentionDays' :   30

}
# Informations sur la connexion SQL, mettre status = 0 pour igoner ce bloc si inutie
SqlItems = {
            'sqlServer'     :   'FRPARSQLTEST01',   # Nom ou adresse IP du serveur SQL
            'sqlPort'       :   1320,               # Mettre à 0 si on passe par les NamedPipes
            'sqlDataBase'   :   'TEST_HFE',         # Base de données où les fichiers seront importés
            'sqlSchema'     :   'imp',              # Schéma de la table dans la BDD
            'sqlTableMode'  :   'auto',             # fixed / auto -- Fixed : Table créée au préalable/ auto : la table sera créée si elle n'existe pas
            'autoAddColumns':   True,               # rajoute les colonnes manquantes à la table SQL automatiquement
            #auto :  les noms des tables seront extraits des noms des fichiers importés en se basant sur les éléments suivants
            'sqlStartPos'   :   16,                  # Position de départ pour le Split du nom du fichier
            'sqlStopStr'    :   '.',            # Chaine recherchée qui détermine la position de fin du split du nom de fichier
            'sqlTablePrefix':   'tTmpHFE_', # Préfixe pour les nom des tables crées par el split des noms de fichiers
            #Fixed
            'sqlTable'      :   [
                                 'tTmpPythoImportTEST54',
                                ],                      # Nom de la table SQL de destination si sqlMode = fixed
            'importMode'    :   'truncate',             # append/truncate/replace
            'spExec'        :   [
                                 '--pPythoImportSOWEE_Leads',
                                ], # Nom de la procédure SQL à lancer après l'import des données dans la table. Préfixez avec "--" pour désactiver
            'useBCP'        :   True,
            'firstDataRow'  :   2,
            'bcpEncoding'   :   'utf-16le',
            'bcpSeparator'  :   '¤',
            'status'        :   1
}
# Informations sur l'envoi des rapports d'intégration par mail, mettre status = 0 pour igoner ce bloc si inutie
MailItems = {
            'smtp_server'   :   getenv('HFE_SERVER'),           # Servur SMTP
            'port'          :   int(getenv('HFE_PORT')),        # Port SMTP
            'useTLS'        :   True,                           # TLS True or False
            'sender_email'  :   getenv('HFE_SENDER'),           # Emetteur
            'receiver_email':   [getenv('HFE_RECEIVER')],       # Destinataire du mail
            'login_email'   :   getenv('HFE_USER'),             # Login SMTP
            'password'      :   getenv('HFE_PASSWORD'),         # Mot de Passe SMTP
            'level'         :   'action', 	                    #info/action/error      # info : mails envoyés après chaque exécution, action : mail envoyé s'il y a eu une action d'import export, erreur : mails envoyés en cas d'erreur (l'objet sera modifié en si présence d'erreur)
            'status'        :   1
}

# Bloc Cryptage Mot de Passe
password = b"PutPasswordHereSaveRunCopyResultThenRemoveIt"
password_bytes = Fernet(getenv('FERNET_KEY')).encrypt(password)
# print (password_bytes)  # Décommenter et exécuter ou lancer en ligne de commande avec python.exe settings.py pour récupérer le mot de passe crypté
