#################################################################################################
#                           Fichier de paramétrage PythoImport                                  #
#################################################################################################

# Informations sur l'automate d'import et les dossier à créer
piItems = {
            'Provenance':    'Clients',
            'Client'    :    'Client',
            'Campagne'  :    'Campagne',
            'Sufixe'    :    '_EA'                  # Inutile, mais rajouté pour de la rétrocompatibilité avec AutoImport
}
# Informations sur la connexion SFTP, mettre status = 0 pour igoner ce bloc si inutie
sftpItems = {
            'host'      :   'sftpServer',
            'port'      :   22,
            'username'  :   'user',
            'password'  :   '*************',
            'status'    :   0                       # 1 = Enabled  0 = Disabled
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
            'username'  :   'fezai.5',
            'password'  :   'cURHTjYtS3RqSkEtMm1pRUEtZmdrSnEtUnNUeko=',
            'headers'   :   {"X-RapidAPI-Key": "**********************","X-RapidAPI-Host": "wft-geo-db.p.rapidapi.com"},
            'endpoints' :   ['/countries', '/cities'],
            'status'    :   0                       # 1 = Enabled  0 = Disabled
}
# Hormis remoteFolder, et le répertoire d'application, tous les autres dossiers seront automatiquement créés s'ils n'existent pas
# NB : Les BackSlash "\" sont à remplacer par ForwardSlash "/" dans tous le chemins d'accès SFTP, OC, Filer ou même en local
rootFolder = 'D:/Test/' + piItems['Provenance'] +'/' + piItems['Client'] + '/' + piItems['Campagne'] + '_PythoImport'
LogFolder  = 'D:/Test/' + piItems['Provenance'] +'/' + piItems['Client'] + '/' + piItems['Campagne'] + '_PythoImport'
# Information sur le process de transfert. Si SFTP et Owncloud sont désactivés, remoteFolfer est considéré comme source : emplacement réseau ou local
# Tags disponibles pour les masques (File ou Zip): §yyyy§ §MM§ §dd§ §W§ §hh§ §mm§ §ss§
TsfItems = {
            'remoteFolder'          :   '/',                # Emplacement Source distant SFTP, OC, Filer ou local
            'fileMask'              :   ['*.csv'],          # Masque pour les fichiers trouvés sur Remote Folder et dans les Zip
            'useFileColumns'        :   True,               # Si False, il faut renseigner la variable columnNames suivante avec les numéros de colonnes à récupérer et leurs nouveaux noms
            'lookUpDay'             :   -1,                 # Décalage en jours par rapport à aujourd'hui pour le calcul des masque de fichiers
            'lookForZip'            :   True,               # Télecharge les Zips qui correspondent au zipMask et vérifie dedans la présence de fichiers avec fileMask
            'zipMask'               :   '*.zip',            # Masque pour les fichiers zip trouvés sur RemoteFolder
            'encoding'              :   'windows-1252',     # File Encoding 'windows-1252', 'utf-8',...
            'separator'             :   ';',                # Field Separator
            'quotechar'             :   None,               # Délimiteur de champs
            'forceAlltoNVARCHAR'    :   True,               # Forcer tous les champs à NVARCHAR(255) lors de la création des tables et de l'injection des données (evite les problèmes de conversion)
            'nvarcharLength'        :   100,                # Taille du champs NVARCHAR
            'localFolder'           :   rootFolder + '/ToProcess',  # Répertoire qui va accueillir les fichiers transférés en attente du traitement
            'archiveFolder'         :   rootFolder + '/Processed',  # Répertoire qui va accueillir les fichiers traités ou les zips téléchargés
            'errorFolder'           :   rootFolder + '/InError',    # Répertoire qui va accueillir les fichiers en erreur suite traitement
            'deleteAfter'           :   False               # Supprime les fichiers distants après leur récupération
}
# Numéros de colonnes à récupérer du fichier source (commence à 1) avec les nouveaux nom à leur donner
columnNames ={
    2:'Status',
    3:'Nom Prénom',
    7:'Téléphone',
    8:'Email'
}
# Informations sur le logging des exécutions
LogItems = {
            'logFolder'     :   LogFolder + '/Logs',
            'filePrefix'    :   'PythoImport',      # Préfixe pour les fichiers Logs
            'MaxFileSizeKB' :   512                 # Taille limite souhaitée d'un fichier log
}
# Informations sur la connexion SQL, mettre status = 0 pour igoner ce bloc si inutie
SqlItems = {
            'sqlServer'     :   'localhost',        # Nom ou adresse IP du serveur SQL
            'sqlPort'       :   0,                  # Mettre à 0 si on passe par les NamedPipes
            'sqlDataBase'   :   'database',        # Base de données où les fichiers seront importés
            'sqlSchema'     :   'dbo',              # Schéma de la table dans la BDD
            'sqlTableMode'  :   'fixed',            # fixed / auto -- Fixed : Table créée au préalable/ auto : la table sera créée si elle n'existe pas
            #auto :  les noms des tables seront extraits des noms des fichiers importés en se basant sur les éléments suivants
            'sqlStartPos'   :   0,                  # Position de départ pour le Split du nom du fichier
            'sqlStopStr'    :   '_2022',            # Chaine recherchée qui détermine la position de fin du split du nom de fichier
            'sqlTablePrefix':   'tTmpPythoImport_', # Préfixe pour les nom des tables crées par el split des noms de fichiers
            #Fixed
            'sqlTable'      :   ['tTmpPythoImport_PythonE'], # Nom de la table SQL de destination si sqlMode = fixed
            'importMode'    :   'truncate',          # append/truncate/replace
            'spExec'        :   ['pPythoImportLogFiles'], # Nom de la procédure SQL à lancer après l'import des données dans la table. Préfixez avec "--" pour désactiver
            'status'        :   1
}
# Informations sur l'envoi des rapports d'intégration par mail, mettre status = 0 pour igoner ce bloc si inutie
MailItems = {
            'smtp_server'   :   'smtp.server.com',              # Servur SMTP
            'port'          :   465,                            # Port SMTP
            'sender_email'  :   'email@gmail.com',              # Emetteur
            'receiver_email':   ['email@gmail.com'],            # Destinataire du mail
            'login_email'   :   'email@gmail.com',              # Login SMTP
            'password'      :   '*************',                # Mot de Passe SMTP
            'level'         :   'info',    #info/error          # info : mails envoyés après chaque exécution, erreur : mails envoyés en cas d'erreur (l'objet sera modifié en si présence d'erreur)
            'status'        :   0
}
# Bloc Cryptage Mot de Passe
import base64

password = "PutPasswordHereSaveRunCopyResultThenRemoveIt"
password_bytes = base64.b64encode(password.encode('ascii')).decode('ascii')
# print (password_bytes)  # Décommenter et exécuter ou lancer en ligne de commande avec python.exe settings.py pour récupérer le mot de passe crypté
