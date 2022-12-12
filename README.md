# dbaTools
# pythoImport
A little tool developed to help automate a little part of my daily tasks:
Extract Data from various files and load them into SQL Server Tables.
Sources can be SFTP server, OwnCloud drive, Network Shared Folders, local folders
Files can be CSV or Excel
Files can also be compressed inside Zip Files
Support Python Lists in fileMask, SQL Tables and Stored Procedures (Lists MUST HAVE same number of elemenets)
We can look for only files matching a certain mask in their names at a remote folder or inside Zips on that same folder
After loading the files into an SQL table, the script launches a Stored Procedure (usually to merge new records into a bigger table)
The logging part is well implemented (formatted text files and/or Email in HTML Format with recepients supporting Python Lists)
All the needed configuration variables are gathered in a separte file
Inside the script some parts swith automatically depending whether the script is on local testing machine  or deployed to a prduction server.
