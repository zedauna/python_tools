# -*- coding: utf-8 -*-
"""
# ---------------------------------------------------------------------------
# Nom : 
# Creation : 31/12/2021
# Modification : 11/01/2022
# Auteur(s) : Jéros VIGAN 
# Projet : 
# Description : Toutes les fonctions d'insertion des données dans une table SQL
# ---------------------------------------------------------------------------
"""
### ===============================================================
###  Déclaration du dossier de travail
### ===============================================================
import os
base = r'D:\Navigation\Téléchargements\07_python\postgres'
base = base.replace('\\', '/')
os.chdir(base)

### ===============================================================
### Importation des packages
### ===============================================================
import pandas as pd
from fonctions_sql import *

### ===============================================================
### Importation data
### ===============================================================
df = pd.read_csv('https://raw.githubusercontent.com/Muhd-Shahid/Learn-Python-Data-Access/main/iris.csv',index_col=False)
df.head()
df.sample()

### ===============================================================
### Creation database
### ===============================================================
def database_create():
    conn_params_dic = {
        "host"      : "localhost",
        "port"      :  "5433",
        "user"      : "postgres",
        "password"  : "postgres"
    }

    conn = connect(conn_params_dic)
    conn.autocommit = True

    if conn!=None:
        try:
            cursor = conn.cursor();
            cursor.execute("DROP DATABASE IF EXISTS test_python_v2;") #name of database is test_python
            cursor.execute("CREATE DATABASE test_python_v2;");
            print("database is created successfully...")
            cursor.close()
            conn.close()
            
        except OperationalError as err:
            show_psycopg2_exception(err)
            conn = None

#database_create()

### ===============================================================
### Insertion datas
### ===============================================================
conn_params_dic = {
    "host"      : "localhost",
    "port"      :  "5433",
    "database"  : "test_python_v2",
    "user"      : "postgres",
    "password"  : "postgres"
}

connect_alchemy = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
    conn_params_dic['user'],
    conn_params_dic['password'],
    conn_params_dic['host'],
    conn_params_dic['port'],
    conn_params_dic['database']
)

using_alchemy_v2(df,'iris',connect_alchemy)
