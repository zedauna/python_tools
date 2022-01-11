# -*- coding: utf-8 -*-
"""
# ---------------------------------------------------------------------------
# Nom : fontions_sql
# Creation : 31/12/2021
# Auteur(s) : Jéros VIGAN 
# Projet : 
# Description : Toutes les fonctions d'insertion des données dans une table SQL'
# ---------------------------------------------------------------------------
"""

### ===============================================================
### Importation des packages
### ===============================================================
import os
import sys
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras
import pandas as pd
from io import StringIO
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine

### ===============================================================
### Fonctions
### ===============================================================

def show_psycopg2_exception(err):
    err_type, err_obj, traceback = sys.exc_info()
    line_n = traceback.tb_lineno
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)
    print ("\nextensions.Diagnostics:", err.diag)
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")

def connect(conn_params_dic):
    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dic)
        print("Connection successful..................")  
    except OperationalError as err:
        show_psycopg2_exception(err)
        conn = None
    return conn

def copy_from_dataFile_StringIO(conn, df, table):
    buffer = StringIO()
    df.to_csv(buffer, header=False, index = False)
    buffer.seek(0)
    contents = buffer.getvalue()
    print(contents)
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
        print("Data inserted using copy_from_datafile_StringIO() successfully....")
    except (Exception, psycopg2.DatabaseError) as error:
        show_psycopg2_exception(error)
        cursor.close()
    cursor.close()

def sql_create_table(df):
    dicto={}

    def correspondance_type(text):
        if text =='float64':
            return ' DECIMAL(2,1)'
        elif text =='object':
            return ' CHAR(250)'
        elif text=='int64':
            return ' integer'
        elif text=='datetime64':
            return ' CHAR(11)'

    for col in df.columns.tolist():
        tup=[]
        nulite=""
        print(col,df[col].dtypes)
        
        check_for_nan =df[col].isnull().values.any()
        if check_for_nan=='false':
            nulite=" NULL"
        else:
            nulite=" NOT NULL"
        
        tup.append(col)
        tup.append(correspondance_type(df[col].dtypes))
        tup.append(nulite)
        dicto[col]=''.join(tup)

    sql = '''CREATE TABLE  IF NOT EXISTS iris('''
    for i in dicto.values():
       sql =sql+i+','
    sql =sql+''')'''
    print(sql)

def create_table(conn,table_name):
    cursor = conn.cursor();
    try:
        cursor.execute("DROP TABLE IF EXISTS "+table_name+" ;")
        
        sql = '''CREATE TABLE {}(
        sepal_length DECIMAL(2,1) NOT NULL, 
        sepal_width DECIMAL(2,1) NOT NULL, 
        petal_length DECIMAL(2,1) NOT NULL, 
        petal_width DECIMAL(2,1),
        species CHAR(11)NOT NULL
        )'''.format(table_name)

        cursor.execute(sql);
        print("{} table is created successfully................".format(table_name)) 
        
    except OperationalError as err:
        show_psycopg2_exception(err)
        conn = None
    cursor.close()
        
def single_inserts(conn, df, table):
    cursor = conn.cursor();
    for i in df.index:
        cols  = ','.join(list(df.columns))
        vals  = [df.at[i,col] for col in list(df.columns)]
        query = "INSERT INTO %s(%s) VALUES(%s,%s,%s,%s,'%s')" % (table, cols, vals[0], vals[1], vals[2],vals[3],vals[4])
        cursor.execute(query)
    print("single_inserts() done")
    cursor.close()

def execute_many(conn, df, table):
    tpls = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(sql, tpls)
        print("Data inserted using execute_many() successfully...")
    except (Exception, psycopg2.DatabaseError) as err:
        show_psycopg2_exception(err)
        cursor.close()

def execute_batch(conn, df, table, page_size=150):
    tpls = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_batch(cursor, sql, tpls, page_size)
        print("Data inserted using execute_batch() successfully...")
    except (Exception, psycopg2.DatabaseError) as err:
        show_psycopg2_exception(err)
        cursor.close()

def execute_values(conn, df, table):
    tpls = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, sql, tpls)
        print("Data inserted using execute_values() successfully..")
    except (Exception, psycopg2.DatabaseError) as err:
        show_psycopg2_exception(err)
        cursor.close()

def execute_mogrify(conn, df, table):
    tpls = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    cursor = conn.cursor()
    values = [cursor.mogrify("(%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tpls]
    sql  = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)
    try:
        cursor.execute(sql, tpls)
        print("Data inserted using execute_mogrify() successfully.")
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as err:
        show_psycopg2_exception(err)
        cursor.close()
        
def copy_from_dataFile(conn, df, table):
    tmp_df = './Learn Python Data Access/iris_temp.csv'
    df.to_csv(tmp_df, header=False,index = False)
    f = open(tmp_df, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, sep=",")
        print("Data inserted using copy_from_datafile() successfully....")
    except (Exception, psycopg2.DatabaseError) as err:
        os.remove(tmp_df)
        show_psycopg2_exception(err)
        cursor.close()
        
def using_alchemy(df,table_name,connect_alchemy):
    try:
        engine = create_engine(connect_alchemy)
        df.to_sql(table_name, con=engine, index=False, if_exists='append',chunksize = 1000)
        print("Data inserted using to_sql()(sqlalchemy) done successfully...")
    except OperationalError as err:
        show_psycopg2_exception(err)

def sqlcol(df):
    dtypedict = {}
    for i,j in zip(df.columns, df.dtypes):
        # if "object" in str(j):
        #     dtypedict.update({i: sqlalchemy.types.String(length=255)})     #varchar ou char 
        
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.String()})    #Text

        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float()})
            
        # if "float" in str(j):
        #     dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})  # decimal

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.Integer()})
        
        if "bool" in str(j):
            dtypedict.update({i: sqlalchemy.types.Boolean()})

    return dtypedict

def using_alchemy_v2(df,table_name,connect_alchemy):
    try:
        outputdict = sqlcol(df)
        engine = create_engine(connect_alchemy)
        df.to_sql(table_name, con=engine, if_exists = 'append', index = False, dtype = outputdict)
        print("Data inserted using to_sql()(sqlalchemy) done successfully...")
    except OperationalError as err:
        show_psycopg2_exception(err)