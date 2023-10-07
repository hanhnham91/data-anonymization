import sqlite3
from sqlite3 import Error
import pandas as pd


def CreateConnection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def CreateTable(conn):
    conn.execute('''CREATE TABLE eval_result (  
        id              INTEGER PRIMARY KEY AUTOINCREMENT,      
        algo            CHAR(10),
        k               INT,
        total           INT,
        time_exe        REAL,
        sg_bf           INT,
        ug_bf           INT,
        um              INT,
        sg_af           INT,
        ug_af           INT,
        rules_bf        INT,
        rules_af        INT,
        lrp             REAL,
        nrp             REAL,
        drp             REAL,
        cavg            REAL
        );''')


def InsertRecord(conn, evalResult):
    """
    Create a new eval_result into the eval_result table
    :param conn:
    :param eval_result:
    :return: eval_result_id
    """
    sql = ''' INSERT INTO eval_result(algo,k,total,time_exe,
                                sg_bf,ug_bf,um,sg_af,ug_af,rules_bf,rules_af,
                                lrp, nrp, drp, cavg)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?); '''
    cur = conn.cursor()
    cur.execute(sql, evalResult)
    conn.commit()
    return cur.lastrowid


def Find(conn):
    df = pd.read_sql_query('''SELECT 
            *, 
            AVG(lrp) AS lrp1,
            AVG(nrp) AS nrp1,
            AVG(drp) AS drp1,
            AVG(cavg) AS cavg1     
        FROM eval_result 
        WHERE total>30000 
            AND algo<>'datafly' 
            AND k IN (5,15,25,35,45,55,65)
            AND algo IN ('m3ar','mm3ar','oka','topdown')
        GROUP BY algo,k;''', con=conn)
    return df
