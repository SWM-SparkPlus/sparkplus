import sys
import os

import pandas as pd
import mysql.connector


conn= mysql.connector.connect(user='root', password='9315', host='localhost', port=3306)
cursor = conn.cursor()

cursor.execute('CREATE DATABASE test_db;')
cursor.execute('USE test_db;')
cursor.execute('CREATE TABLE Trains(SUBWAY_ID INT, STATN_ID INT, STATN_NM INT);')

# trains = pd.read_csv("../train.csv", sep=";")
#train_tuples = list(trains.itertuples(index=False, name=None))
# train_tuples_string = ",".join(["(" + ",".join([str(t) for t in tt]) + ")" for tt in train_tuples])

cursor.execute("INSERT INTO Trains(SUBWAY_ID, STATN_ID, STATN_NM) VALUES " + "(1, 1, 1)" + ";")
cursor.execute("FLUSH TABLES;")