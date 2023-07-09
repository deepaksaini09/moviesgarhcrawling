import configparser
import sys
import mysql.connector
from mysql.connector import Error
from constants import constants
import os


class dBConnection:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.config = None
        self.constantObj = constants()
        self.currentPath = (os.getcwd().split(self.constantObj.METAREEL_CRAWLING)[0])+self.constantObj.METAREEL_CRAWLING

    def configDataParser(self):
        self.config = configparser.ConfigParser()
        self.config.read(self.currentPath + "/config/config.ini")
        return self.config

    def dBConnectionForStreamA2Z(self):
        try:
            config = self.configDataParser()
            hostName = config.get('mysql_movies_garh', 'host')
            userName = config.get('mysql_movies_garh', 'user')
            passwd = config.get('mysql_movies_garh', 'passwd')
            dbName = config.get('mysql_movies_garh', 'db')
            print(hostName, dbName, passwd, userName)
            self.conn = mysql.connector.connect(host=hostName,
                                                database=dbName,
                                                user=userName,
                                                password=passwd,
                                                ssl_disabled=True
                                                )
            if self.conn.is_connected():
                db_Info = self.conn.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                self.cursor = self.conn.cursor()
        except Error as error:
            print("Error while connecting to MySQL", error)
        finally:
            if self.conn.is_connected():
                print("MySQL self.conn is closed")
                return self.conn, self.cursor