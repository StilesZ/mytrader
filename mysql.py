#  -*- coding:utf-8 -*-
import pymysql
from dbutils.pooled_db import PooledDB


class DBMySQL:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = database
        self.conn = None
        self.cur = None
        self.pool = None

    def connect(self):
        self.pool = PooledDB(
            creator=pymysql,
            maxconnections=5,
            mincached=2,
            blocking=True,
            maxusage=None,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.db,
            charset='utf8'
        )
        self.conn = self.pool.connection()

    def close(self):
        self.conn.close()
        self.pool.close()

    def query(self, sql):
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        result = self.cur.fetchall()
        self.cur.close()
        self.conn.commit()
        return result

    def nonquery(self, sql):
        self.cur = self.conn.cursor()
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except:
            self.conn.rollback()
        self.cur.close()
