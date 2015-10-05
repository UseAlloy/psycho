#!/usr/bin/env python

"""
A very simple wrapper for psycopg2

Methods:
    getOne() - get a single row
    getAll() - get all rows
    insert() - insert a row
    insertOrUpdate() - insert a row or update it if it exists
    update() - update rows
    delete() - delete rows
    query()  - run a raw sql query
    leftJoin() - do an inner left join query and get results

License: MIT

Scott Clark, Alloy
September 2015
"""

import datetime

import psycopg2
from collections import namedtuple


class Psycho:
    connection = None
    cursor = None
    config = None

    def __init__(self, **kwargs):
        self.config = kwargs
        self.config["keep_alive"] = kwargs.get("keep_alive", False)
        self.config["charset"] = kwargs.get("charset", "utf8")
        self.config["host"] = kwargs.get("host", "localhost")
        self.config["port"] = kwargs.get("port", 3306)
        self.config["autocommit"] = kwargs.get("autocommit", False)

        self.connect()

    def connect(self):
        """Connect to the postgresql server"""

        try:
            self.connection = psycopg2.connect(
                database=self.config['database'],
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
            )
            self.cursor = self.connection.cursor()
        except:
            print ("Postgresql connection failed")
            raise

    def getOne(self, table=None, fields='*', where=None, order=None, limit=1):
        """
        Get a single result

        table = (str) schema_name.table_name
        fields = (field1, field2 ...) list of fields to select
        where = ("parameterizedstatement", [parameters])
                        eg: ("id=%s and name=%s", [1, "test"])
        order = [field, ASC|DESC]
        limit = limit
        """

        cursor = self._select(table, fields, where, order, limit)
        result = cursor.fetchone()

        row = None
        if result:
            Row = namedtuple("Row", [f[0] for f in cursor.description])
            row = Row(*result)

        return row

    def getAll(self, table=None, fields='*', where=None, order=None, limit=None):
        """
        Get all results

        table = (str) schema_name.table_name
        fields = (field1, field2 ...) list of fields to select
        where = ("parameterizedstatement", [parameters])
                        eg: ("id=%s and name=%s", [1, "test"])
        order = [field, ASC|DESC]
        limit = limit
        """

        cursor = self._select(table, fields, where, order, limit)
        result = cursor.fetchall()

        return self.getRows(cursor, result)

    def leftJoin(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None):
        """
        Run an inner left join query

        tables = tuple(str, str) (schema.table1, schema.table2)
        fields = ([fields from table1], [fields from table 2])  # fields to select
        join_fields = (field1, field2)  # fields to join. field1 belongs to table1 and field2 belongs to table 2
        where = ("parameterizedstatement", [parameters])
                        eg: ("id=%s and name=%s", [1, "test"])
        order = [field, ASC|DESC]
        limit = limit
        """

        cursor = self._select_join(tables, fields, join_fields, where, order, limit)
        result = cursor.fetchall()

        return self.getRows(cursor, result)

    def insert(self, table, data):
        """Insert a record"""

        table_sql = ".".join(['"' + t + '"' for t in table.split(".")])

        query = self._serialize_insert(data)

        sql = "INSERT INTO %s (%s) VALUES (%s);" % (table_sql, query[0], query[1])

        # Check data values for python datetimes
        for key, value in data.items():
            if isinstance(value, datetime.datetime):
                data[key] = self._dumps_datetime(value)

        return self.query(sql, list(data.values()))

    def update(self, table, data, where=None):
        """Insert a record"""

        table_sql = ".".join(['"' + t + '"' for t in table.split(".")])

        query = self._serialize_update(data)

        sql = "UPDATE %s SET %s" % (table_sql, query)

        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        # Check data values for python datetimes
        for key, value in data.items():
            if isinstance(value, datetime.datetime):
                data[key] = self._dumps_datetime(value)

        return self.query(
            sql,
            list(data.values()) + where[1] if where and len(where) > 1 else data.values()
        )

    def insertOrUpdate(self, table, data, keys):
        table_sql = ".".join(['"' + t + '"' for t in table.split(".")])

        insert_data = data.copy()

        data = {k: data[k] for k in data if k not in keys}

        insert = self._serialize_insert(insert_data)

        update = self._serialize_update(data)

        sql = "INSERT INTO %s (%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s" % (table_sql, insert[0], insert[1], update)

        # Check values for python datetimes
        values = insert_data.values() + data.values()
        for idx, value in enumerate(values):
            if isinstance(value, datetime.datetime):
                values[idx] = self._dumps_datetime(value)

        return self.query(sql, list(values))

    def delete(self, table, where=None):
        """Delete rows based on a where condition"""

        table_sql = ".".join(['"' + t + '"' for t in table.split(".")])

        sql = "DELETE FROM %s" % table_sql

        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        return self.query(sql, where[1] if where and len(where) > 1 else None)

    def query(self, sql, params=None):
        """Run a raw query"""

        # check if connection is alive. if not, reconnect
        try:
            self.cursor.execute(sql, params)
        except psycopg2.DatabaseError:
            print("Database Error")
            raise
        except:
            print("Query failed")
            raise

        return self.cursor

    def commit(self):
        """Commit a transaction (transactional engines like InnoDB require this)"""
        return self.connection.commit()

    def is_open(self):
        """Check if the connection is open"""
        return self.connection.open

    def end(self):
        """Kill the connection"""
        self.cursor.close()
        self.connection.close()

    def getRows(self, cursor, result=None):
        rows = None
        if not result:
            result = cursor.fetchall()

        Row = namedtuple("Row", [f[0] for f in cursor.description])
        rows = [Row(*r) for r in result]

        return rows

    # === Private

    def _dumps_datetime(self, value):
        """If value is python datetime instance, dump it as string"""
        return value.strftime("%Y-%m-%d %H:%M:%S")

    def _serialize_insert(self, data):
        """Format insert dict values into strings"""
        keys = ",".join(["\"{}\"".format(key) for key in data.keys()])
        vals = ",".join(["%s" for k in data])

        return [keys, vals]

    def _serialize_update(self, data):
        """Format update dict values into string"""
        return "=%s,".join(data.keys()) + "=%s"

    def _select(self, table=None, fields=(), where=None, order=None, limit=None):
        """Run a select query"""

        table_sql = ".".join(['"' + t + '"' for t in table.split(".")])

        sql = "SELECT %s FROM %s" % (",".join(fields), table_sql)

        # where conditions
        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        # order
        if order:
            sql += " ORDER BY %s" % order[0]

            if len(order) > 1:
                sql += " %s" % order[1]

        # limit
        if limit:
            sql += " LIMIT %s" % limit

        return self.query(sql, where[1] if where and len(where) > 1 else None)

    def _select_join(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None):
        """Run an inner left join query"""

        table_sql = []
        for table in tables:
            table_sql.append(".".join(['"' + t + '"' for t in table.split(".")]))

        fields = [table_sql[0] + "." + f for f in fields[0]] + [table_sql[1] + "." + f for f in fields[1]]

        sql = "SELECT %s FROM %s LEFT JOIN %s ON (%s = %s)" % (
            ",".join(fields),
            table_sql[0],
            table_sql[1],
            table_sql[0] + "." + join_fields[0],
            table_sql[1] + "." + join_fields[1]
        )

        # where conditions
        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        # order
        if order:
            sql += " ORDER BY %s" % order[0]

            if len(order) > 1:
                sql += " " + order[1]

        # limit
        if limit:
            sql += " LIMIT %s" % limit

        return self.query(sql, where[1] if where and len(where) > 1 else None)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.end()
