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

from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime
import logging
import traceback

import psycopg2


psycho_logger = logging.getLogger(__name__)


class ConfigError(Exception):
    def __init__(self, message):
        self.message = message


class Psycho:
    connection = None
    cursor = None
    config = None

    def __init__(self, **kwargs):
        try:
            self.schema = kwargs.pop("schema")
        except KeyError:
            raise ConfigError("You must specify a default schema.")

        self.config = kwargs
        self.config["keep_alive"] = kwargs.get("keep_alive", False)
        self.config["charset"] = kwargs.get("charset", "utf8")
        self.config["host"] = kwargs.get("host", "localhost")
        self.config["port"] = kwargs.get("port", 3306)
        self.config["autocommit"] = kwargs.get("autocommit", False)

        try:
            self.connect()
        except psycopg2.OperationalError:
            pass

    @contextmanager
    def atomic(self):
        """
        Wraps a series of database transactions to make them atomic.
        If any of the transactions throws an exception, the connection is closed,
        rolling back any of the changes that were made. Otherwise, the changes get
        committed.

        Additionally, if atomic_commit=False kwarg is passed to the Psycho
        constructor, every atomic transaction will be rolled back upon completion.
        This is useful for testing application logic modularly without committing
        to the database.
        """
        self.connect()

        try:
            yield self

        except Exception as e:
            self.end()
            raise e

        else:
            self.commit()

    def connect(self):
        """Connect to the postgresql server"""

        try:
            self.connection = psycopg2.connect(
                database=self.config['database'],
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                autocommit=self.config['autocommit'],
            )

        except Exception as exc:
            self.log('error', 'DB: Application failed to connect to database: {}'.format(str(exc)), exc_info=exc)
            raise exc

    @contextmanager
    def named_logger(self, logger):
        self.logger = logger
        try:
            yield self

        except Exception:
            raise

        finally:
            delattr(self, 'logger')

    def log(self, level, message, **kwargs):
        if 'extra' not in kwargs:
            kwargs['extra'] = {}

        if hasattr(self, 'log_data'):
            kwargs['extra'].update(self.log_data)

        logger = getattr(self, 'logger', psycho_logger)
        getattr(logger, level.lower())(message, **kwargs)

    def get_one(self, table=None, fields='*', where=None, order=None, limit=1, schema=None):
        """
        Get a single result

        table = (str) table_name
        fields = (field1, field2 ...) list of fields to select
        where = ("parameterizedstatement", [parameters])
                        eg: ("id=%s and name=%s", [1, "test"])
        order = [field, ASC|DESC]
        limit = limit
        """
        try:
            query = self._select(table, fields, where, order, limit, schema)
            cursor = query['cursor']
            result = cursor.fetchone()

        except psycopg2.DatabaseError:
            try:
                self.connect()

            except psycopg2.DatabaseError as exc:
                self.log(
                    'error',
                    'DB: Application failed to connect to Database: {}'.format(str(exc)),
                    extra={'postgresql': {
                        'query': query['sql'],
                        'params': str(query['params']),
                        'filtered_stack': [
                            line for line in traceback.format_stack() if 'python3' not in line],
                    }},
                    exc_info=exc)
                raise exc

            else:
                query = self._select(table, fields, where, order, limit, schema)
                cursor = query['cursor']
                result = cursor.fetchone()

        columns = [f[0] for f in cursor.description]
        string_result = str(result)
        self.log('info', 'DB: Executed query.', extra={
            'postgresql': {
                'query': query['sql'],
                'params': str(query['params']),
                'result_columns': str(columns),
                'result': string_result[:1000] if len(string_result) > 1000 else string_result,
                'filtered_stack': [
                    line for line in traceback.format_stack() if 'python3' not in line],
                'execution_time': str(datetime.utcnow() - query['start'])
            }
        })

        row = None
        if result:
            Row = namedtuple("Row", columns)
            row = Row(*result)

        return row

    def get_all(self, table=None, fields='*', where=None, order=None, limit=None, schema=None):
        """
        Get all results

        table = (str) table_name
        fields = (field1, field2 ...) list of fields to select
        where = ("parameterizedstatement", [parameters])
                        eg: ("id=%s and name=%s", [1, "test"])
        order = [field, ASC|DESC]
        limit = limit
        """
        try:
            query = self._select(table, fields, where, order, limit)
            cursor = query['cursor']
            result = cursor.fetchall()

        except psycopg2.DatabaseError:
            try:
                self.connect()

            except psycopg2.DatabaseError as exc:
                self.log(
                    'error',
                    'DB: Application failed to connect to Database: {}'.format(str(exc)),
                    extra={'postgresql': {
                        'query': query['sql'],
                        'params': str(query['params']),
                        'filtered_stack': [
                            line for line in traceback.format_stack() if 'python3' not in line],
                    }},
                    exc_info=exc
                )
                raise exc

            else:
                query = self._select(table, fields, where, order, limit)
                cursor = query['cursor']
                result = cursor.fetchall()

        columns = [f[0] for f in cursor.description]
        string_result = str(result)
        self.log('info', 'DB: Executed query.', extra={
            'postgresql': {
                'query': query['sql'],
                'params': str(query['params']),
                'result_columns': str(columns),
                'result': (string_result[:2000] + '...') if len(string_result) > 2000 else string_result,
                'filtered_stack': [
                    line for line in traceback.format_stack() if 'python3' not in line],
                'execution_time': str(datetime.utcnow() - query['start'])
            }
        })

        return self.get_rows(query, result)

    def left_join(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None, schemas=()):
        """
        Run an inner left join query

        tables = tuple(str, str) (table1, table2)
        fields = ([fields from table1], [fields from table 2])  # fields to select
        join_fields = (field1, field2)  # fields to join. field1 belongs to table1 and field2 belongs to table 2
        where = ("parameterizedstatement", [parameters])
                        eg: ("id=%s and name=%s", [1, "test"])
        order = [field, ASC|DESC]
        limit = limit
        schemas = tuple(str, str) (schema1, schema2)
        """
        try:
            query = self._select_join(tables, fields, join_fields, where, order, limit, schemas)
            cursor = query['cursor']
            result = cursor.fetchall()

        except psycopg2.DatabaseError:
            try:
                self.connect()

            except psycopg2.DatabaseError as exc:
                self.log(
                    'error',
                    'DB: Application failed to connect to Database: {}'.format(str(exc)),
                    extra={'postgresql': {
                        'query': query['sql'],
                        'params': str(query['params']),
                        'filtered_stack': [
                            line for line in traceback.format_stack() if 'python3' not in line]
                    }})
                raise

            else:
                query = self._select_join(tables, fields, join_fields, where, order, limit, schemas)
                cursor = query['cursor']
                result = cursor.fetchall()

        return self.get_rows(query, result)

    def insert(self, table, data, schema=None, returning=None, close=True):
        """Insert a record"""
        if schema is None:
            schema = self.schema

        query = self._serialize_insert(data)

        sql = "INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s)" % (schema, table, query[0], query[1])

        if returning is not None:
            sql += " RETURNING (%s)" % ",".join(returning)

        # Check data values for python datetimes
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = self._dumps_datetime(value)

        params = list(data.values())
        try:
            cursor_obj = self.query(sql, params)

        except Exception as exc:
            self.log(
                'error',
                'DB: Query failed to insert: {}'.format(str(exc)),
                extra={'postgresql': {
                    'query': sql,
                    'params': str(params),
                    'filtered_stack': [
                        line for line in traceback.format_stack() if 'python3' not in line]
                }},
                exc_info=exc
            )
            raise exc

        cursor = cursor_obj['cursor']
        if returning is not None:
            try:
                return_val = cursor.fetchone()

            except Exception as exc:
                self.log(
                    'error',
                    'DB: INSERT query failed to return specified fields ({}): {}'.format(
                        str(returning), str(exc)),
                    extra={'postgresql': {
                        'query': sql,
                        'params': str(params),
                        'filtered_stack': [
                            line for line in traceback.format_stack() if 'python3' not in line]
                    }},
                    exc_info=exc)
                raise exc

            else:
                self.log(
                    'info',
                    'DB: Fetched return value for INSERT query.',
                    extra={'postgresql': {
                        'query': sql,
                        'params': str(params),
                        'result': str(return_val),
                        'filtered_stack': [
                            line for line in traceback.format_stack() if 'python3' not in line]}})

        if close:
            cursor.close()

        return return_val if returning is not None else cursor

    def update(self, table, data, where=None, schema=None, close=True):
        """Insert a record"""
        if schema is None:
            schema = self.schema

        query = self._serialize_update(data)

        sql = "UPDATE \"%s\".\"%s\" SET %s" % (schema, table, query)

        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        # Check data values for python datetimes
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = self._dumps_datetime(value)

        params = list(data.values()) + where[1] if where and len(where) > 1 else data.values()
        cursor_obj = self.query(sql, params)
        cursor = cursor_obj['cursor']
        if close:
            cursor.close()
        return cursor

    def insert_or_update(self, table, data, keys, schema=None, close=True):
        if schema is None:
            schema = self.schema

        insert_data = data.copy()

        data = {k: data[k] for k in data if k not in keys}

        insert = self._serialize_insert(insert_data)

        update = self._serialize_update(data)

        sql = "INSERT INTO \"%s\".\"%s\" (%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s" % \
              (schema, table, insert[0], insert[1], update)

        # Check values for python datetimes
        values = insert_data.values() + data.values()
        for idx, value in enumerate(values):
            if isinstance(value, datetime):
                values[idx] = self._dumps_datetime(value)

        cursor_obj = self.query(sql, list(values))
        cursor = cursor_obj['cursor']
        if close:
            cursor.close()
        return cursor

    def delete(self, table, where=None, schema=None, close=True):
        """Delete rows based on a where condition"""
        if schema is None:
            schema = self.schema

        sql = "DELETE FROM \"%s\".\"%s\"" % (schema, table)

        if where and len(where) > 0:
            sql += " WHERE %s" % where[0]

        cursor_obj = self.query(sql, where[1] if where and len(where) > 1 else None)
        cursor = cursor_obj['cursor']
        if close:
            cursor.close()
        return cursor

    def query(self, sql, params=None):
        """Run a raw query"""
        # check if connection is alive. if not, reconnect
        for count in range(0, 5):
            try:
                cursor = self.connection.cursor()
                start = datetime.utcnow()
                cursor.execute(sql, params)

            except (psycopg2.IntegrityError, psycopg2.ProgrammingError) as exception:
                self.log(
                    'error',
                    'DB: Query failed: {}.'.format(str(exception)),
                    extra={'postgresql': {
                        'query': sql,
                        'params': str(params),
                        'filtered_stack': [
                            line for line in traceback.format_stack() if 'python3' not in line]
                    }},
                    exc_info=exception)
                raise exception

            except (psycopg2.DatabaseError, AttributeError) as exception:
                try:
                    self.connect()

                except (psycopg2.DatabaseError) as exception:
                    self.log(
                        'error',
                        'DB: Application failed to connect to database: {}.'.format(str(exception)),
                        extra={'postgresql': {
                            'query': sql,
                            'params': str(params),
                            'filtered_stack': [
                                line for line in traceback.format_stack() if 'python3' not in line]
                        }},
                        exc_info=exception)
                    raise exception

                else:
                    cursor.close()
                    continue

            else:
                break

        return {'cursor': cursor, 'start': start}

    def commit(self):
        """Commit a transaction (transactional engines like InnoDB require this)"""
        return self.connection.commit()

    def is_open(self):
        """Check if the connection is open"""
        return self.connection.open

    def end(self):
        """Kill the connection"""
        self.connection.close()

    def get_rows(self, query, result=None):
        rows = []
        cursor = query['cursor']
        if not result:
            for count in range(0, 5):
                try:
                    result = cursor.fetchall()

                except (psycopg2.DatabaseError, psycopg2.ProgrammingError):
                    try:
                        self.connect()

                    except (psycopg2.DatabaseError, psycopg2.ProgrammingError) as exc:
                        self.log(
                            'error',
                            'DB: Application failed to connect to database: {}'.format(str(exc)),
                            extra={'postgresql': {
                                'query': query['sql'],
                                'params': str(query['params']),
                                'filtered_stack': [
                                    line for line in traceback.format_stack() if 'python3' not in line]
                            }},
                            exc_info=exc
                        )
                        raise exc

                    else:
                        continue

                except Exception as exc:
                    self.log(
                        'error',
                        'DB: Query failed: {}'.format(str(exc)),
                        extra={'postgresql': {
                            'query': query['sql'],
                            'params': str(query['params']),
                            'filtered_stack': [
                                line for line in traceback.format_stack() if 'python3' not in line],
                        }},
                        exc_info=exc)
                    raise exc

                else:
                    break

        if result:
            Row = namedtuple("Row", [f[0] for f in cursor.description])
            rows = [Row(*r) for r in result]

        # Close the cursor
        cursor.close()

        return rows

    def query_rows(self, sql, params=None):
        query = self.query(sql, params)
        rows = self.get_rows(query)
        columns = [f[0] for f in query['cursor'].description]
        string_result = str(rows)
        self.log(
            'info',
            'DB: Executed query.',
            extra={'postgresql': {
                'query': sql,
                'params': str(params),
                'result_columns': str(columns),
                'result': string_result[:1000] if len(string_result) > 1000 else string_result,
                'filtered_stack': [
                    line for line in traceback.format_stack() if 'python3' not in line],
                'execution_time': str(datetime.utcnow() - query['start'])}})
        return rows

    def query_dict(self, sql, params=None):
        return self.row_to_dict(self.query_rows(sql, params))

    def row_to_dict(self, rows):
        if rows is None:
            return None

        elif type(rows) == list:
            row_list = []
            for row in rows:
                row_list.append(self._convert_row_to_dict(row))
            return row_list

        return self._convert_row_to_dict(rows)

    # === Private

    def _convert_row_to_dict(self, row):
        row_dict = {}
        for field in row._fields:
            row_dict[field] = getattr(row, field)
        return row_dict

    def _dumps_datetime(self, value):
        """If value is python datetime instance, dump it as string"""
        return value.strftime("%Y-%m-%d %H:%M:%S")

    def _serialize_insert(self, data):
        """Format insert dict values into strings"""
        keys = ",".join(["{}".format(key) for key in data.keys()])
        vals = ",".join(["%s" for k in data])

        return [keys, vals]

    def _serialize_update(self, data):
        """Format update dict values into string"""
        return "=%s,".join(data.keys()) + "=%s"

    def _select(self, table=None, fields=(), where=None, order=None, limit=None, schema=None):
        """Run a select query"""
        if schema is None:
            schema = self.schema

        sql = "SELECT %s FROM \"%s\".\"%s\"" % (",".join(fields), schema, table)

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

        params = where[1] if where and len(where) > 1 else None
        cursor_obj = self.query(sql, params)
        cursor_obj.update({'sql': sql, 'params': params})
        return cursor_obj

    def _select_join(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None, schemas=()):
        """Run an inner left join query"""
        if not schemas:
            schemas = (self.schema, self.schema)

        fields = ["\"" + schemas[0] + "\".\"" + tables[0] + "\"." + f for f in fields[0]] + \
                 ["\"" + schemas[1] + "\".\"" + tables[1] + "\"." + f for f in fields[1]]

        sql = "SELECT %s FROM \"%s\".\"%s\" LEFT JOIN \"%s\".\"%s\" ON (%s = %s)" % (
            ",".join(fields),
            schemas[0],
            tables[0],
            schemas[1],
            tables[1],
            "\"" + schemas[0] + "\".\"" + tables[0] + "\"." + join_fields[0],
            "\"" + schemas[1] + "\".\"" + tables[1] + "\"." + join_fields[1]
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

        query = self.query(sql, where[1] if where and len(where) > 1 else None)
        return query

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.end()
