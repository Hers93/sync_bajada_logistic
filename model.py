import psycopg2
import psycopg2.extras

class conection(object):
    def __init__(self, dbHost, dbName, dbUser, dbPassword):
        self.conn_string = "host='" + dbHost + "' dbname='" + dbName + "' user='" + dbUser + "' password='" + dbPassword + "'"

        try:
            self.conn = psycopg2.connect(self.conn_string)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception as e:
            print("Se produjo un error al conectarse:\n" + str(e))
            raise

    def getData(self, dbFields, dbFrom, dbWhere='', debug=False):
        resultado = None
        if debug:
            dbQuery = 'Select ' + dbFields + ' From ' + dbFrom
            return dbQuery
        try:
            # print "Connected!\n"
            dbQuery = 'Select ' + dbFields + ' From ' + dbFrom
            if dbWhere != '':
                dbQuery = dbQuery + ' where ' + dbWhere
            print(dbQuery)
            self.cursor.execute(dbQuery)
            resultado = self.cursor.fetchall()
        except Exception as e:
            resultado = e
            self.cursor.execute('COMMIT;')
            self.cursor.rollback()
            self.cursor.committ()
        finally:
            return resultado

    def doUpdate(self, dbFrom, dbFields, dbWhere='', debug=False):
        updated_rows = 0
        try:
            dbQuery = 'Update ' + dbFrom + ' Set ' + dbFields
            if dbWhere != '':
                dbQuery = dbQuery + ' where ' + dbWhere
            if debug:
                updated_rows = dbQuery
            else:
                self.cursor.execute(dbQuery.encode('utf-8'))
                updated_rows = self.cursor.rowcount
                self.conn.commit()
                self.cursor.execute('COMMIT;')
        except Exception:
            self.cursor.execute('COMMIT;')
            updated_rows = 0
            raise
        finally:
            return updated_rows

    def doInsert(self, dbFrom, dbFields, values, dbWhere='', debug=False, notExists=False):
        inserted_rows = None
        try:
            if notExists:
                dbQuery = 'Insert into ' + dbFrom + ' ( ' + ', '.join([str(x) for x in dbFields]) + ' ) ' + ' SELECT ' + \
                          ', '.join([str(x) for x in values])
            else:
                dbQuery = 'Insert into ' + dbFrom + ' ( ' + ', '.join([str(x) for x in dbFields]) + ' ) ' + ' values ' + \
                          ' ( ' + ', '.join([str(x) for x in values]) + ' ) '
            if dbWhere != '':
                dbQuery = dbQuery + ' where ' + dbWhere
            if debug:
                inserted_rows = dbQuery.replace('\'\'', 'NULL')
            else:
                self.cursor.execute(dbQuery.replace('\'\'', 'NULL'))
                inserted_rows = self.cursor.rowcount
        except Exception:
            inserted_rows = None
        finally:
            return inserted_rows

    def committ(self):
        self.conn.commit()

    def closeConection(self):
        self.cursor.close()
        self.conn.close()