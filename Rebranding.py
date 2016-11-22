#!/usr/bin/python
#
# Please read help() for more details:
# ./script -h
#
# Before we run it we need to export PGPASSWORD with the
# proper credentials.
#

import os
import sys
import argparse
import psycopg2

TABLES = ['table1', 'table2', 'table3'] # specify your own tables


class CaseCode(object):
    '''
    Main class.
    :param user: username for postgresql
    :param password: password for postgresql
    :param database: database where we need to connect
    '''
    def __init__(self, user, database, debug=False):
        self.user = user
        self.database = database
        self.password = os.environ['PGPASSWORD']
        self.port = 5432
        self.hostname = 'localhost'
        self.debug = debug

    def main(self):
        ''' Main method '''
        # Checking if it's dry-run mode.
        if self.debug:
            print "Running in DRY-RUN mode."

        # Connect to the database
        try:
            conn = psycopg2.connect(dbname=self.database,
                                    user=self.user,
                                    host=self.hostname,
                                    password=self.password)
            cur = conn.cursor()
        except Exception as error:
            print "Cannot connect to database: %s" % error
            sys.exit(1)

        # Iterate over tables that we want to check
        for table in TABLES:
            print "\nWorking on table: %s" % table

            # Main query for each table
            rows = self.query_select(table, cur)

            # Check if there are records to parse
            if not rows:
                print " -> no records to replace"
            else:
                for row in rows:
                    self.query_update(row, table, cur)

                # Comit database query
                conn.commit()

        # Close database connection
        cur.close()
        conn.close()

    @staticmethod
    def query_select(table, cur):
        try:
            cur.execute(
                """SELECT id, email FROM %s
                WHERE email SIMILAR TO '%%my_domain%%'""" % (table))
            rows = cur.fetchall()
        except Exception as error:
            print "Cannot execute query: %s" % error
            sys.exit(1)
        return rows

    def query_update(self, row, tablename, cur):
        '''
        method for executing queries
        row: row from the database
        tablename: table name from TABLES
        cur: conn object
        '''
        try:
            table_id = row[0]
            table_email = row[1]
            table_new_email = self.change_mail(table_email)

            print "UPDATE %s SET email='%s' WHERE id='%s';" % (tablename, table_new_email, table_id)

            # Run it only when debug is False
            if self.debug is False:
                cur.execute(
                    """UPDATE %s SET email=('%s')
                       WHERE id=('%s')""" % (tablename, table_new_email, table_id))
        except Exception as error:
            print "Cannot execute query: %s on %s, %s" % (row, tablename, error)
            sys.exit(1)

    @staticmethod
    def change_mail(mail):
        '''
        method for replacing old e-mail with new the new one
        mail: old mail string
        '''
        try:
            return mail.replace('old_domain', 'new_domain')
        except Exception as error:
            print "Cannot change mail: %s, %s" % (mail, error)
            sys.exit(1)

if __name__ == '__main__':
    '''
    We start sript here, passing some args to it
    Remember to export PGPASSWORD
    '''
    parser = argparse.ArgumentParser(description="Rebranding: domain")
    parser.add_argument('-u', '--user',
                        help='PostgreSQL user', required=True)
    parser.add_argument('-d', '--database',
                        help='PostgreSQL database', required=True)
    parser.add_argument('-t', '--dryrun', action='store_true',
                        help='Dry run mode', required=False)

    args = vars(parser.parse_args())

    if args['user'] and args['database']:
        start = CaseCode(args['user'], args['database'], args['dryrun'])
        start.main()
