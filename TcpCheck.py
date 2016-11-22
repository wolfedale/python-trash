#!/usr/bin/python
''' TcpCheck - simple script for checking TCP connections '''
#
# This script tests TCP connections with the specified host.
# You need to specify few values: host, port and timeout.
#
# (host) - String
# (port) - Int
# (timeout) - Int
#
# OK:0, WARNING:1, CRITICAL:2, UNKNOWN:3
#
# To run it you need to setup variables:
# export MAIL_FROM ...
# export MAIL_TO ...
#

import os
import sys
import argparse
import smtplib
import socket
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

TEMPFILE = "/tmp/TcpCheckFile.tmp"
DEBUG = True  # Setup True if you need to enable it


class TcpCheck(object):
    ''' Main Class '''
    def __init__(self, host, port, timeout, name):
        self.host = host
        self.port = int(port)
        self.timeout = float(timeout) / float(3.02)
        self.name = name
        if DEBUG is False:
            self.mailfrom = os.environ["MAIL_FROM"]
            self.mailto = os.environ["MAIL_TO"]

    def main(self):
        ''' main method, the logic is here '''
        if self.status() != 0:
            self.exit_code('CRITICAL')
        else:
            if os.path.exists(TEMPFILE):
                self.exit_code('RESOLVED')
            else:
                self.exit_code('OK')

    def exit_code(self, status):
        ''' exit code method '''
        if status == 'CRITICAL':
            self.status_save()
            self.send_mail('CRITICAL')
            sys.exit(2)
        if status == 'OK':
            if DEBUG is False:
                sys.exit(0)
            else:
                self.send_mail('OK')
                sys.exit(0)
        if status == 'RESOLVED':
            self.status_delete()
            self.send_mail('OK', 'RESOLVED')
            sys.exit(0)

    def status(self):
        ''' checking connection '''
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            return s.connect_ex((self.host, self.port))
        except:
            return 3

    @staticmethod
    def status_save():
        ''' creating temp file '''
        with open(TEMPFILE, 'w') as f:
            f.write(str(1))

    @staticmethod
    def status_delete():
        ''' deleting temp file '''
        if os.path.exists(TEMPFILE):
            try:
                os.remove(TEMPFILE)
            except Exception as error:
                raise CheckErrorException(
                    "Cannot delete file: %s. %s." %
                    (TEMPFILE, error))

    def send_mail(self, status, problem="PROBLEM"):
        ''' sending e-mail '''
        if DEBUG is False:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = "** %s Service Alert: %s is %s **" \
                             % (problem, self.name, status)
            msg['From'] = self.mailfrom
            msg['To'] = self.mailto
            text = "*** Sensu Monitoring ***\n\nHost:%s\nPort:%s\nState:%s\n" \
                   % (self.host, self.port, status)
            msg.attach(MIMEText(text, 'plain'))
            s = smtplib.SMTP('localhost')
            s.sendmail(self.mailfrom, self.mailto, msg.as_string())
            s.quit()
        else:
            print "** %s Service Alert: %s is %s **" \
                  % (problem, self.host, status)


class CheckErrorException(Exception):
    ''' Exception '''
    pass

if __name__ == '__main__':
    """
    We are starting script here.
    """
    parser = argparse.ArgumentParser(description="This plugin tests TCP \
                                     connections with the specified host.")
    parser.add_argument('-d', '--destination',
                        help='Destination host', required=True)
    parser.add_argument('-p', '--port',
                        help='External port', required=True)
    parser.add_argument('-t', '--timeout',
                        help='Timeout in second', required=True)
    parser.add_argument('-s', '--name',
                        help='Service name', required=True)
    parser.add_argument('-f', '--filelog',
                        help='File log', required=False)

    args = vars(parser.parse_args())
    d_set = args['destination']
    p_set = args['port']
    t_set = args['timeout']
    s_set = args['name']
    f_set = args['filelog']

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if DEBUG is False:
        handler = logging.FileHandler(f_set)
    else:
        handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)

    if d_set and p_set and t_set:
        start = TcpCheck(d_set, p_set, t_set, s_set)
        start.main()
