#!/usr/bin/python
''' FileMD5CheckTransfer script '''
#
# This script will do two things:
# - verify md5 checksum for files received from SFTP server
# - uncompress them
# - and send them to new SFTP server
#
# Also:
# - get reports
# - created md5 checksum for them
#

import hashlib
import os
import tarfile
import logging
import datetime
import shutil
import sys
import argparse
from random import choice
from string import ascii_uppercase
from string import digits
import paramiko

# temporary directory's
TEMP_DIR = ''
TEMP_DIR_UNCOMPRESS = ''

# backup directory
BACKUP_PATH = ''

# path to the log file
N_LOGFILE = ''

# SFTP settings
NODETWO_PORT = 22
NODETWO_IP = ''
NODETWO_USERNAME = ''
NODETWO_PUBLIC_KEY = ''
NODEONE_PORT = 22
NODEONE_IP = ''
NODEONE_USERNAME = ''
NODEONE_PUBLIC_KEY = ''


class Logs(object):
    ''' Logs class for dealing with logfile and levels '''
    def __init__(self):
        ''' init method '''
        logging.basicConfig(filename=N_LOGFILE,
                            level=logging.INFO,
                            format='%(asctime)s %(levelname)s %(message)s')


class Files(object):
    '''
    Files will deal with files.
    :param file_type: type of files that we want to check
    :param nodetwo_dir: this is remote dir on nodetwo sftp server
    :param nodeone_dir: this is a remote dir on nodeone sftp
    '''
    def __init__(self, file_type, nodetwo_dir, nodeone_dir):
        ''' initialize few settings '''
        self.queue = self.__random_string()
        logging.info('%s Started', self.queue)
        self.temp = TEMP_DIR + self.queue + "/"
        self.temp_uncompress = TEMP_DIR_UNCOMPRESS + self.queue + "/"
        self.file_type = file_type
        self.nodetwo_remote_path = nodetwo_dir
        self.nodeone_remote_path = nodeone_dir

        if not os.path.exists(self.temp):
            self.temp_dir(self.temp)

    def main(self, prefix):
        ''' main method for all steps '''
        if prefix == 'nodetwo':

            if not os.path.exists(self.temp_uncompress):
                self.temp_dir(self.temp_uncompress)

            logging.info(
                '%s Connecting to nodeone SFTP server.',
                self.queue)
            b = SFTPConnect(NODEONE_IP,
                            NODEONE_USERNAME,
                            NODEONE_PUBLIC_KEY,
                            self.queue, NODEONE_PORT)

            # Download new files
            for i in b.sftp_list_files(self.nodetwo_remote_path,
                                       self.file_type):
                b.sftp_get_file(i, self.temp)

                # Backup new files
                self.__backup_file(i, BACKUP_PATH)

                # Delete file from SFTP Server
                # Hash it if you want to keep them.
                b.sftp_remove_file(i)

            # Check MD5 sum of new files
            for root, dirs, files in os.walk(self.temp):
                for i in files:
                    if self.__compare_md5(self.temp + i):
                        self.__uncompress(self.temp + i)

            # Copy OB files to Traveller
            if self.file_type.startswith('OB'):
                self.__ob_to_traveller()

            # Upload uncompressed files
            for root, dirs, files in os.walk(self.temp_uncompress):
                for i in files:
                    logging.info(
                        '%s Uploading file: %s to nodeone SFTP server',
                        self.queue, i)
                    b.sftp_upload_file(self.temp_uncompress + i,
                                       self.nodeone_remote_path + i)

            # Deleted old temp files
            try:
                logging.info(
                    '%s Deleting directory: %s and %s',
                    self.queue, self.temp, self.temp_uncompress)
                shutil.rmtree(self.temp)
                shutil.rmtree(self.temp_uncompress)
                b.sftp_close()
            except Exception as error:
                logging.info(
                    "%s Cannot delete: %s or %s error: %s",
                    self.queue, self.temp, self.temp_uncompress, error)

        elif prefix == "nodeone":
            # Connect to SFTP nodeone
            logging.info(
                '%s Connecting to nodeone SFTP server.',
                self.queue)
            b = SFTPConnect(NODEONE_IP,
                            NODEONE_USERNAME,
                            NODEONE_PUBLIC_KEY,
                            self.queue, NODEONE_PORT)

            # Download new files/reports
            for i in b.sftp_list_files(self.nodeone_remote_path,
                                       self.file_type):
                b.sftp_get_file(i, self.temp)

                # Backup new files
                self.__backup_file(i, BACKUP_PATH)

                # Delete file from SFTP server
                b.sftp_remove_file(i)

            # Create MD5 for file
            for root, dirs, files in os.walk(self.temp):
                for i in files:
                    self.__create_md5(self.temp + i)

            # Connect to SFTP nodetwo
            logging.info('%s Connecting to nodetwo server.', self.queue)
            s = SFTPConnect(NODETWO_IP,
                            NODETWO_USERNAME,
                            NODETWO_PUBLIC_KEY,
                            self.queue, NODETWO_PORT)

            # Upload new files to nodetwo SFTP server
            for root, dirs, files in os.walk(self.temp):
                for i in files:
                    logging.info(
                        '%s Uploading file: %s to: %s',
                        self.queue, i, self.nodetwo_remote_path)
                    s.sftp_upload_file(self.temp + i, self.nodetwo_remote_path + i)
            try:
                logging.info('%s Deleting %s', self.queue, self.temp)
                shutil.rmtree(self.temp)
            except Exception as error:
                logging.info(
                    "%s Cannot delete: %s error: %s",
                    self.queue, self.temp, error)
        else:
            print "Somethings went wrong on the main() function."
            sys.exit(1)
        logging.info('%s Finished.', self.queue)

    def __check_md5(self, filename):
        '''
        This function check md5 from file and return string.
        '''
        logging.info('%s check_md5() for %s', self.queue, filename)
        try:
            with open(filename, 'rb') as f:
                m = hashlib.md5()
                while True:
                    data = f.read(8192)
                    if not data:
                        break
                    m.update(data)
                return m.hexdigest()
        except Exception as error:
            logging.info(
                "%s Cannot check_md5 on %s error: %s",
                self.queue, filename, error)

    def __read_md5(self, filename):
        '''
        This function read md5 sum from file and return string.
        '''
        logging.info('%s read_md5() for %s', self.queue, filename)
        localfile = filename.split('.tar.bz2')[0] + '.md5'
        if os.path.exists(localfile):
            try:
                with open(localfile, 'r') as f:
                    return f.read().split(' ')[0].rstrip()
            except Exception as error:
                logging.info(
                    "%s Cannot read_md5 on %s error: %s",
                    self.queue, filename, error)
        else:
            logging.info(
                '%s read_md5() for %s - No md5 file!',
                self.queue, localfile)
            return False

    def __compare_md5(self, filename):
        '''
        This function compare md5 sum and return True or False
        '''
        if 'md5' not in filename:
            logging.info('%s compare_md5() for %s', self.queue, filename)
            if self.__check_md5(filename) == self.__read_md5(filename):
                logging.info(
                    '%s check_md5 == read_md5 (%s)',
                    self.queue, filename)
                return True
            else:
                logging.info(
                    '%s check_md5 != read_md5 (%s)',
                    self.queue, filename)
                return False

    def __uncompress(self, filename):
        '''
        This function uncompress compressed files
        '''
        logging.info('%s uncompress() for %s', self.queue, filename)
        if tarfile.is_tarfile(filename):
            try:
                tar = tarfile.open(filename)
                tar.extractall(self.temp_uncompress)
                tar.close()
            except Exception as error:
                logging.info(
                    "%s Cannot uncompress file: %s error: %s",
                    self.queue, filename, error)
        else:
            logging.info("%s No such file: %s", self.queue, filename, error)
            return False

    def __ob_to_traveller(self):
        '''
        This function do the copy of OB file to Traveller"
        '''
        logging.info('%s ob_to_traveller()', self.queue)
        for root, dirs, files in os.walk(self.temp_uncompress):
            for i in files:
                try:
                    newfile = i.replace('OB', 'Traveller_Userlist')
                    logging.info('%s convert %s to %s', self.queue, i, newfile)
                    shutil.copy2(self.temp_uncompress + i, self.temp_uncompress + newfile)
                    with open(self.temp_uncompress + newfile) as fin:
                        lines = fin.readlines()
                    lines[0] = lines[0].replace('OB', 'Traveller_Userlist')
                    with open(self.temp_uncompress + newfile, 'w') as fout:
                        for line in lines:
                            fout.write(line)
                except Exception as error:
                    logging.info(
                        "%s Cannot ob_to_traveller(), error %s",
                        self.queue, error)

    def __create_md5(self, filename):
        '''
        This function create md5 sum from file.
        '''
        logging.info('%s create_md5() for %s', self.queue, filename)
        if 'md5' not in filename:
            localfile = filename.split('.csv')[0] + '.md5'
            md5_content = self.__check_md5(filename) + " " + filename + "\n"
            if os.path.exists(localfile):
                return False
            else:
                try:
                    with open(localfile, 'w') as f:
                        write_data = f.write(md5_content)
                except Exception as error:
                    logging.info(
                        "%s Cannot save md5 to file: %s, %s",
                        self.queue, localfile, error)

    def __backup_file(self, filename, backup_path):
        '''
        This function create backup files
        '''
        date_now = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M")
        archive_dir = backup_path + date_now
        logging.info(
            '%s backup_file() %s in %s',
            self.queue, filename, archive_dir)
        archive_dir = backup_path + date_now
        self.temp_dir(archive_dir)
        try:
            shutil.copy2(self.temp + filename, archive_dir + "/" + filename)
        except Exception as error:
            logging.info(
                "%s Cannot copy file: %s, %s error: %s",
                self.queue, filename, archive_dir, error)

    def __delete_file(self, filename):
        '''
        This function delete file
        '''
        logging.info('%s delete_file() %s', self.queue, filename)
        if os.path.exists(filename):
            try:
                os.remove(filename)
            except Exception as error:
                logging.info(
                    "%s Cannot delete file: %s %s",
                    self.queue, filename, error)
        else:
            return False

    @staticmethod
    def __random_string():
        '''
        This function is generating random string
        '''
        try:
            return ''.join(choice(ascii_uppercase)+choice(digits) for i in range(7))
        except Exception as error:
            logging.info("Cannot generate random string: %s", error)

    @staticmethod
    def temp_dir(temp="temp"):
        """
        This function create directory
        """
        if not os.path.exists(temp):
            try:
                logging.info('temp_dir() creating directory: %s', temp)
                os.makedirs(temp)
            except Exception as error:
                logging.info(
                    "Cannot create temp_dir: %s error: %s",
                    temp, error)
        else:
            return False


class FileSendException(Exception):
    ''' Dealing with exceptions '''
    pass


class SFTPConnect(object):
    '''
    SFTPConnect will deal with connection to SFTP server.
    '''
    def __init__(self, ip, username, public_key, queue, port=22):
        ''' init method '''
        self.queue = queue
        self.port = port
        self.hostname = ip
        self.username = username
        self.transport = paramiko.Transport((self.hostname, self.port))

        try:
            self.key = paramiko.RSAKey.from_private_key_file(public_key)
            self.transport.connect(username=self.username,
                                   pkey=self.key)
            self.sftp = paramiko.SFTP.from_transport(self.transport)
        except Exception as error:
            logging.info(
                "%s Cannot connect to SFTP server: %s %s %s %s",
                self.queue, ip, username, public_key, error)
            sys.exit(1)

    def sftp_list_files(self, directory, file_type):
        '''
        This function get a list of files.
        '''
        logging.info(
            '%s sftp_list_files() %s%s',
            self.queue, directory, file_type)
        count = True
        try:
            self.sftp.chdir(directory)
        except Exception as error:
            logging.info(
                "%s Failed to list files in %s, error: %s",
                self.queue, directory, error)
        else:
            for i in self.sftp.listdir():
                if i.startswith(file_type):
                    count = False
                    yield i
            if count:
                logging.info("%s sftp_list_files() No such file: %s%s",
                             self.queue, directory, file_type)
                sys.exit(1)

    def sftp_get_file(self, filename, local_dir):
        '''
        This function download file
        '''
        logging.info(
            '%s sftp_get_file() downloading: %s%s',
            self.queue, local_dir, filename)
        try:
            if os.path.exists(local_dir):
                self.sftp.get(filename, local_dir+filename)
            else:
                # temp_dir(local_dir)
                Files.temp_dir(local_dir)
                self.sftp.get(filename, local_dir+filename)
        except Exception as error:
            logging.info(
                "%s Cannot sftp_get_file(): %s. Error: %s",
                self.queue, filename, error)

    def sftp_upload_file(self, local_path, remote_path):
        '''
        This function upload file
        '''
        logging.info(
            '%s sftp_upload_file() %s %s',
            self.queue, local_path, remote_path)
        try:
            self.sftp.put(local_path, remote_path)
        except Exception as error:
            logging.info(
                "%s Problem with local: %s or remote %s path. Error: %s",
                self.queue, local_path, remote_path, error)

    def sftp_remove_file(self, filename):
        '''
        This function remove file
        '''
        logging.info('%s sftp_remove_file() removing remote file: %s', self.queue, filename)
        try:
            self.sftp.remove(filename)
        except Exception as error:
            logging.info("%s queue, error: %s", self.queue, error)

    def sftp_close(self):
        '''
        This function close connection to SFTP server
        '''
        try:
            self.sftp.close()
        except Exception as error:
            logging.info(
                "%s Cannot close connection. Error: %s",
                self.queue, error)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="nodetwo - scheduled task framework - md5 check,"
        "archival and zip functionality."
        "Script can:"
        " - download new files from nodetwo SFTP Server."
        " - check md5sum on each file"
        " - upload files to nodeone SFTP Server"
        " - download new files from nodeone SFTP Server"
        " - create md5sum for them"
        " - upload new files to nodetwo SFTP Server")

    parser.add_argument('-s', '--nodetwo_dir',
                        help='Path for dir in nodetwo server',
                        required=True)
    parser.add_argument('-b', '--nodeone_dir',
                        help='Path for dir in nodeone server',
                        required=True)
    parser.add_argument('-t', '--type',
                        help='Type of files',
                        required=True)
    parser.add_argument('-d', '--destination',
                        help='Destination/way: nodetwo or nodeone',
                        required=True)

    args = vars(parser.parse_args())

    if args['nodetwo_dir'] and args['nodeone_dir']:
        if args['type'] and args['destination']:
            start = Logs()
            run = Files(args['type'],
                        args['nodetwo_dir'],
                        args['nodeone_dir'])
            run.main(args['destination'])
