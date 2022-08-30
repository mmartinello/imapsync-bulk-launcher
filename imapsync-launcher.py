#!/usr/bin/env python3

"""
Imapsync Launcher
Launch a bunch of Imapsync processes

authors:
    Mattia Martinello - mattia@mattiamartinello.com
"""

# Built in constants
_VERSION = "1.0"
_VERSION_DESCR = "Imapsync Launcher"
_LOG_FILE_PATH = "imapsync-launcher.log"
_IMAPSYNC_BIN_PATH = "imapsync"

import argparse
from unittest import skip
import colorlog
import csv
import logging
import logging.handlers
import os
import shutil
import sys

from rich import print
from rich.prompt import Confirm


# Program main class
class ImapsyncLauncher:
    def __init__(self):
        # Setup logger
        self.setup_logger("imapsync-launcher", _LOG_FILE_PATH)

    # Setup logger
    def setup_logger(self, logger_name, log_file_path, level=logging.INFO):
        # Logging instance
        self.logger = logging.getLogger(logger_name)

        # File handler
        formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
        handler = logging.handlers.RotatingFileHandler(log_file_path,
                                                       mode='w',
                                                       backupCount=1)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Stream handler (colored logs on console)
        handler = colorlog.StreamHandler()
        coloredformat = colorlog.ColoredFormatter('%(log_color)s%(asctime)s %(name)s %(levelname)s %(message)s')
        handler.setFormatter(coloredformat)
        self.logger.addHandler(handler)

        self.logger.setLevel(level)

    # Add and instance command arguments
    def add_arguments(self, parser):
        parser.add_argument(
            '-V', '--version',
            action='version',
            version = '%(prog)s v{} - {}'.format(_VERSION, _VERSION_DESCR)
        )
        parser.add_argument(
            '-v', '--debug',
            action="store_true",
            help='Print debugging info to console.'
        )
        parser.add_argument(
            '-u', '--user-file',
            type=str,
            default='users.csv',
            dest='user_file_path',
            help='The path of the CSV file containing the list of users (default: users.csv)'
        )
        parser.add_argument(
            '-s', '--skip-first-line',
            default=False,
            dest='skip_first_line',
            action="store_true",
            help='Skip the first line of the CSV file (default: False).'
        )
        parser.add_argument(
            '-i', '--imapsync-path',
            default='imapsync',
            dest='imapsync_path',
            help='The path of the imapsync command (default: imapsync).'
        )
        parser.add_argument(
            '-c', '--concurrency',
            default=None,
            dest='concurrency',
            help='The maximum number of concurrent jobs to execute (default: the number of CPU cores).'
        )

    # Manage arguments: do things based on configured command arguments
    def manage_arguments(self, args):        
        # Check if user file exists
        self.user_file_path = getattr(args, 'user_file_path')
        if not os.path.exists(self.user_file_path):
                msg = "User file '{}' does not exists!"
                msg = msg.format(self.user_file_path)
                self.logger.error(msg)
                sys.exit(1)

        self.skip_first_line = getattr(args, 'skip_first_line')
        self.imapsync_path = getattr(args, 'imapsync_path', 'imapsync')

    # Parse the user CSV file
    def parse_csv_file(self, csv_file_path, skip_first_line=False):
        with open(csv_file_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            current_line_n = 0

            users = {}
            for row in csv_reader:
                current_line_n += 1

                if skip_first_line and current_line_n == 1:
                    continue
                else:
                    source_user = row[0]
                    source_host = row[1]
                    source_port = row[2]
                    source_ssl = self.value2bool(row[3])
                    source_password = row[4]
                    dest_user = row[5]
                    dest_host = row[6]
                    dest_port = row[7]
                    dest_ssl = self.value2bool(row[8])
                    dest_password = row[9]
                    extra_params = row[10]
                    
                    user = {
                        'source_user': source_user,
                        'source_host': source_host,
                        'source_port': source_port,
                        'source_ssl': source_ssl,
                        'source_password': source_password,
                        'dest_user': dest_user,
                        'dest_host': dest_host,
                        'dest_port': dest_port,
                        'dest_ssl': dest_ssl,
                        'dest_password': dest_password,
                        'extra_params': extra_params
                    }

                    # Add the user to the to users dictionary
                    users[source_user] = user

            # Sort users by username and return
            users = dict(sorted(users.items()))

            return users

    # Return boolean representation of a given CSV value
    def value2bool(self, value):
        true_values = [True, 1, 'yes', 'on', '1', 'true', 'True']
        false_values = [False, 0, 'no', 'off', '0', 'false', 'False']

        # If the value is a string, transform to lowercase
        if type(value) == str:
            value = value.lower()

        # Check if value is in true or false values, else raise Exception
        if value in true_values:
            return True
        elif value in false_values:
            return False
        else:
            msg = "Unknown value given: {}".format(value)
            raise Exception(msg)

    # Check if command exists
    def command_exists(self, cmd):
            """
            Check if command exists
            """
            if shutil.which(cmd):
                return True
            else:
                return False

    # Build Imapsync command
    def build_imapsync_cmd(
            self, source_user, source_password, dest_user, dest_host,
            dest_password, imapsync_cmd_path='imapsync',
            source_host='127.0.0.1', source_port=993, source_ssl=True,
            dest_port=993, dest_ssl=True, extra_params=''):
        
        # Build arguments dictionary
        args = {}
        args['--host1'] = source_host
        args['--port1'] = source_port
        args['--password1'] = source_password
        args['--host2'] = dest_host
        args['--port2'] = dest_port
        args['--password2'] = dest_password
        args['--user1'] = source_user
        args['--user2'] = dest_user
        
        # SSL arguments
        if source_ssl:
            args['--ssl1'] = ""
        if dest_ssl:
            args['--ssl2'] = ""

        # Build the Imapsync arguments from the args dictionary
        imapsync_args = []
        for name, value in args.items():
            arg_value = "{} {}".format(name, value)
            imapsync_args.append(arg_value)

        # Build the final Imapsync command
        imapsync_args_string = " ".join(imapsync_args)
        imapsync_command = "{} {} {}".format(imapsync_cmd_path,
                                             imapsync_args_string,
                                             extra_params)
        return imapsync_command


    # Handle & exec function called from main
    def handle(self):
        # Command line parser
        parser = argparse.ArgumentParser(
            description="Imapsync Status: imapsync-status.py"
        )
        self.add_arguments(parser)

        # Read the command line
        args = parser.parse_args()

        # Manage arguments
        self.manage_arguments(args)

        # Get list of users from the CSV file
        users = self.parse_csv_file(self.user_file_path, self.skip_first_line)
        users_count = len(users)

        # Prompt the number of processes to start and exit if not confirmed
        msg = "I am going to spawn {} Imapsync processes. Continue?"
        msg = msg.format(users_count)
        if not Confirm.ask(msg):
            print("OK! :waving_hand:")

        # Loop users
        for username, user in users.items():
            imapsync_command = self.build_imapsync_cmd(
                source_user=user['source_user'],
                source_password=user['source_password'],
                dest_user=user['dest_user'],
                dest_host=user['dest_host'],
                dest_password=user['dest_password'],
                imapsync_cmd_path=self.imapsync_path,
                source_host=user['source_host'],
                source_port=user['source_port'],
                source_ssl=user['source_ssl'],
                dest_port=user['dest_port'],
                dest_ssl=user['dest_ssl'],
                extra_params=user['extra_params']
            )
            
            msg = "[red]Imapsync command for user [b]{}[/b]:[/red] {}".format(username, imapsync_command)
            print(msg)

            
# Main: run program
if __name__ == "__main__":
    main = ImapsyncLauncher()
    main.handle()