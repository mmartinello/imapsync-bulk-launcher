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

import argparse
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
            '--skip-first-line',
            default=False,
            dest='skip_first_line',
            action="store_true",
            help='Skip the first line of the CSV file (default: False).'
        )
        parser.add_argument(
            '-i', '--imapsync-command',
            default='imapsync',
            dest='imapsync_command',
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
        else:
            self.parse_csv_file(self.user_file_path)

        self.skip_first_line = getattr(args, 'skip_first_line')

    # Parse the user CSV file
    def parse_csv_file(self, csv_file_path, skip_first_line=False):
        with open(csv_file_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            line_count = 1

            users = {}
            for row in csv_reader:
                if skip_first_line and line_count == 1:
                    continue
                else:
                    source_user = row[0]
                    source_host = row[1]
                    source_port = row[2]
                    source_ssl = row[3]
                    source_password = row[4]
                    dest_user = row[5]
                    dest_host = row[6]
                    dest_port = row[7]
                    dest_ssl = row[8]
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

                    users[source_user] = user
                    
                line_count += 1

            # Sort users by username and return
            users = dict(sorted(users.items()))
            return users

    # Check if command exists
    def command_exists(self, cmd):
            """
            Check if command exists
            """
            if shutil.which(cmd):
                return True
            else:
                return False


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


            
            
# Main: run program
if __name__ == "__main__":
    main = ImapsyncLauncher()
    main.handle()
