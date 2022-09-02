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
import subprocess

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
            default=False,
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
            '-d', '--dry-run',
            default=False,
            dest='dry_run',
            action="store_true",
            help="Enable dry run mode (print commands and don't execute them)."
        )
        parser.add_argument(
            '-y', '--yes', '--assume-yes',
            default=False,
            dest='assume_yes',
            action="store_true",
            help="Automatic yes to prompts."
        )
        parser.add_argument(
            '-i', '--imapsync-path',
            default='imapsync',
            dest='imapsync_path',
            help='The path of the imapsync command (default: imapsync).'
        )
        parser.add_argument(
            '-e', '--extra',
            default='',
            dest='imapsync_extra',
            help='Extra Imapsync options (will be added to each user process).'
        )
        parser.add_argument(
            'users_limit',
            metavar='USER',
            type=str,
            nargs='*',
            help='Filter a user from the CSV file.'
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
        self.users_limit = getattr(args, 'users_limit')
        self.imapsync_extra = getattr(args, 'imapsync_extra', '')
        self.debug = getattr(args, 'debug', False)
        self.dry_run = getattr(args, 'dry_run', False)
        self.assume_yes = getattr(args, 'assume_yes', False)

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
            dest_port=993, dest_ssl=True, extra_params='',
            global_extra_params='', return_type='string', pid_file=None,
            pid_file_locking=True):

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
            args['--ssl1'] = True
        if dest_ssl:
            args['--ssl2'] = True

        # PID file
        if pid_file:
            args['--pidfile'] = pid_file
        if pid_file_locking:
            args['--pidfilelocking'] = True

        # Extra parameters
        extra_params_string = ''
        if extra_params != '':
            extra_params_string = " {}".format(extra_params)

        # Global extra params
        global_extra_params_string = ''
        if global_extra_params != '':
            global_extra_params_string = " {}".format(global_extra_params)

        # Build the Imapsync arguments from the args dictionary
        imapsync_args = []
        for name, value in args.items():
            if value == True:
                arg_value = "{}".format(name)
            else:
                arg_value = "{} {}".format(name, value)
            imapsync_args.append(arg_value)

        # Build the final Imapsync command
        imapsync_args_string = " ".join(imapsync_args)
        imapsync_command = "{} {}{}{}".format(imapsync_cmd_path,
                                             imapsync_args_string,
                                             extra_params_string,
                                             global_extra_params_string)

        # If return type is string return the full command string, if args
        # return the command arguments splitted as array
        if return_type == "string":
            return imapsync_command
        elif return_type == "args":
            return imapsync_command.split(' ')
        else:
            msg = "Unsupported return type requested: {}".format(return_type)
            raise Exception(msg)

    # Execute a new process and return a subprocess.Popen object
    def subprocess_exec(self, args, **kwargs):
        process = subprocess.Popen(args, **kwargs)
        return process

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

        # Filter user from CSV file if user filter is enabled
        if self.users_limit:
            for key, value in list(users.items()):
                if key not in self.users_limit:
                    del users[key]

        # Count users
        users_count = len(users)

        # Prompt the number of processes to start and exit if not confirmed
        if not self.assume_yes:
            msg = "I am going to spawn [bold bright_cyan]{}[/bold bright_cyan]"
            msg+= " Imapsync processes. Continue?"
            msg = msg.format(users_count)
            if not Confirm.ask(msg):
                print("OK, bye! :waving_hand:")
                exit(1)

        # Loop users
        for username, user in users.items():
            pid_file_path = "imapsync-{}.pid".format(username)

            imapsync_command_args = self.build_imapsync_cmd(
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
                pid_file=pid_file_path,
                pid_file_locking=True,
                extra_params=user['extra_params'],
                global_extra_params=self.imapsync_extra,
                return_type='args'
            )

            if self.debug or self.dry_run:
                msg = "[red]Imapsync command for user [b]{}[/b]:[/red] {}"
                msg = msg.format(username, imapsync_command_args)
                print(msg)

            # Executing a new imapsync process
            if not self.dry_run:
                process = self.subprocess_exec(imapsync_command_args)
            
                pid = process.pid
                msg = "New process for user [b]{}[/b] executed with PID {}"
                msg = msg.format(username, pid)
                print(msg)
            
# Main: run program
if __name__ == "__main__":
    main = ImapsyncLauncher()
    main.handle()
