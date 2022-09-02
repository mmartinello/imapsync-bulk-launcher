#!/usr/bin/env python3

"""
Imapsync Status
Display the status of current Imapsync processes

authors:
    Mattia Martinello - mattia@mattiamartinello.com
"""

# Built in constants
_VERSION = "1.0"
_VERSION_DESCR = "Imapsync Status"
_LOG_FILE_PATH = "imapsync-status.log"

import argparse
import colorlog
import csv
import datetime
import glob
import logging
import logging.handlers
import os
import re
import random
import shutil
import subprocess
import sys

from rich import print
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn
from rich.table import Table
from rich.text import Text

from time import sleep


# Program main class
class ImapsyncStatus:
    def __init__(self):
        # Setup logger
        self.setup_logger("imapsync-status", _LOG_FILE_PATH)

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
            help='The path of the CSV file containing the list of users'
        )
        parser.add_argument(
            '-s', '--skip-first-line',
            default=False,
            dest='skip_first_line',
            action="store_true",
            help='Skip the first line of the CSV file.'
        )
        parser.add_argument(
            '-r', '--show-running',
            default=False,
            dest='show_running',
            action="store_true",
            help='Show only running users.'
        )
        parser.add_argument(
            '-n', '--no-clear-console',
            default=False,
            dest='no_clear_console',
            action="store_true",
            help='Avoid to clear the console at program start.'
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
        self.show_running = getattr(args, 'show_running')
        self.no_clear_console = getattr(args, 'no_clear_console')

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

    # Read a PID file
    def parse_pid_file(self, file_path):
        f = open(file_path, 'r')
        data = f.read()  
        user = re.search('imapsync-(.+).pid$', file_path)
        pid = re.search('^[1-9]+$', data, re.MULTILINE)
        log_file_path = re.search('.+.txt$', data, re.MULTILINE)

        return_data = {}
        return_data['user'] = user.group(1)
        return_data['pid'] = pid.group()
        return_data['log_file_path'] = log_file_path.group()

        return return_data

    # Get PID files
    def get_pid_files(self, dir_path='.'):
        path = os.path.join(dir_path, '*.pid')
        files = glob.glob(path)
        return files

    # Get the PID file for a given user
    def get_user_pid_file(self, username, dir_path='.'):
        pid_file_name = "imapsync-{}.pid".format(username)
        pid_file_path = os.path.join(dir_path, pid_file_name)
        return pid_file_path

    # Check if PID file for a given user exists or not
    def user_pid_file_exists(self, username, dir_path='.'):
        pid_file_path = self.get_user_pid_file(username, dir_path)

        if os.path.exists(pid_file_path):
            return True
        else:
            return False

    # Get last line of a given file
    # FIXME: this works only on Linux! Needs to be adapted to be portable!
    def get_last_line(self, file_path):
        last_line = subprocess.check_output(['tail', '-1', file_path])
        last_line = last_line.decode()
        return last_line

    # Get sync status from a given log file line
    def get_sync_status(self, line):
        if re.search('([0-9]+)\/([0-9]+) msgs left$', line, re.MULTILINE):
            return "syncing"
        else:
            return "running"

    # Get sync progress from a given log file line
    def get_sync_progress(self, line):
        match = re.search('ETA: .+ ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).+ ([0-9]+)\/([0-9]+) msgs left$', line)
        if match:
            # Get messages transfer information
            left_messages = int(match.group(2))
            total_messages = int(match.group(3))
            transferred_messages = total_messages - left_messages
            current_percentage = transferred_messages / total_messages * 100

            # Get ETA
            eta = match.group(1)
            eta = datetime.datetime.strptime(eta, '%Y-%m-%d %H:%M:%S')

            return_data = {}
            return_data['eta'] = eta
            return_data['transferred_messages'] = transferred_messages
            return_data['total_messages'] = total_messages
            return_data['left_messages'] = left_messages
            return_data['current_percentage'] = current_percentage
            return return_data
        else:
            return None

    # Pick a random color
    def pick_random_color(self):
        color = random.randrange(0, 255)
        return color
    
    # Clear the console
    def clear_console(self):
        commands = ['clear', 'cls']
        for command in commands:
            if self.command_exists(command):
                os.system(command)
                return True

        return False

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

        # Get PID files
        pid_files = self.get_pid_files()

        # Clear the console
        if not self.no_clear_console:
            self.clear_console()

        # Print the title panel
        title = "[b]Welcome to [dodger_blue1]Imapsync[/dodger_blue1] Status![/b]"
        print(Panel(title))

        # Get running PID files to build current jobs progress
        jobs = {}
        users = self.parse_csv_file(self.user_file_path, self.skip_first_line)
        users_count = len(users)

        # Create the main table
        progress_table = Table.grid(expand=True)

        # Overall status: create the progress and add it as a new row
        status_progress = Progress(
            TextColumn("Total users: [b bright_cyan]{task.fields[total_users]}"),
            TextColumn("Running jobs: [b bright_cyan]{task.fields[running_jobs]}"),
            TextColumn("Idle users: [b bright_cyan]{task.fields[idle_users]}"),
            TextColumn("Maximum ETA: [b]{task.fields[max_eta]}"),
            expand=True,
        )
        status_task = status_progress.add_task(
            "Overall Status:",
            total=None,
            total_users=users_count,
            running_jobs='0',
            idle_users=users_count,
            max_eta='?',
        )
        progress_table.add_row(
            Panel(
                status_progress,
                title="[b]Overall Status",
                border_style="blue"
            )
        )

        # Main job progress: contains all sync progress jobs
        job_progress = Progress(
            "{task.description}",
            SpinnerColumn(),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            TextColumn("ETA: {task.fields[eta]}"),
            TextColumn("[progress.percentage]{task.percentage:>3.2f}%"),
            expand=True,
        )

        # Create a job for each user taken from the CSV file
        for username, user in users.items():
            color = self.pick_random_color()
            job_title = "[color({})]{}".format(color, username)

            # Check if a progress bar needs to be added for the current user:
            # if --show-running is enabled the user is added only if it has
            # a PID file running
            pid_file_exists = self.user_pid_file_exists(username)

            if not self.show_running or (
                self.show_running and pid_file_exists
                ):
                jobs[username] = job_progress.add_task(job_title,
                                                       start=False,
                                                       total=None,
                                                       eta="?")

        # Add the main job progress row
        progress_table.add_row(
            Panel(
                job_progress,
                title="[b]Jobs",
                border_style="red",
                padding=(1, 2),
            )
        )

        # Overall progress: create the progress and add it as a new row
        overall_progress = Progress(
            TextColumn("{task.fields[running_jobs]} Running Jobs"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.2f}%"),
            expand=True,
        )
        overall_task = overall_progress.add_task(
            "Overall Progress",
            total=None,
            running_jobs='0'
        )
        progress_table.add_row(
            Panel(
                overall_progress,
                title="Overall Progress",
                border_style="green", 
                padding=(1, 2)
            )
        )

        # Update the progress using Live()
        with Live(progress_table, refresh_per_second=10):
            while True:
                sleep(1)

                max_eta = None
                max_eta_timestamp = None
                max_eta_string = '?'

                # Get PID files
                pid_files = self.get_pid_files()
                for pid_file in pid_files:
                    log_data = self.parse_pid_file(pid_file)
                    log_file_path = log_data['log_file_path']
                    user = log_data['user']
                    last_line = self.get_last_line(log_file_path)

                    sync_status = self.get_sync_status(last_line)
                    if sync_status == "syncing":
                        sync_progress = self.get_sync_progress(last_line)
                        transferred = sync_progress['transferred_messages']
                        total = sync_progress['total_messages']

                        eta = sync_progress['eta']
                        eta_string = eta.strftime('%A %Y-%m-%d %H:%M:%S')

                        if user in jobs['user']:
                            job_progress.start_task(jobs[user])
                            job_progress.update(jobs[user],
                                                total=total, 
                                                completed=transferred,
                                                eta=eta_string)
                        else:
                            color = self.pick_random_color()
                            job_title = "[color({})]{}".format(color, username)
                            jobs[username] = job_progress.add_task(job_title,
                                                                start=False,
                                                                total=None,
                                                                eta="?")

                        # Calculate maximum ETA
                        eta_timestamp = eta.timestamp()
                        if max_eta is None or max_eta_timestamp is None or eta_timestamp > max_eta_timestamp:
                            max_eta_timestamp = eta_timestamp
                            max_eta = datetime.datetime.fromtimestamp(max_eta_timestamp)

                    # Update the progress of the overall task
                    overall_total = 0
                    overall_completed = 0
                    running_tasks = 0                    
                    for task in job_progress.tasks:
                        if task.total is not None:
                            overall_total = overall_total + task.total
                            overall_completed = overall_completed + task.completed
                        if task.started:
                            running_tasks += 1

                    idle_users = users_count - running_tasks

                    # Update the overall status task
                    if type(max_eta) == datetime.datetime:
                        max_eta_string = max_eta.strftime('%A %Y-%m-%d %H:%M:%S')

                    status_progress.update(
                        status_task,
                        running_jobs=running_tasks,
                        idle_users=idle_users,
                        max_eta=max_eta_string
                    )

                    # Update the overall progress task
                    overall_progress.update(
                        overall_task,
                        total=overall_total,
                        completed=overall_completed,
                        running_jobs=running_tasks
                    )
            
            
# Main: run program
if __name__ == "__main__":
    main = ImapsyncStatus()
    main.handle()
