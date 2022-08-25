#!/usr/bin/env python3

"""
Imapsync Status
Display the status of current Imapsync processes

authors:
    Mattia Martinello - mattia.martinello@infojuice.eu
"""

# Built in constants
_VERSION = "1.0"
_VERSION_DESCR = "Imapsync Status"
_LOG_FILE_PATH = "imapsync-status.log"

import argparse
import colorlog
import csv
import glob
import logging
import logging.handlers
import os
import re
import random
import subprocess
import sys

from rich import print
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn, TimeRemainingColumn
from rich.table import Table

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
            '--skip-first-line',
            default=False,
            action="store_true",
            help='Skip the first line of the CSV file.'
        )

    # Manage arguments: do things based on configured command arguments
    def manage_arguments(self, args):        
        # Check if user file exists
        self.user_file_path = getattr(args, 'user_file_path', None)
        if not os.path.exists(self.user_file_path):
                msg = "User file '{}' does not exists!"
                msg = msg.format(self.user_file_path)
                self.logger.error(msg)
                sys.exit(1)
        else:
            self.parse_csv_file(self.user_file_path)

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
                    dest_user = row[1]
                    
                    user = {
                        'source_user': source_user,
                        'dest_user': dest_user
                    }

                    users[source_user] = user
                line_count += 1

            return users

    # Return the list of users from a user CSV file
    def get_user_list(self, csv_file_path):
        users = self.parse_csv_file(csv_file_path)

        user_list = []
        for user in users:
            user_list.append(user['source_user'])

        return user_list

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
        pid_file_name = "{}.pid".format(username)
        pid_file_path = os.path.join(dir_path, pid_file_name)
        return pid_file_path

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
        match = re.search('ETA: (.+) \+.+\s+([0-9]+)\/([0-9]+) msgs left$', line)
        if match:
            eta = match.group(1)
            left_messages = int(match.group(2))
            total_messages = int(match.group(3))
            transferred_messages = total_messages - left_messages
            current_percentage = transferred_messages / total_messages * 100

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
        imapsync_count = len(pid_files)

        title = "Welcome to [dodger_blue1]Imapsync[/dodger_blue1] Status!"
        print(Panel(title))

        job_progress = Progress(
            "{task.description}",
            SpinnerColumn(),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            TextColumn("ETA: {task.fields[eta]}"),
            TextColumn("[progress.percentage]{task.percentage:>3.2f}%"),
        )

        # Get running PID files to build current jobs progress
        jobs = {}

        users = self.parse_csv_file(self.user_file_path)

        for username, user in users.items():
            color = self.pick_random_color()
            job_title = "[color({})]{}".format(color, username)

            # Add a job progress for the current user
            jobs[username] = job_progress.add_task(job_title,
                                                   total=None,
                                                   eta="?")

        overall_progress = Progress()
        overall_task = overall_progress.add_task("All Jobs", total=0)

        progress_table = Table.grid(expand=True)
        progress_table.add_row(
            Panel(
                job_progress,
                title="[b]Jobs",
                border_style="red",
                padding=(1, 2)
            )
        )
        progress_table.add_row(
            Panel(
                overall_progress,
                title="Overall Progress",
                border_style="green", 
                padding=(1, 2)
            )
        )

        overall_total = 0
        with Live(progress_table, refresh_per_second=10):
            while True:
                sleep(1)

                for pid_file in pid_files:
                    log_data = self.parse_pid_file(pid_file)
                    log_file_path = log_data['log_file_path']
                    user = log_data['user']
                    last_line = self.get_last_line(log_file_path)
                    
                    sync_status = self.get_sync_status(last_line)
                    if sync_status == "syncing":
                        sync_progress = self.get_sync_progress(last_line)
                        eta = sync_progress['eta']
                        transferred = sync_progress['transferred_messages']
                        total = sync_progress['total_messages']

                        job_progress.update(jobs[user],
                                            total=total, 
                                            completed=transferred,
                                            eta=eta)

                        # Update the progress of the overall task
                        for task in job_progress.tasks:
                            if task.total is not None:
                                overall_total = overall_total + task.total
                                overall_progress.update(overall_task,
                                                        total=overall_total)

                    # Update completed status of the overall task
                    completed = sum(task.completed for task in job_progress.tasks)
                    overall_progress.update(overall_task, completed=completed)
            
            

# Main: run program
if __name__ == "__main__":
    main = ImapsyncStatus()
    main.handle()
