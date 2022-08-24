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
import glob
import logging
import logging.handlers
import os
import re
import rich
import subprocess

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

    # Enable debug log
    def enable_debug(self):
        self.logger.info("Enabling debug log ...")
        self.logger.setLevel(colorlog.colorlog.logging.DEBUG)
        self.logger.debug("Debug log enabled")

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

    # Manage arguments: do things based on configured command arguments
    def manage_arguments(self, args):
        # Enable debug
        if getattr(args, "debug", False):
            self.enable_debug()

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

    # Get last line of a given file
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

        # Get PID files to build current jobs progress
        jobs = {}
        for pid_file in pid_files:
            log_data = self.parse_pid_file(pid_file)
            user = log_data['user']
            job_title = "[green]{}".format(user)

            # Add a job progress for the current user
            jobs[user] = job_progress.add_task(job_title, total=None, eta="?")

        #total = sum(task.total for task in job_progress.tasks)
        overall_progress = Progress()
        overall_task = overall_progress.add_task("All Jobs", total=0)

        progress_table = Table.grid(expand=True)
        progress_table.add_row(
            Panel(
                job_progress,
                title="[b]Jobs",
                border_style="red",
                padding=(1, 2)),
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
