#!/usr/bin/python3
import argparse
import sys
import libzfs_core
import subprocess
import re
from math import ceil
from time import time, sleep
from datetime import datetime
from termcolor import colored

# Add your datasets here
DATASETS = [
    'frigate',
    'lxd',
    'private',
    'libvirt',
    ]

# Customize for your source and target dataset
SRC_DS='lxdzfs'
DST_DS='backup/lxdzfs_backup'
# Optionally customize snapshot names
ORIGIN_SNAP='current'
HEAD_SNAP='backup'

class zfs_backup:
    def __init__(self):
        self.datasets   = DATASETS
        self.simulate   = False
        self.verbose    = False
        self.timings    = list()
        self.retry      = False
        self.mount      = False

    def scan_datasets(self,ds_list=False):
        if not ds_list:
            ds_list = self.datasets
        datasets = list()
        for dataset in ds_list:
            if not self.check_ds_exists(dataset):
                self.print_log(f'Listed dataset {SRC_DS}/{dataset} does not exist, skipping....',sev='err',bold=True,indent=0)
            elif not self.check_ds_ready(dataset):
                self.print_log(f'Dataset source snapshot does not exist ({SRC_DS}/{dataset}@{ORIGIN_SNAP}), --init required',sev='warn',bold=True,indent=0)
            else:
                datasets.append(dataset)
        return datasets

    def check_ds_exists(self,dsname,ds=SRC_DS):
        return libzfs_core.lzc_exists(f'{ds}/{dsname}'.encode('utf-8'))

    def check_ds_ready(self,dsname,snap=ORIGIN_SNAP,ds=SRC_DS):
        return libzfs_core.lzc_exists(f'{ds}/{dsname}@{snap}'.encode('utf-8'))

    def list_datasets(self):
        for dataset in self.datasets:
            if self.check_ds_ready(dataset):
                origin = f'Origin [{colored("Ready","green",attrs=["bold"])}]'
            else:
                origin = f'Origin [{colored("Missing","red",attrs=["bold"])}] ({colored("init","red")})'
            if self.check_ds_ready(dataset,HEAD_SNAP):
                head = f'Head [{colored("Ready","red",attrs=["bold"])}]\t{colored("(Retry?)","red",attrs=["bold"])}'
            else:
                head = f'Head [{colored("Missing","green",attrs=["bold"])}]\t{colored("(Ready)","white",attrs=["bold"])}'
            if len(dataset) < 6:
                tabs = '\t\t'
            else:
                tabs = '\t'
            print(f'** Dataset {colored(SRC_DS,"yellow")}/{colored(dataset,"cyan",attrs=["bold"])} {tabs} {origin} \t {head} ')

    def run_cmd(self,cmd):
        if self.simulate:
            self.print_log(f'SIM: {cmd}',indent=0,sev='warn')
            return 0
        if self.verbose > 2:
            self.print_log(f'RUN: {cmd}',indent=0,sev='warn')
        try:
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        except:
            self.print_log(f'ERROR{sys.exc_info()[1]} while running {cmd}',sev='err',bold=True)
            self.print_log(str(sys.exc_info()),sev='err',bold=True)
            return 1
        while True:
            output = process.stdout.readline()
            if process.poll() is not None:
                break
            if output and self.verbose:
                print(output.strip().decode())
        rc = process.poll()
        return rc

    def guess_incremental_xfer_size(self,dataset):
        cmd = f'zfs send -Rvwn -i {SRC_DS}/{dataset}@{ORIGIN_SNAP} {SRC_DS}/{dataset}@{HEAD_SNAP} 2>&1 | grep total'
        if self.simulate:
            self.print_log(f'SIM: {cmd}',sev='warn')
        (rc, out) = subprocess.getstatusoutput(cmd)
        if rc != 0:
            return False
        else:
            total = re.search(r'(\d+\.?\d*)(\w)$',out)
            if total:
                size = float(total.group(1))
                unit = total.group(2)
                if unit == 'B':
                    return f'{ceil(size)}'
                else:
                    return f'{ceil(size)}{unit.upper()}'
            else:
                return False

    def hesitate(self,duration_secs):
        for _ in range(duration_secs):
            print('.',end='',flush=True)
            sleep(1)
        print()

    def destroy_dataset(self,dataset,ds=DST_DS,snapshot=False,level=2,recursive=True):
        dataset = f'{ds}/{dataset}'
        if snapshot:
            dataset = f'{dataset}@{snapshot}'
        self.print_log(f'WARNING -- destroying {dataset} in 10s, hit ctrl+c to abort',sev='warn',indent=level,bold=True,newline=False)
        self.hesitate(10)
        opts = ''
        if self.verbose > 2:
            opts = '-v'
        if recursive:
            opts = opts+' -r'
        rc = self.run_cmd(f'zfs destroy {opts} {dataset}')
        if rc:
            return False
        else:
            return True

    def initialize_dataset(self,dataset):
        ready = True
    # Check if dataset exists
        self.print_log(f'Checking for source dataset (must exist to init)...')
        if not self.check_ds_exists(dataset):
            self.print_log(f'Unable to continue, {SRC_DS}/{dataset} does not exist...',sev='err',indent=2,bold=True)
            ready = False
        else:
            self.print_log(f'Dataset {SRC_DS}/{dataset} exists',sev='good',indent=2)
    # Check for destination
        self.print_log(f'Checking for target dataset (should not exist to --init)...')
        if self.check_ds_exists(dataset,DST_DS):
            if self.retry:
                if not self.destroy_dataset(dataset,ds=DST_DS):
                    self.print_log(f'Failed to destroy destination...',sev='err',bold=True,indent=2)
                    ready = False
            else:
                self.print_log(f'Unable to initialize, please zfs destroy -r {DST_DS}/{dataset} or use --retry to continue',sev='err',indent=2,bold=True)
                ready = False
        else:
            self.print_log(f'Snapshot {DST_DS}/{dataset} does not exist',sev='good',indent=2)
    # Check for HEAD
        self.print_log(f'Checking for dataset head (should not exist to --init)...')
        if self.check_ds_ready(dataset,HEAD_SNAP):
            if self.retry:
                if not self.destroy_dataset(dataset,ds=SRC_DS,snapshot=HEAD_SNAP):
                    self.print_log(f'Failed to destroy head snapshot...',sev='err',bold=True,indent=2)
                    ready = False
            else:
                self.print_log(f'Unable to initialize, please zfs destroy -r {SRC_DS}/{dataset}@{HEAD_SNAP} or use --retry to continue',sev='err',indent=2,bold=True)
                ready = False
        else:
            self.print_log(f'Snapshot {SRC_DS}/{dataset}@{HEAD_SNAP} does not exist',sev='good',indent=2)
    # Check for ORIGIN
        self.print_log(f'Checking for dataset origin (should not exist to --init)...')
        if self.check_ds_ready(dataset):
            if self.retry:
                if not self.destroy_dataset(dataset,ds=SRC_DS,snapshot=ORIGIN_SNAP):
                    self.print_log(f'Failed to destroy origin snapshot...',sev='err',bold=True,indent=2)
                    ready = False
            else:
                self.print_log(f'Unable to initialize, please zfs destroy -r {SRC_DS}/{dataset}@{ORIGIN_SNAP} or use --retry to continue',sev='err',indent=2,bold=True)
                ready = False
        else:
            self.print_log(f'Snapshot {SRC_DS}/{dataset}@{ORIGIN_SNAP} does not exist',sev='good',indent=2)
    # Do nothing if prep failed
        if not ready:
            self.print_log(f'Failed to prep for initialization, giving up...',sev='err',bold=True)
        else:
            verbose=''
            if self.verbose > 2:
                verbose = '-v'
        # Create origin snapshot
            self.print_log(f'Creating origin snapshot {SRC_DS}/{dataset}@{ORIGIN_SNAP}')
            snap_rc = self.run_cmd(f'zfs snapshot -r {SRC_DS}/{dataset}@{ORIGIN_SNAP}')
            if snap_rc:
                self.print_log('Failed to create origin snapshot, failing hard.',sev='err',bold=True,indent=2)
                sys.exit(1)
        # Perform initialization xfer
            self.print_log(f'Transferring origin {SRC_DS}/{dataset}@{ORIGIN_SNAP} to target {DST_DS}/{dataset}...')
            xfer_rc = self.run_cmd(f'zfs send {verbose} -Rw {SRC_DS}/{dataset}@{ORIGIN_SNAP}|pv|zfs recv {verbose} -Fu {DST_DS}/{dataset}')
            if snap_rc:
                self.print_log('Failed to initialize dataset, failing hard.',sev='err',bold=True,indent=2)
                sys.exit(1)
            else:
                self.print_log('Dataset initialized successfully, ready for incremental backup',sev='good',bold=True)

    def backup_dataset(self,dataset):
        start_time = time()
        if self.verbose:
            self.print_log(f'Preparing incremental xfer of {SRC_DS}/{dataset}@{ORIGIN_SNAP} -> {HEAD_SNAP} to {DST_DS}/{dataset}',indent=0,bold=True)
    # Create target snapshot if this is not a retry
        if not self.retry:
            if self.check_ds_ready(dataset,HEAD_SNAP):
                if self.verbose:
                    self.print_log('HEAD snapshot already exists, run again with --retry',sev='err',bold=True)
                self.timings.append((dataset, None, time() - start_time, 1))
                return 1
            if self.verbose:
                self.print_log(f'Creating snapshot {SRC_DS}/{dataset}@{HEAD_SNAP}...')
            cmd = f'zfs snapshot -r {SRC_DS}/{dataset}@{HEAD_SNAP}'
            self.run_cmd(cmd)
        else:
            if self.verbose:
                self.print_log(f'Skipping {HEAD_SNAP} snapshot creation, this is a retry...',sev='warn')
            if not self.check_ds_ready(dataset,HEAD_SNAP):
                self.print_log(f'Unable to continue, retry specified but final snap {SRC_DS}/{dataset}@{HEAD_SNAP} does not exist!',sev='err',bold=True)
                self.timings.append((dataset, None, time() - start_time, 1))
                return 1
    # Simulate incremental transfer to guess xfer size
        if self.verbose:
            self.print_log('Guessing size of incremental transfer...')
        size = self.guess_incremental_xfer_size(dataset)
        if size:
            if self.verbose:
                self.print_log(f'Estimated size {size}',indent=2,sev='good')
            pv_sz = f'-s {size}'
        else:
            if self.verbose:
                self.print_log(f'Unable to estimate size, trying anyways...',sev='warn',indent=2)
            pv_sz = ''
    # Perform incremental transfer
        pv = str()
        if self.verbose:
            self.print_log(f'Starting incremental xfer of {size} to {DST_DS}/{dataset}')
        if self.verbose > 1:
            pv = f'|pv {pv_sz}'
        opts = str()
        if not self.mount:
            opts = '-o canmount=noauto'
        if self.verbose > 2:
            opts = f'-v {opts}'
        backup_rc = self.run_cmd(f'zfs send -Rw -i {SRC_DS}/{dataset}@{ORIGIN_SNAP} {SRC_DS}/{dataset}@{HEAD_SNAP}{pv}|zfs recv {opts} -Fu {DST_DS}/{dataset}')
        if backup_rc != 0:
            if self.verbose:
                self.print_log(f'Failed to backup dataset, not rotating snapshots, retry with --retry',sev='err',bold=True)
        else:
        # Rotate snapshots
            if self.verbose:
                self.print_log(f'Destroying old origin {SRC_DS}/{dataset}@{ORIGIN_SNAP}')
            self.run_cmd(f'zfs destroy -r {SRC_DS}/{dataset}@{ORIGIN_SNAP}')
            if self.verbose:
                self.print_log(f'Moving head ({HEAD_SNAP}) to origin ({ORIGIN_SNAP})')
            self.run_cmd(f'zfs rename -r {SRC_DS}/{dataset}@{HEAD_SNAP} {SRC_DS}/{dataset}@{ORIGIN_SNAP}')
    # Record time
        self.timings.append((dataset, size, time() - start_time, backup_rc))

    def print_log(self,msg,indent=1,sev='info',bold=False,no_pre=False,dt=False,newline=True):
    # Log Prefix
        if not no_pre:
            if sev == 'info':
                pre = colored('** ','cyan',attrs=['bold'])
            elif sev == 'good':
                pre = colored('** ','green',attrs=['bold'])
            elif sev == 'warn':
                pre = colored('*! ','yellow',attrs=['bold'])
            elif sev == 'err':
                pre = colored('!! !! ','red',attrs=['bold'])
        else:
            pre = ''
    # Timestamp
        if dt:
            pre = f'{colored(datetime.now(),"white","on_grey",attrs=["bold"])} {pre}'
    # Msg Style
        style = []
        if bold:
            style = ['bold']
        msg = '\t'*indent + pre + colored(msg,attrs=style)
        if newline:
            print(f'{msg}')
        else:
            print(f'{msg}',end='',flush=True)

    def print_timings(self):
        self.print_log("Dataset\t\tSize\tTime",indent=0,no_pre=True,bold=True)
        for time in self.timings:
            if len(time[0]) < 7:
                tabs = '\t\t'
            else:
                tabs = '\t'
            if time[3] == 0:
                result = colored('Successful', 'green', attrs=['bold'])
            else:
                result = colored('Failed', 'red', attrs=['bold'])
            print(f'{colored(time[0],"cyan",attrs=["bold"])}:{tabs}{time[1]}\t{time[2]:.1f}s\t[{result}]')

def main():
    parser = argparse.ArgumentParser(description='Backup ZFS Filesystems to local backup pool')
    parser.add_argument('-r', '--retry', action='store_true', help='Retry incremental transfer of previous @backup snapshot')
    parser.add_argument('-d', '--dataset', action='append', help='Limit backup to particular dataset -- specify multiple times for more than one dataset.')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='Verbose output (multiple times to increase verbosity)')
    parser.add_argument('-l', '--list', action='store_true', help='Do nothing but list datasets')
    parser.add_argument('-m', '--mount', action='store_true', help='Allow target dataset to mount')
    parser.add_argument('-s', '--simulate', action='store_true', help='Just echo commands, don\'t actually do anything')
    parser.add_argument('-i', '--init', action='store_true', help='Re-initialize dataset, must not contain origin snapshot. Will take a long time, suggest -vvv')
    args = parser.parse_args()
    zfs = zfs_backup()
    zfs.simulate    = args.simulate
    zfs.verbose     = args.verbose
    zfs.retry       = args.retry
    zfs.mount       = args.mount
    start_time      = time()
# Update datasets if given
    if args.dataset:
        zfs.datasets = zfs.scan_datasets(args.dataset)
    else:
        zfs.scan_datasets()
# Just check and list datasets
    if args.list:
        zfs.list_datasets()
# Initialize or re-initialize dataset (prepare for incremental)
    elif args.init:
        if not args.dataset or len(args.dataset) != 1:
            zfs.print_log('Only one dataset can be initialized at a time. Use --dataset to specify',sev='err',bold=True,indent=0)
        else:
            zfs.print_log(f'Re-initializing dataset {args.dataset[0]}',indent=0,bold=True,dt=True)
            zfs.initialize_dataset(args.dataset[0])
            zfs.print_log(f'Initialization Completed in {time()-start_time:.2f}s',bold=True,indent=0,dt=True,sev='good')
    else:
        if args.verbose:
            zfs.print_log(f'Beginning backup with args {str(args)}',bold=True,indent=0,dt=True,sev='good')
    # Perform backups
        for dataset in zfs.datasets:
            print()
            zfs.backup_dataset(dataset)
    # Print timing stats
        if args.verbose:
            print()
            zfs.print_timings()
            print()
            zfs.print_log(f'Backups Completed in {time()-start_time:.2f}s',bold=True,indent=0,dt=True,sev='good')

if __name__ == '__main__':
    main()
