# zfs-backup
### ZFS Snapshot based utility for full and incremental backups to local zpool
This script can be used to perform full and incremental backups between two zpools
on a local system. The name of the source and target zpool dataset should be 
configured along with the name of the "origin" and "head" snapshots:

### Preparing for incremental backup
Before your first incremental backup, a full replicated backup must be performed.
To "initialize" a dataset for backup, run with the -i parameter (e.g. `backup_zfs.py -v -d somedataset -i`).
If the destination dataset already exists, or the origin snapshot already exists, you can pass the --retry
argument which will destroy the target dataset as well as the origin snapshot.

### Re-trying a failed backup
If for some reason a dataset fails, snapshots will be preserved until manual intervention is taken.

There are two options should an incremental backup fail:

1. Re-initialize the dataset with -i -r and then perform another backup
1. Re-try the incremental transfer with -r (--retry)

Since the origin and head snapshots were not rotated, everything is left preserved. In most 
cases, a failure will be due to a busy dataset. Running with -vvv will give more information 
about the actual cause of the failure.

### Usage
```
usage: backup_zfs.py [-h] [-r] [-d DATASET] [-v] [-l] [-m] [-s] [-i]

Backup ZFS Filesystems to local backup pool

optional arguments:
  -h, --help            show this help message and exit
  -r, --retry           Retry incremental transfer of previous @backup snapshot
  -d DATASET, --dataset DATASET
                        Limit backup to particular dataset -- specify multiple times for more than one dataset.
  -v, --verbose         Verbose output (multiple times to increase verbosity)
  -l, --list            Do nothing but list datasets
  -m, --mount           Allow target dataset to mount
  -s, --simulate        Just echo commands, don't actually do anything
  -i, --init            Re-initialize dataset, must not contain origin snapshot. Will take a long time, suggest -vvv
```
