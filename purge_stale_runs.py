#!/home/kwu1/software/python/bin/python
"""
Script to identify and clean runs with fastq and bams
"""
import os
import argparse
import re
import time
import subprocess
import datetime

# This regex pattern is used to perform a first-pass filter of contents of top level directory
VALID_RUNFOLDER_MATCHER = r"[0-9]{6}_\w+\d+_\d{4}_\w+"

def get_runfolder_days_since_created(runfolder, current_time):
    # Note that this only checks the modified timestamps
    now = datetime.datetime.now()
    if len(runfolder) == 0:
        return None, current_time
    run_date = os.path.basename(runfolder).split("_")[0]
    date = [run_date[i:i+2] for i in range(0, len(run_date), 2)]
    days = int(date[0])*365+int(date[1])*30+int(date[2])
    year, month,day = now.year,now.month,now.day
    days_today = (year-2000)*365 + (month)*30 + day
    days_delta = days_today - days
    return days_delta


def identify_runs(folder, max_days):
    """Traverse folder contents, identifying runs to archive based on timestamps"""
    contents = os.listdir(folder)
    # Filter the contents of the folder based on the 
    valid_runfolders = [f for f in contents if re.match(VALID_RUNFOLDER_MATCHER, os.path.basename(f)) is not None]
    # If the runfolder doesn't even have a SampleSheet.csv, then it's probably not even close to being done
    # valid_runfolders = [f for f in valid_runfolders if os.path.isfile(os.path.join(folder, f, "SampleSheet.csv"))]

    runs_to_archive = set()
    # Consistent time that we compare all timestamps to
    curr_time = time.time()
    for runfolder in valid_runfolders:
        # Figure out the last modification date
        days_since = get_runfolder_days_since_created(os.path.join(folder, runfolder), curr_time)
        if days_since is None or days_since < max_days:
            # Skip over run folders that either have not had bevmo run, or are fairly recent
            continue
        runs_to_archive.add(os.path.join(folder, runfolder))
    return runs_to_archive


def read_whitelist(folder,whitelist):
    """
    Reads the whitelist file for runfolders to ignore. The return value is a list of the
    basenames ONLY, and not the full path
    """
    retval = set()
    with open(whitelist, 'r') as handle:
        content = handle.read()
        lines = content.splitlines()
        assert len(lines) > 1
        lines = lines[1:] # skip the first line becuase it is a header
        for line in lines:
            runname = os.path.join(folder,os.path.basename(line))
            # print(runname)
            if not line: # Empty strings/lists/tuples/etc are considered False
                continue
            if re.match(VALID_RUNFOLDER_MATCHER, line) is None:
                raise RuntimeError("Encountered illegal entry in whitelist: %s" % line)
            if runname in retval:
                raise RuntimeError("Encountered duplicate entry in whitelist: %s" % runname)
            retval.add(os.path.join(folder,runname))
    return retval

def file_cleanse(runfolder, dry, log):
    """Clean the runs"""
    # Sanity checks
    if not os.path.isdir(runfolder):
        print "WARNING: Given runfolder %s is not a directory. Ignoring..." % runfolder
        return
    # Build the command
    fastq1 = os.path.join(runfolder, "AnalysisOGP*/Fastq/*.fastq.gz")
    fastq2 = os.path.join(runfolder, "AnalysisOGP*/Fastq/*/*.fastq.gz")
    fastq3 = os.path.join(runfolder, "AnalysisOGP*/*Alignment/*/*.fastq.gz")
    bam = os.path.join(runfolder, "AnalysisOGP*/*Alignment/*.bam")
    bai = os.path.join(runfolder, "AnalysisOGP*/*Alignment/*.bam.bai")
    command = "rm -rv " + fastq1 + " " + fastq2 + " " + fastq3 + " " + bam + " " + bai
    thumbnail_cmd = "cd %s ; /illumina/scratch/ZodiacR2/Users/kclark/cleanup/cleanup_week2_v4.sh" % runfolder 
    print "Command to be executed: " + command
    print "Image purge command to be executed: " + thumbnail_cmd
    if not dry:
        print "Executing..."
        subprocess.call('rm -rv ' + fastq1 + " " + fastq2 + " " + fastq3 + " " + bam + " " + bai, shell=True)
        subprocess.call("cd %s ; /illumina/scratch/ZodiacR2/Users/kclark/cleanup/cleanup_week2_v4.sh" % runfolder, shell=True)
        f = open(log, 'a')
        f.write(runfolder + ' '+ 'is purged\n')

def build_parser():
    """
    Builds the argument parser
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--Folder", type=str, required=False, default='/illumina/scratch/ZodiacR2/Runs',
                        help="The folder to traverse to look for runs")
    parser.add_argument("--MaxDays", type=int, default=60,
                        help="Number of days since create timestamp to bevmo analysis to allow before considering for archive")
    parser.add_argument("--WhitelistFile", type=str, required=False, default='/illumina/scratch/ZodiacR2/Runs/whitelist.txt',
                        help="File containing newline delimited list of run folders to ignore (i.e. not consider for archive)")
    parser.add_argument("--Dry", action="store_true", help="Use this flag generate commands without exeucting them")                       
    parser.add_argument("--LogFile", type=str, required=True,
                        help="File to write processed runs to")
    return parser



def main():
    parser = build_parser()
    args = parser.parse_args()
    candidate_runs = identify_runs(args.Folder, args.MaxDays)
    if args.WhitelistFile is not None:
        whitelist_runs = read_whitelist(args.Folder, args.WhitelistFile)
        runs_to_archive = candidate_runs - whitelist_runs
    else:
        runs_to_archive = candidate_runs
    for item in sorted(list(runs_to_archive)):
        file_cleanse(item, args.Dry, args.LogFile)
        
    
if __name__ == "__main__":
    main()