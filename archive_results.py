#!/illumina/scratch/OncoBU/Software/ThirdParty/python27/bin/python
"""
Script to move runs to staging folder
"""

import os
import subprocess
import argparse
import re
import csv
from time import gmtime, strftime


# This regex pattern is used to perform a first-pass filter of contents of top level directory
VALID_RUNFOLDER_MATCHER = r"[0-9]{6}_\w+\d+_\d{4}_\w+"
MZM_REGEX=r"MegaZodiacMasterV.+"
TIME_STAMP = strftime("%Y-%m-%d-%H-%M-%S",gmtime())
def identify_runs(folder):
    """Traverse folder contents, identifying runs based on name and existence of MZM summaries"""
    contents = os.listdir(folder)
    # Filter the contents of the folder based on the 
    valid_runfolders = [f for f in contents if re.match(VALID_RUNFOLDER_MATCHER, os.path.basename(f)) is not None]
    runs_to_archive = set()
    for runfolder in valid_runfolders:
    # Find the the MZM folder and look for sample sheet
        runfolder_full_path = os.path.join(folder,runfolder)
        analysis_folders = os.listdir(runfolder_full_path)
        MZM_folders = [f for f in analysis_folders if re.match(MZM_REGEX, os.path.basename(f)) is not None]
        SampleSheet_used = [f for f in MZM_folders if os.path.isfile(os.path.join(runfolder_full_path, os.path.basename(f), "SampleSheet_used.csv")) is True]
        for samplesheet_exist in SampleSheet_used:
            MZMresult = os.path.join(runfolder_full_path, samplesheet_exist)
            poolSamples = [False, False]
            Data_start = False
            Manifest_col = None
            with open(os.path.join(MZMresult,"SampleSheet_used.csv")) as samplesheet:
                samplereader = csv.reader(samplesheet,delimiter=',')
                for i, row in enumerate(samplereader):
                    for j,field in enumerate(row):
                        if field == "[Data]":
                            Data_start = True
                            # print(i)
                        if field == "Manifest":
                            Manifest_col = j
                            # print(j)
                    if Data_start & (Manifest_col is not None):
                        if row[Manifest_col] == "PoolDNA":
                            # print "PoolDNA is in file"
                            poolSamples[0] = True
                        if row[Manifest_col] == "PoolRNA":
                            # print "PoolRNA is in file"
                            poolSamples[1] = True
            poolDNA_summary=os.path.join(MZMresult,"DNA/Summary/downsampled_aggregate.enrichment.HsMetrics_summary.csv")
            poolRNA_summary_new=os.path.join(MZMresult,"RNA/downsampled/DAGR_input.csv")
            poolRNA_summary_old=os.path.join(MZMresult,"RNA/downsampled/DAG_input.csv")
            # flag_for_archive=[False,False]
            if poolSamples[0]:
                if os.path.isfile(poolDNA_summary):
                    runs_to_archive.add(MZMresult)
                else:
                    runs_to_archive.discard(MZMresult)
            if poolSamples[1]:
                if os.path.isfile(poolRNA_summary_new) | os.path.isfile(poolRNA_summary_old):
                    runs_to_archive.add(MZMresult)
                else:
                    runs_to_archive.discard(MZMresult)
    # print(runs_to_archive)
    return runs_to_archive
    

def send_run_to_cp(runfolder, staging_folder, dry, log):
    """Sends the given run to staging folder"""
    # Sanity checks
    if not os.path.isdir(runfolder):
        print "WARNING: Given runfolder %s is not a directory. Ignoring..." % runfolder
        return
    if os.path.exists(os.path.join(staging_folder, os.path.basename(runfolder))):
        raise RuntimeError("Destination in staging folder already exists! %s" % runfolder)
    # Build the command
    dir_name = os.path.basename(os.path.dirname(runfolder))
    basename = os.path.basename(runfolder) 
    command = ['cp', '-r',runfolder, os.path.join(staging_folder,dir_name,basename)]
    archive_run = os.path.join(staging_folder,dir_name,basename)
    parent_archive_dir = os.path.join(staging_folder,dir_name)
    print "Command to be executed: " + ' '.join(command)
    if not dry:
        f = open(log, 'a')
        print "Executing..."
        if os.path.isdir(archive_run):
            print "File already exist. Ignoring"
            f.write(archive_run + ' '+ 'is ignored\n')
        else:
            # print "Command to be executed: " + ' '.join(command)
            if not os.path.isdir(parent_archive_dir):
                os.makedirs(parent_archive_dir)
            subprocess.call(command)
            f.write(archive_run + ' '+ 'is copied\n')



def build_parser():
    """
    Builds the argument parser
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--RunFolder", type=str, default='/illumina/scratch/ZodiacR2/Runs/',
                        help="The run folder where runs are located")
    parser.add_argument("--ArchiveFolder", type=str, default='/illumina/scratch/ZodiacR2/MZMResults', 
                        help="The archiving folder, into which the runs will be copied")
    parser.add_argument("--Dry", action="store_true", help="Use this flag generate commands without exeucting them")
    parser.add_argument("--LogFile", type=str, required = False, default='/illumina/scratch/ZodiacR2/MZMResults/Logs/'+TIME_STAMP+'_log.txt',
                        help="File to write processed runs to")
    return parser


def main():
    """Runs the script"""
    parser = build_parser()
    args = parser.parse_args()
    runs_to_cp = identify_runs(args.RunFolder)
    # Copy analysis folder
    for item in sorted(list(runs_to_cp)):
        send_run_to_cp(item, args.ArchiveFolder, args.Dry,args.LogFile)

if __name__ == "__main__":
    main()
