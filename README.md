# psync-parser

These are the scripts for parsing pSync log files. Current branch parses the logs from the KGEC UAV drill.  

## Requirements

Download the logs and keep them in same directory as the scripts and change input to scripts accordingly(needs improvement)

## Drill scenario and output

This drill was performed with one mobile device and one Rasberry Pi mounted on a UAV. Objective was to find the data delivery rate at different altitude. After the UAV reaches the desired altitude pSnc was started. This was tested in various sessions altering the height of the UAV. 

This parser gives out the amount of data delivered within each session. 

## Steps to run 

1. Run `psync_analysis.py`
