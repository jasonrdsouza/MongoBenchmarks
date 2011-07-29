#!/usr/bin/python

'''
DriveStats.py - Python script to get disk information in Windows. 

Uses the win32 extension module from:
-http://sourceforge.net/projects/pywin32/files/

'''


__author__ = ('jasonrdsouza (Jason Dsouza)')

import win32file
import datetime
import os


def get_drivestats(drive):
  '''Drive for instance 'C'
     returns total_space, free_space, used_space'''
  drive = drive.rstrip(':\\').rstrip(':/')
  sectPerCluster, bytesPerSector, freeClusters, totalClusters = \
      win32file.GetDiskFreeSpace(drive + ":\\")
  r = (
    (totalClusters*sectPerCluster*bytesPerSector),
    (freeClusters*sectPerCluster*bytesPerSector),
    ((totalClusters-freeClusters )*sectPerCluster*bytesPerSector)
  )
  return r

def printDriveStats(drive='C'):
  '''Helper function to print the drive space info.'''
  total_space, free_space, used_space = get_drivestats(drive)

  total_space_kb = total_space / 1024
  free_space_kb = free_space / 1024
  used_space_kb = used_space / 1024
  
  total_space_mb = total_space_kb / 1024
  free_space_mb = free_space_kb / 1024
  used_space_mb = used_space_kb / 1024

  total_space_gb = total_space_mb / 1024
  free_space_gb = free_space_mb / 1024
  used_space_gb = used_space_mb / 1024

  print("""
  total_space = %d bytes
  free_space = %d bytes
  used_space = %d bytes""" % (total_space, free_space, used_space))

  print("""
  total_space = %d kilobytes
  free_space = %d kilobytes
  used_space = %d kilobytes""" % (total_space_kb, free_space_kb, used_space_kb))
  
  print("""
  total_space_mb = %d megabytes
  free_space_mb = %d megabytes
  used_space_mb = %d megabytes""" % (total_space_mb, free_space_mb, used_space_mb))

  print("""
  total_space_gb = %d gigabytes
  free_space_gb = %d gigabytes
  used_space_gb = %d gigabytes""" % (total_space_gb, free_space_gb, used_space_gb))

def getDriveStatsLogEntry(drive='C'):
  '''Function to get a Mongo JSON style document for logging in the db'''
  total_space, free_space, used_space = get_drivestats(drive)

  total_space_kb = total_space / 1024
  free_space_kb = free_space / 1024
  used_space_kb = used_space / 1024
  
  total_space_mb = total_space_kb / 1024
  free_space_mb = free_space_kb / 1024
  used_space_mb = used_space_kb / 1024

  total_space_gb = total_space_mb / 1024
  free_space_gb = free_space_mb / 1024
  used_space_gb = used_space_mb / 1024
  
  entry = {"EntryType": "HD Usage",
           "Timestamp": datetime.datetime.now(),
           "Hostname": os.getenv('COMPUTERNAME'),
           "TotalSpace": [total_space, total_space_kb, total_space_mb, total_space_gb],
           "FreeSpace": [free_space, free_space_kb, free_space_mb, free_space_gb],
           "UsedSpace": [used_space, used_space_kb, used_space_mb, used_space_gb]}
  return entry

  
def main():
  '''Program to log the drive space of the computer'''

if __name__ == '__main__':
  main()
