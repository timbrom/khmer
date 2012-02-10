#!/usr/bin/python

import os

cpus = [ ]

# Path to where the system CPU information is kept
cpuPath = "/sys/devices/system/cpu/"

# Find out how many CPUs there are
for dirs in os.listdir(cpuPath) :
    if dirs.startswith("cpu") and dirs[3].isdigit() :
        cpus.append(dirs)

# For each CPU, get its cache information
cpus.sort()
for cpu in cpus :
    print cpu + ":"

    # Find out how many caches the CPU has
    caches = os.listdir(cpuPath + cpu + "/cache") 
    caches.sort()

    # Print out the information for each cache
    for cache in caches :
        cachePath = cpuPath + cpu + "/cache/" + cache
        
        # Get the level and name of this cache
        f = open(cachePath + "/level", "r")
        level = f.read();
        f.close();
        f = open(cachePath + "/type", "r")
        cachetype = f.read();
        f.close();

        print "\tL" + level.rstrip() + " " + cachetype.rstrip()

        # Print the cache size
        f = open(cachePath + "/size", "r")
        value = f.read();
        f.close();

        print "\t\tSize: " + value.rstrip()

        # Is the cache shared?
        f = open(cachePath + "/shared_cpu_list", "r")
        value = f.read();
        f.close();

        tmp = value.split(",")

        if len(tmp) is 1 :
            print "\t\tShared: no"
        else :
            print "\t\tShared: yes (with CPUs " + value.rstrip()  + ")"

        # Print the cache line size
        f = open(cachePath + "/coherency_line_size", "r")
        value = f.read();
        f.close();

        print "\t\tLine size: " + value.rstrip()

        # Print the cache associativity
        f = open(cachePath + "/ways_of_associativity", "r")
        value = f.read();
        f.close();

        print "\t\tAssociativity: " + value.rstrip()

        
