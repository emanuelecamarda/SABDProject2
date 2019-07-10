#!/bin/bash

if [ "$1" = "cluster" ]; then
    /apache-storm-1.1.0/bin/storm jar /data/SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar Topology
    exit
fi

/apache-storm-1.1.0/bin/storm jar /data/SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar Topology query1
