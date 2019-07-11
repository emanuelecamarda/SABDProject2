#!/bin/bash

if [ "$1" = "1" ]; then

    if [ "$2" = "local" ]; then
        /apache-storm-1.1.0/bin/storm jar /data/SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar TopologyQ1
        exit
    fi

    /apache-storm-1.1.0/bin/storm jar /data/SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar TopologyQ1 query1
    exit

fi

if [ "$1" = "2" ]; then

    if [ "$2" = "local" ]; then
        /apache-storm-1.1.0/bin/storm jar /data/SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar TopologyQ2
        exit
    fi

    /apache-storm-1.1.0/bin/storm jar /data/SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar TopologyQ2 query2
    exit

fi

echo    "Usage:
echo    "   sh ./launchTopology <query number> [local]"
echo    "       - query number can be 1 or 2."
