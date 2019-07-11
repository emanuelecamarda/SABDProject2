#!/bin/bash

if [ "$1" = "query1" ]; then

    if [ "$2" = "1Hour" ]; then
        java -cp .:SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar ExternalConsumer 1 1
        exit
    fi

    if [ "$2" = "24Hour" ]; then
        java -cp .:SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar ExternalConsumer 1 2
        exit
    fi

    if [ "$2" = "7Day" ]; then
        java -cp .:SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar ExternalConsumer 1 3
        exit
    fi

    continue
fi

if [ "$1" = "query2" ]; then

    if [ "$2" = "24Hour" ]; then
        java -cp .:SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar ExternalConsumer 2 1
        exit
    fi

    if [ "$2" = "7Day" ]; then
        java -cp .:SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar ExternalConsumer 2 2
        exit
    fi

    if [ "$2" = "1Month" ]; then
        java -cp .:SABDProject2-1.0-SNAPSHOT-jar-with-dependencies.jar ExternalConsumer 2 3
        exit
    fi

    continue
fi

echo    "Usage: sh ./launchMonitor query1|query2 <windowSize>"
echo    "   if query1, windowSize can be 1Hour|24Hour|7Day"
echo    "   if query2, windowSize can be 24Hour|7Day|1Month"
