#!/bin/bash

mvn clean install package
cp target/*jar-with-dependencies.jar dist/data/.