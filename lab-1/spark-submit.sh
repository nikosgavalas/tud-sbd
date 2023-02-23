#!/bin/bash

docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit "$@"

