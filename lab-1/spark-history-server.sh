#!/bin/bash

docker run -it --rm -v "`pwd`"/spark-events:/spark-events \
-p 18080:18080 spark-history-server

