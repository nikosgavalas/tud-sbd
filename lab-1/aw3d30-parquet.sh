#!/bin/bash

docker run -it --rm -v `pwd`:/io aw3d30 -t /io/tif -p /io/parquet "$1" # $1 in {netherlands,europe,world}

