#!/bin/bash

docker run -it --rm -v "`pwd`":/root sbt sbt "$@"

