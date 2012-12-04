#!/bin/bash

SPECS=`dirname $0`
MOCHA=$SPECS/../node_modules/mocha/bin/mocha
echo Running $SPECS/$1*

$MOCHA --timeout 10000 --reporter spec --require should --compilers coffee:coffee-script $2 $SPECS/$1*
