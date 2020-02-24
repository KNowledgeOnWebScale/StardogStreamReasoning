#!/bin/bash
#rm -f $STARDOG_HOME/system.lock
cp -f /stardog-license/stardog-license-key.bin $STARDOG_HOME 2>/dev/null
/opt/stardog/bin/stardog-admin server start --foreground --disable-security
