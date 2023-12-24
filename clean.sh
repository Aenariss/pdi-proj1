#!/bin/sh

pid=$(ps -ef | grep "redirectToLocalhost" | awk '{ print $2 }')
lines=$(echo $pid | wc -w)
if [ $lines -lt 2 ]
then
    return
else
    id=$(ps -ef | grep "redirectToLocalhost" | awk '{ print $2 }' | head -n 1)
    kill -9 $id
fi
