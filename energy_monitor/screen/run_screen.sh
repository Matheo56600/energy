#!/bin/sh

export PATH="/bin:/sbin:/usr/bin:/usr/sbin"
export TERM=linux

lua /root/energy_monitor/screen/main.lua
sleep 2
