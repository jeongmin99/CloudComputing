#!/bin/bash
#슬롯 갯수
tmp=$(condor_status -l | grep '^Cpus')
uc=$(echo "$tmp" | wc -l)

#작업 갯수
jobs=$(condor_q | grep 'Total for all users' | awk -F "jobs" '{ print $0}' | awk -F ' ' '{print $5}')
ud=$(echo "$jobs")

#작업 할당된 노드의 hostname
sched=$(condor_status | grep 'internal' | grep 'Claimed' | awk -F " " '{print $1}')
echo "$uc""$ud""$sched"

