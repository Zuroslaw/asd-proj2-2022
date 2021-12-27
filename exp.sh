#!/bin/bash

n=$1
membership="172.30.10.101,172.30.10.102,172.30.10.103"
client_machine="shelder-1"

t=3
job=$(oarsub -l \{"host in ('charmander-1', 'charmander-2', 'charmander-3', 'shelder-1')"\}/nodes=4 "./go.sh $membership $t $client_machine" | grep OAR_JOB_ID | cut -d'=' -f2)

for i in $(seq 2 "$n")
do
  t=$((i*3))
  job=$(oarsub -l \{"host in ('charmander-1', 'charmander-2', 'charmander-3', 'shelder-1')"\}/nodes=4 -a "$job" "./go.sh $membership $t $client_machine" | grep OAR_JOB_ID | cut -d'=' -f2)
done