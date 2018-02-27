#!/bin/bash
echo "Reading config and spawning replicas..." >&2
trap "kill 0" EXIT

while IFS='' read -r line || [[ -n "$line" ]]; do
    python3 rep.py $line &
done < "replicas.config"
wait
