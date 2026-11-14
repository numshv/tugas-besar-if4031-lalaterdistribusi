#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <node-for-slot-a> <node-for-slot-b> <node-for-slot-c> <node-for-slot-d>" >&2
  exit 1
fi

slots=(a b c d)
for i in 0 1 2 3; do
  kubectl label node "${@:$((i + 1)):1}" "bnpb/slot=${slots[$i]}" --overwrite
done

kubectl get nodes -L bnpb/slot
