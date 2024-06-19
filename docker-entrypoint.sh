#!/usr/bin/env bash

args=("$@")

case "${1}" in
    "bash")
        shift
        exec bash -c "${args[@]:1}"
        ;;
    "delay")
        exec bash -c "while true; do sleep 20; done"
        ;;
    "cli")
        exec bash -c "./cli.py ${args[@]:1}"
        ;;
    "alembic")
        exec bash -c "alembic ${args[@]:1}"
        ;;
esac
