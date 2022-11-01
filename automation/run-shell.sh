#!/bin/bash

set -ex

VENV_ROOT=$WORKSPACE/venvs
mkdir -p $VENV_ROOT

if [ ! -d "$VENV_ROOT/analytics-tasks" ]
then
    virtualenv --python=python2 $VENV_ROOT/analytics-tasks
fi
if [ ! -d "$VENV_ROOT/analytics-configuration" ]
then
    virtualenv --python=python3.8 $VENV_ROOT/analytics-configuration
fi

TASKS_BIN=$VENV_ROOT/analytics-tasks/bin
CONF_BIN=$VENV_ROOT/analytics-configuration/bin

. $CONF_BIN/activate
make -C analytics-configuration provision.emr

function terminate_cluster() {
    . $CONF_BIN/activate
    make -C analytics-configuration terminate.emr
}
if [ "$TERMINATE" = "true" ]; then
    trap terminate_cluster EXIT
fi

. $TASKS_BIN/activate
make -C analytics-tasks bootstrap

remote-task --job-flow-name="$CLUSTER_NAME" --user $TASK_USER --sudo-user '' --shell "$@"

