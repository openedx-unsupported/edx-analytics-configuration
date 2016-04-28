#!/bin/bash

set -ex

VENV_ROOT=$WORKSPACE/venvs
mkdir -p $VENV_ROOT

rm -rf $WORKSPACE/logs

if [ ! -d "$VENV_ROOT/analytics-tasks" ]
then
    virtualenv $VENV_ROOT/analytics-tasks
fi
if [ ! -d "$VENV_ROOT/analytics-configuration" ]
then
    virtualenv $VENV_ROOT/analytics-configuration
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

TASKS_REPO=${TASKS_REPO:-https://github.com/edx/edx-analytics-pipeline.git}
VIRTUALENV_EXTRA_ARGS="${VIRTUALENV_EXTRA_ARGS:-}"

# Define task on the command line, including the task name and all of its arguments.
# All arguments provided on the command line are passed through to the remote-task call.
remote-task --job-flow-name="$CLUSTER_NAME" --repo $TASKS_REPO --branch $TASKS_BRANCH --wait --log-path $WORKSPACE/logs/ --remote-name automation --user $TASK_USER --virtualenv-extra-args="$VIRTUALENV_EXTRA_ARGS" --secure-config-branch $SECURE_BRANCH --secure-config-repo $SECURE_REPO --secure-config $SECURE_CONFIG "$@"

cat $WORKSPACE/logs/* || true
