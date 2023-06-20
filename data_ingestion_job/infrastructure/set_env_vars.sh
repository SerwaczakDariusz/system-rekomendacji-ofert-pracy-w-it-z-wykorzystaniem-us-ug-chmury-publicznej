#!/usr/bin/env bash
if [ "$0" = "$BASH_SOURCE" ]; then
    echo "Error: Script must be sourced"
    exit 1
fi

export ARM_SUBSCRIPTION_ID="<INSERT VALUE>"
export ARM_CLIENT_ID="<INSERT VALUE>"
export ARM_TENANT_ID="<INSERT VALUE>"
export ARM_CLIENT_SECRET="<INSERT VALUE>"