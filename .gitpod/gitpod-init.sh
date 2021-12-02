#! /usr/bin/env bash

if [[ -n "${GITPOD_WORKSPACE_URL}" ]]; then
    # Move the VS Code settings to the location that VS Code uses for user settings
    mkdir -p .vscode && cp .gitpod/gitpod-vscode-settings.json .vscode/settings.json
fi
