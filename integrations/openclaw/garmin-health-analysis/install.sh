#!/bin/bash
set -e

if ! command -v garmin >/dev/null 2>&1; then
  echo "garmin binary not found in PATH"
  exit 1
fi

echo "garmin-health-analysis skill is ready."
garmin --version
