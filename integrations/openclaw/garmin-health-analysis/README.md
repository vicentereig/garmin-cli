# garmin-health-analysis

OpenClaw skill wrapper for the local `garmin` CLI.

This skill uses the source-built Garmin CLI from the local fork:
- Repo: `/home/grunt/.openclaw/workspace/skills/garmin-cli`
- Binary: `/home/grunt/.local/bin/garmin`

The skill itself is lightweight. Natural-language interpretation lives in `SKILL.md`, and command execution happens through direct `garmin` CLI calls.
