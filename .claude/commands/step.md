---
description: Execute one step of docs/architecture.md and stop
argument-hint: <step number>
---
Read @docs/architecture.md and execute ONLY step $ARGUMENTS.
Previous steps are done and committed; do not redo or re-verify them.

When finished:
1. Run `uv run pytest` and `docker compose exec pipeline ruff check .`.
2. Summarize: files created/changed, test results, and decisions that need my confirmation.
3. STOP. Do not start the next step and do not commit.

Reply in Brazilian Portuguese.