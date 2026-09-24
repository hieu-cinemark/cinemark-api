"""Kira-task policy: per-task enabled toggle + system-prompt overrides
(app/kira/client.py) plus the classifiers built on top (relevance,
sentiment's Bee fallback, report, import_parser). The actual HTTP client,
retry/backoff and credentials live in app.ai_client, shared with app/bee/."""
