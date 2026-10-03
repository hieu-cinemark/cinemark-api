"""Everything LLM, both providers (Kira, Bee) in one place:

  client.py    provider-agnostic OpenAI-compatible HTTP client - credentials
               (ai_providers table), retry/backoff, concurrency, JSON parsing
  kira.py      Kira task policy: per-task enabled toggle + system-prompt
               overrides from the ai_settings table, call_kira
  bee.py       Beeknoee facade (call_bee, DEFAULT_BEE_MODEL) - reports only
  defaults.py  code-default model + per-task system prompts
  prompts/     the prompt text for each task
  tasks/       the classifiers/generators built on top: post_relevance
               (ingest), sentiment (comment sweep) and import_parser
               (Settings bulk import) on Kira; report (social-topic
               reports) on Bee, falling back to Kira

The non-AI lexicon sentiment fallback lives in app/services/sentiment_lexicon.py.
"""
