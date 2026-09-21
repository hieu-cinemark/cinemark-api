-- Parent comment's platform id (Threads reply pk / Facebook comment_id).
-- Null = top-level reply to the post. Safe to re-run: ignore duplicate-column errors.
ALTER TABLE comments ADD COLUMN parent_external_id TEXT;
