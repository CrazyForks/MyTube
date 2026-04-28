/* tsqllint-disable */
ALTER TABLE "subscriptions" ADD COLUMN "retention_days" integer;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "download_history_retention_subscription_idx" ON "download_history" ("subscription_id", "status", "finished_at");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "download_history_retention_video_refs_idx" ON "download_history" ("video_id", "status", "subscription_id");
