import { and, eq, inArray, isNotNull, isNull, lt, ne, or } from "drizzle-orm";
import { db } from "../db";
import { downloadHistory, subscriptions } from "../db/schema";
import { logger } from "../utils/logger";
import * as storageService from "./storageService";

// Max videos to delete per subscription per hourly tick, preventing a spike
// when retention is first enabled on a subscription with a large backlog.
const CANDIDATES_PER_SUBSCRIPTION_PER_RUN = 100;

export interface SubscriptionRetentionCleanupSummary {
  checkedSubscriptions: number;
  deletedVideos: number;
  skippedMissingVideos: number;
  skippedSharedVideos: number;
  errors: number;
}

let isCleanupRunning = false;

async function getExternallyReferencedVideoIds(
  videoIds: string[],
  subscriptionId: string
): Promise<Set<string>> {
  if (videoIds.length === 0) {
    return new Set();
  }

  const externalReferences = await db
    .select({ videoId: downloadHistory.videoId })
    .from(downloadHistory)
    .where(
      and(
        inArray(downloadHistory.videoId, videoIds),
        eq(downloadHistory.status, "success"),
        or(
          isNull(downloadHistory.subscriptionId),
          ne(downloadHistory.subscriptionId, subscriptionId)
        )
      )
    );

  return new Set(
    externalReferences
      .map((reference) => reference.videoId)
      .filter((videoId): videoId is string => Boolean(videoId))
  );
}

/**
 * Deletes videos downloaded exclusively by a subscription once they are older
 * than that subscription's retentionDays setting.
 *
 * Shared videos (referenced by another subscription's success history or a
 * manual download) are intentionally kept: `download_history.status='deleted'`
 * means the local file is gone, so flipping a row while another row still
   * keeps the file alive would be a lie. As a result, a video shared by two
   * subscriptions persists until every other referencing entry is removed by
   * history cleanup or the file itself is manually deleted.
 */
export async function runSubscriptionRetentionCleanup(): Promise<SubscriptionRetentionCleanupSummary> {
  const summary: SubscriptionRetentionCleanupSummary = {
    checkedSubscriptions: 0,
    deletedVideos: 0,
    skippedMissingVideos: 0,
    skippedSharedVideos: 0,
    errors: 0,
  };

  if (isCleanupRunning) {
    logger.debug("[RetentionCleanup] Cleanup already running, skipping tick");
    return summary;
  }

  isCleanupRunning = true;

  try {
    const subsWithRetention = await db
      .select()
      .from(subscriptions)
      .where(isNotNull(subscriptions.retentionDays));

    summary.checkedSubscriptions = subsWithRetention.length;

    if (subsWithRetention.length === 0) {
      return summary;
    }

    logger.info(
      `[RetentionCleanup] Checking ${subsWithRetention.length} subscription(s) with retention policy`
    );

    for (const subscription of subsWithRetention) {
      const retentionDays = subscription.retentionDays;
      // Keep this guard for defensive handling of legacy/manual database values.
      if (!retentionDays || retentionDays <= 0) {
        continue;
      }

      const cutoffMs = Date.now() - retentionDays * 24 * 60 * 60 * 1000;
      const candidates = await db
        .select({
          id: downloadHistory.id,
          videoId: downloadHistory.videoId,
          finishedAt: downloadHistory.finishedAt,
        })
        .from(downloadHistory)
        .where(
          and(
            eq(downloadHistory.subscriptionId, subscription.id),
            eq(downloadHistory.status, "success"),
            isNotNull(downloadHistory.videoId),
            lt(downloadHistory.finishedAt, cutoffMs)
          )
        )
        .limit(CANDIDATES_PER_SUBSCRIPTION_PER_RUN);

      if (candidates.length === CANDIDATES_PER_SUBSCRIPTION_PER_RUN) {
        logger.info(
          `[RetentionCleanup] Reached per-run limit (${CANDIDATES_PER_SUBSCRIPTION_PER_RUN}) for subscription "${subscription.author}" — remaining videos will be cleaned up in the next tick`
        );
      }

      const uniqueCandidates = Array.from(
        new Map(
          candidates
            .filter((candidate) => Boolean(candidate.videoId))
            .map((candidate) => [candidate.videoId as string, candidate])
        ).values()
      );
      const candidateVideoIds = uniqueCandidates.map(
        (candidate) => candidate.videoId as string
      );
      const externallyReferencedVideoIds =
        await getExternallyReferencedVideoIds(candidateVideoIds, subscription.id);

      for (const candidate of uniqueCandidates) {
        if (!candidate.videoId) {
          continue;
        }

        const videoRecord = storageService.getVideoById(candidate.videoId);
        if (!videoRecord) {
          storageService.markDownloadHistoryDeletedByVideoId(candidate.videoId);
          summary.skippedMissingVideos += 1;
          continue;
        }

        if (externallyReferencedVideoIds.has(candidate.videoId)) {
          summary.skippedSharedVideos += 1;
          logger.debug(
            `[RetentionCleanup] Skipping shared video id=${candidate.videoId} for subscription ${subscription.id}`
          );
          continue;
        }

        try {
          const deleted = storageService.deleteVideo(candidate.videoId);
          if (deleted) {
            summary.deletedVideos += 1;
            logger.info(
              `[RetentionCleanup] Deleted video "${videoRecord.title}" (id=${candidate.videoId}) ` +
                `from subscription "${subscription.author}" (retentionDays=${retentionDays})`
            );
          }
        } catch (error) {
          summary.errors += 1;
          logger.error(
            `[RetentionCleanup] Failed to delete video ${candidate.videoId}:`,
            error instanceof Error ? error : new Error(String(error))
          );
        }
      }
    }

    return summary;
  } catch (error) {
    summary.errors += 1;
    logger.error(
      "[RetentionCleanup] Unexpected error during retention cleanup:",
      error instanceof Error ? error : new Error(String(error))
    );
    return summary;
  } finally {
    isCleanupRunning = false;
  }
}
