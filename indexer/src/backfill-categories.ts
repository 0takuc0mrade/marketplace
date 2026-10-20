import dotenv from 'dotenv';
import prisma from './db.js';
import { resolveMetadataCategory } from './ipfs.js';

dotenv.config();

/**
 * Retry a database operation with exponential backoff for transient failures.
 */
async function withRetry<T>(
    operation: () => Promise<T>,
    maxRetries = 3,
    baseDelayMs = 1000
): Promise<T> {
    let lastError: Error;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            return await operation();
        } catch (error) {
            lastError = error instanceof Error ? error : new Error(String(error));
            if (attempt === maxRetries) break;

            // Check if error is transient (connection issues, timeouts)
            const isTransient =
                lastError.message.includes('connection') ||
                lastError.message.includes('timeout') ||
                lastError.message.includes('ECONNREFUSED') ||
                lastError.message.includes('ECONNRESET');

            if (!isTransient) throw lastError;

            const delay = baseDelayMs * Math.pow(2, attempt);
            console.warn({
                msg: 'Category backfill: retrying operation',
                attempt: attempt + 1,
                delayMs: delay,
                error: lastError.message,
            });
            await new Promise((resolve) => setTimeout(resolve, delay));
        }
    }
    throw lastError!;
}

/**
 * Backfill the `category` column on listings created before category
 * denormalization existed. Resolves each distinct metadata CID once (cached
 * in-memory) and updates every listing that shares it. Best-effort — listings
 * whose metadata cannot be resolved keep `category = null`.
 */
export async function runCategoryBackfill() {
    const BATCH_SIZE = 200;

    let rows;
    try {
        rows = await withRetry(() =>
            prisma.listing.findMany({
                where: { category: null },
                select: { listingId: true, metadataCid: true },
                orderBy: { listingId: 'asc' },
            })
        );
    } catch (error) {
        console.error({
            msg: 'Category backfill: failed to fetch listings',
            err: error instanceof Error ? error.message : String(error),
        });
        throw error;
    }

    console.log({ msg: 'Category backfill: found candidates', count: rows.length });

    const categoryByCid = new Map<string, string>();
    let updated = 0;
    let failed = 0;

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        for (const row of batch) {
            const cid = row.metadataCid;
            if (!cid) continue;

            try {
                let category = categoryByCid.get(cid);
                if (category === undefined) {
                    category = (await resolveMetadataCategory(cid)) ?? '';
                    categoryByCid.set(cid, category);
                }

                if (!category) {
                    failed++;
                    continue;
                }

                await withRetry(() =>
                    prisma.listing.update({
                        where: { listingId: row.listingId },
                        data: { category },
                    })
                );
                updated++;
            } catch (error) {
                console.error({
                    msg: 'Category backfill: failed to update listing',
                    listingId: row.listingId,
                    err: error instanceof Error ? error.message : String(error),
                });
                failed++;
            }
        }
        console.log({
            msg: 'Category backfill: batch progress',
            processed: Math.min(i + BATCH_SIZE, rows.length),
            updated,
            failed,
        });
    }

    console.log({
        msg: 'Category backfill complete',
        candidates: rows.length,
        updated,
        unresolved: failed,
        distinctCids: categoryByCid.size,
    });
}

if (process.argv[1] && process.argv[1].includes('backfill-categories')) {
    runCategoryBackfill().catch((err) => {
        console.error({
            msg: 'Category backfill failed',
            err: err instanceof Error ? err.message : String(err),
        });
        process.exit(1);
    });
}