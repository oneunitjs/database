import { createHash } from "node:crypto";

export type ChecksumAlgorithm = "md5" | "sha256" | "sha512";

export interface ChecksumResult {
    algorithm: ChecksumAlgorithm;
    hash: string;
    rowCount: number;
    bytesHashed: number;
}

export interface ChecksumComparison {
    match: boolean;
    source: ChecksumResult;
    destination: ChecksumResult;
}

function stableSerialize(value: unknown): string {
    if (value === null || value === undefined) {
        return "null";
    }
    if (typeof value === "object") {
        if (Array.isArray(value)) {
            return `[${value.map(stableSerialize).join(",")}]`;
        }
        const obj = value as Record<string, unknown>;
        const keys = Object.keys(obj).sort();
        return `{${keys.map((k) => `${JSON.stringify(k)}:${stableSerialize(obj[k])}`).join(",")}}`;
    }
    if (typeof value === "bigint") {
        return value.toString();
    }
    return JSON.stringify(value);
}

export class ChecksumCalculator {
    constructor(
        private readonly algorithm: ChecksumAlgorithm = "sha256",
        private readonly batchSize = 1000,
    ) {}

    hashRows(rows: ReadonlyArray<Record<string, unknown>>): ChecksumResult {
        const hash = createHash(this.algorithm);
        let bytesHashed = 0;
        for (const row of rows) {
            const serialized = stableSerialize(row);
            hash.update(serialized);
            hash.update("\n");
            bytesHashed += Buffer.byteLength(serialized) + 1;
        }
        return {
            algorithm: this.algorithm,
            hash: hash.digest("hex"),
            rowCount: rows.length,
            bytesHashed,
        };
    }

    hashBatches(batches: ReadonlyArray<ReadonlyArray<Record<string, unknown>>>): ChecksumResult {
        const hash = createHash(this.algorithm);
        let rowCount = 0;
        let bytesHashed = 0;
        for (const batch of batches) {
            for (const row of batch) {
                const serialized = stableSerialize(row);
                hash.update(serialized);
                hash.update("\n");
                bytesHashed += Buffer.byteLength(serialized) + 1;
                rowCount += 1;
            }
        }
        return {
            algorithm: this.algorithm,
            hash: hash.digest("hex"),
            rowCount,
            bytesHashed,
        };
    }

    getBatchSize(): number {
        return this.batchSize;
    }
}

export class ChecksumVerifier {
    constructor(private readonly calculator = new ChecksumCalculator()) {}

    compare(
        sourceRows: ReadonlyArray<Record<string, unknown>>,
        destinationRows: ReadonlyArray<Record<string, unknown>>,
    ): ChecksumComparison {
        const source = this.calculator.hashRows(sourceRows);
        const destination = this.calculator.hashRows(destinationRows);
        return {
            match: source.hash === destination.hash && source.rowCount === destination.rowCount,
            source,
            destination,
        };
    }

    compareHashes(source: ChecksumResult, destination: ChecksumResult): boolean {
        return source.hash === destination.hash && source.rowCount === destination.rowCount;
    }
}
