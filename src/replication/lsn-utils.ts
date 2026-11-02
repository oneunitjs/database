/**
 * PostgreSQL WAL LSN helpers. An LSN is a 64-bit value written as
 * `high/low` hexadecimal segments (e.g. `0/16B3748`).
 */
export class LsnUtils {
    static parseLsn(lsn: string): bigint {
        if (typeof lsn !== "string") {
            throw new Error(`Invalid LSN format: ${String(lsn)}`);
        }
        const trimmed = lsn.trim();
        if (trimmed.length === 0) {
            throw new Error("Invalid LSN format: empty string");
        }
        const parts = trimmed.split("/");
        if (parts.length === 1) {
            if (!/^[0-9a-fA-F]+$/.test(parts[0])) {
                throw new Error(`Invalid LSN format: ${lsn}`);
            }
            return BigInt(`0x${parts[0]}`);
        }
        if (parts.length !== 2 || parts[0].length === 0 || parts[1].length === 0) {
            throw new Error(`Invalid LSN format: ${lsn}`);
        }
        if (!/^[0-9a-fA-F]+$/.test(parts[0]) || !/^[0-9a-fA-F]+$/.test(parts[1])) {
            throw new Error(`Invalid LSN format: ${lsn}`);
        }
        const first = BigInt(`0x${parts[0]}`);
        const second = BigInt(`0x${parts[1]}`);
        return (first << 32n) | second;
    }

    static formatLsn(value: bigint): string {
        if (value < 0n) {
            throw new Error(`LSN cannot be negative: ${value.toString()}`);
        }
        const high = (value >> 32n) & 0xffffffffn;
        const low = value & 0xffffffffn;
        return `${high.toString(16).toUpperCase()}/${low.toString(16).toUpperCase()}`;
    }

    /**
     * Byte difference between two LSNs. Positive when `lsn2` is ahead of `lsn1`.
     */
    static diffLsn(lsn1: string, lsn2: string): bigint {
        return this.parseLsn(lsn2) - this.parseLsn(lsn1);
    }

    static compareLsn(lsn1: string, lsn2: string): number {
        const val1 = this.parseLsn(lsn1);
        const val2 = this.parseLsn(lsn2);
        if (val1 < val2) return -1;
        if (val1 > val2) return 1;
        return 0;
    }

    /**
     * Replication lag in bytes: primary minus replica. Positive when the
     * replica is behind; zero when caught up; negative if the replica reports
     * a position ahead of the primary (clock/LSN anomaly).
     */
    static lagBytes(primaryLsn: string, replicaLsn: string): bigint {
        return this.parseLsn(primaryLsn) - this.parseLsn(replicaLsn);
    }

    static formatBytes(bytes: bigint): string {
        const abs = bytes < 0n ? -bytes : bytes;
        const sign = bytes < 0n ? "-" : "";
        if (abs < 1024n) return `${sign}${abs.toString()}B`;
        if (abs < 1024n * 1024n) return `${sign}${(abs / 1024n).toString()}KB`;
        if (abs < 1024n * 1024n * 1024n) return `${sign}${(abs / (1024n * 1024n)).toString()}MB`;
        return `${sign}${(abs / (1024n * 1024n * 1024n)).toString()}GB`;
    }
}
