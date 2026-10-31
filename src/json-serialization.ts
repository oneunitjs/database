import type { QueryResult } from "pg";

function toUtf8(value: unknown): Buffer {
    return Buffer.from(JSON.stringify(value ?? null), "utf8");
}

function fromUtf8(buffer: Buffer | Uint8Array | string): unknown {
    const text = Buffer.isBuffer(buffer) || buffer instanceof Uint8Array
        ? Buffer.from(buffer).toString("utf8")
        : String(buffer);
    return JSON.parse(text);
}

export function encode(value: unknown): Buffer {
    return toUtf8(value);
}

export function decode(buffer: Buffer | Uint8Array | string): unknown {
    return fromUtf8(buffer);
}

export function encodeToString(value: unknown): string {
    return JSON.stringify(value ?? null);
}

export function decodeFromString(value: string): unknown {
    return JSON.parse(value);
}

export function serializeRow(row: Record<string, unknown>): Buffer {
    return encode(row);
}

export function deserializeRow(buffer: Buffer | Uint8Array | string): Record<string, unknown> {
    return decode(buffer) as Record<string, unknown>;
}

export function serializeQueryResult(result: QueryResult): Buffer {
    return encode({
        rows: result?.rows ?? [],
        rowCount: result?.rowCount ?? 0,
        fields: result?.fields ?? [],
    });
}

export function deserializeQueryResult(buffer: Buffer | Uint8Array | string): { rows: unknown[]; rowCount: number; fields: unknown[] } {
    return decode(buffer) as { rows: unknown[]; rowCount: number; fields: unknown[] };
}