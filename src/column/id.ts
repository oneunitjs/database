export function id(): Record<string, string> {
    return {
        id: "UUID PRIMARY KEY DEFAULT gen_random_uuid()",
    };
}

export function bigIntId(): Record<string, string> {
    return {
        id: "BIGSERIAL PRIMARY KEY",
    };
}

export function uuid(columnName = "id"): Record<string, string> {
    return {
        [columnName]: "UUID DEFAULT gen_random_uuid()",
    };
}