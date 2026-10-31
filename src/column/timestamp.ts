export function timestamp(...columnNames: string[]): Record<string, string> {
    const names = columnNames.length > 0 ? columnNames : ["created_at"];

    return Object.fromEntries(
        names.map((columnName) => [
            columnName,
            "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        ]),
    );
}