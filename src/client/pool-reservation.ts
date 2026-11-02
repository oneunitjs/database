export class PoolReservationSystem {
    private readonly reserved = new Set<string>();

    async reserve(key: string): Promise<boolean> {
        if (this.reserved.has(key)) return false;
        this.reserved.add(key);
        return true;
    }

    release(key: string): void {
        this.reserved.delete(key);
    }

    isReserved(key: string): boolean {
        return this.reserved.has(key);
    }
}

export { PoolReservationSystem as PoolReservationManager };
