export class SchemaRollbackCoordinator {
    constructor(private readonly syncService: any) {}
    async rollback(migration: any, options?: any) {
        return this.syncService?.rollback?.(migration, options);
    }
}
