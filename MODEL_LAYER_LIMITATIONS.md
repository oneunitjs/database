# Model Layer Limitations - Deep Dive Analysis

> **Important:** This document provides a comprehensive analysis of the Model layer limitations in `@oneunit/database`. For usage documentation, see [README.md](README.md). For architecture details, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 📊 Overview

The **Model layer** (`model/`) in `@oneunit/database` is a **lightweight CRUD abstraction** that provides table-level operations. It is **not a full ORM** and lacks many features commonly found in ORMs like Sequelize, TypeORM, or Prisma.

This document details:
1. **Core limitations** of the Model layer
2. **When these limitations become problematic**
3. **Workarounds** for each limitation
4. **Recommendations** on when to use (or avoid) the Model layer
5. **Comparison** with full ORMs

---

## 🎯 Quick Decision Matrix

| Application Type | Model Layer Suitability | Recommendation |
| :--- | :--- | :--- |
| **Simple CRUD** (1-3 isolated tables) | ✅ Perfect | Use Model layer |
| **Admin Panel** (simple internal tool) | ✅ Great | Use Model layer |
| **Rapid Prototype** (speed over features) | ✅ Great | Use Model layer |
| **API with 3-5 tables** | ⚠️ Limited | Use Model + QueryBuilder |
| **API with 5+ related tables** | ❌ Inadequate | Use QueryBuilder or full ORM |
| **E-commerce** (Users, Orders, Products) | ❌ Inadequate | Use full ORM (Prisma, TypeORM) |
| **Social Network** (Users, Posts, Comments) | ❌ Inadequate | Use full ORM |
| **Financial App** (transactions, atomicity) | ❌ Inadequate | Use QueryBuilder + transactions |

---

## 🔍 1. CRUD Operations - Table-Level Abstractions Only

### What It Means

The Model layer provides **basic CRUD operations** but is **strictly limited to a single table per operation**. It cannot:
- Express relationships between tables
- Query across multiple tables
- Join tables together
- Cascade operations across related tables

### Implementation Details

```typescript
// From src/model/model.ts
interface Model<T extends QueryResultRow = QueryResultRow> {
    find(where?: object, options?: object): Promise<QueryResultRow[]>;
    findOne(where?: object, options?: object): Promise<QueryResultRow | null>;
    findById(id: any, options?: object): Promise<QueryResultRow | null>;
    insert(data: object, options?: object): Promise<QueryResultRow | null>;
    insertMany(rows: object[], options?: object): Promise<QueryResultRow[]>;
    update(where: object, data: object, options?: object): Promise<QueryResultRow[]>;
    updateById(id: any, data: object, options?: object): Promise<QueryResultRow | null>;
    delete(where: object, options?: object): Promise<QueryResultRow[]>;
    deleteById(id: any, options?: object): Promise<QueryResultRow | null>;
    count(where?: object, options?: object): Promise<number>;
    exists(where?: object, options?: object): Promise<boolean>;
}
```

**Every method operates on ONE table only.** There is no mechanism to:
- Define relationships (hasMany, belongsTo, hasOne)
- Include related data (eager loading, lazy loading)
- Query across associations
- Cascade operations to related tables

### What You CANNOT Do

```javascript
// ❌ CANNOT: Get user with their orders
const user = await db.model('users').findById(1, {
    include: ['orders'] // ❌ Not supported
});

// ❌ CANNOT: Join tables
const results = await db.model('users')
    .join('orders', 'users.id', 'orders.user_id')
    .execute(); // ❌ Not supported

// ❌ CANNOT: Cascade delete (delete user and all their orders)
await db.model('users').deleteById(1, {
    cascade: true // ❌ Not supported
});
```

### Why It's a Limitation

This makes the Model layer **unsuitable for applications with relationships** between entities. Most real-world applications have:
- Users with Orders
- Posts with Comments
- Products with Categories
- Tenants with Users

Without relationship support, you must manually:
- Write separate queries for each related table
- Manually join the results in application code
- Handle cascading operations yourself
- Ensure data consistency across related tables

### When to Use

✅ **Simple applications** with isolated tables (e.g., settings, logs, audit trails)
✅ **Rapid prototyping** where development speed matters more than relationship features
✅ **Single-table operations** that don't need joins or relationships
✅ **Internal tools** with simple data requirements

### When NOT to Use

❌ **Applications with >3 related tables**
❌ **E-commerce applications** (Users, Orders, Products, Reviews, Payments)
❌ **Social networks** (Users, Posts, Comments, Likes, Followers)
❌ **Any application requiring data from multiple tables in a single query**

---

## 🔍 2. find() - Limited Condition Complexity

### What It Means

The `find()` method only supports **simple equality conditions**. It cannot express:
- Nested AND/OR logic
- Comparison operators (`>`, `<`, `>=`, `<=`, `!=`, `LIKE`, `ILIKE`, `NOT LIKE`)
- IN/NOT IN clauses
- BETWEEN conditions
- NULL checks (`IS NULL`, `IS NOT NULL`)
- Full-text search
- JSON operators
- Custom SQL expressions

### Implementation Details

```typescript
// The find method accepts a simple key-value object
find(where?: object, options?: object)

// Example usage - ONLY equality:
db.model('users').find({ status: 'active', role: 'admin' })
// Generates: WHERE status = 'active' AND role = 'admin'
```

The `where` parameter is a plain object where:
- **Keys** are column names
- **Values** are literal values to match
- All conditions are combined with **AND**
- **No operators** can be specified

### What You CANNOT Do

```javascript
// ❌ CANNOT: Complex nested conditions
const users = await db.model('users').find({
    AND: [
        { status: 'active' },
        { OR: [{ role: 'admin' }, { role: 'superuser' }] }
    ]
});

// ❌ CANNOT: Comparison operators
const users = await db.model('users').find({
    age: { $gt: 25, $lt: 65 } // ❌ Not supported
});

// ❌ CANNOT: IN clause
const users = await db.model('users').find({
    id: [1, 2, 3] // ❌ Becomes: WHERE id = ARRAY[1,2,3] (not IN)
});

// ❌ CANNOT: NOT NULL check
const users = await db.model('users').find({
    email: { $ne: null } // ❌ Not supported
});

// ❌ CANNOT: LIKE operator
const users = await db.model('users').find({
    name: { $like: '%John%' } // ❌ Not supported
});

// ❌ CANNOT: Custom expressions
const users = await db.model('users').find({
    status: 'active',
    full_name: { $concat: ['first_name', ' ', 'last_name'] } // ❌ Not supported
});
```

### Workarounds

#### Option 1: Use QueryBuilder

```javascript
// ✅ Recommended: Use QueryBuilder for complex conditions
const users = await db.from('users')
    .where('status = $1 AND (role = $2 OR role = $3) AND age > $4 AND age < $5')
    .execute(['active', 'admin', 'superuser', 25, 65]);

// ✅ With named parameters
const users = await db.from('users')
    .where(sql`status = ${status} AND age BETWEEN ${minAge} AND ${maxAge}`)
    .execute();
```

#### Option 2: Use Raw SQL

```javascript
// ✅ Use raw SQL for complex queries
const users = await db.query(`
    SELECT * FROM users
    WHERE status = $1
      AND (role = $2 OR role = $3)
      AND age BETWEEN $4 AND $5
      AND email IS NOT NULL
`, ['active', 'admin', 'superuser', 25, 65]);
```

#### Option 3: Filter in Application Code (Not Recommended)

```javascript
// ⚠️ Not recommended: Fetch all and filter in memory
const allUsers = await db.model('users').find({});
const filtered = allUsers.filter(u => u.age > 25 && u.age < 65);
// This is inefficient for large datasets!
```

### Why It's a Limitation

Most real-world queries require complex filtering. The Model layer's simple object-based `where` clause is **too restrictive** for production applications because:

1. **Cannot express common query patterns** (ranges, multi-value, null checks)
2. **Cannot build dynamic queries** based on user input
3. **Cannot use PostgreSQL-specific features** (JSON operators, full-text search, etc.)
4. **Forces switching to QueryBuilder** for anything beyond simple equality

### When to Use

✅ **Simple equality lookups** (exact matches on known fields)
✅ **Primary key lookups** (`findById`)
✅ **Basic filtering** on 2-3 fields with exact values

### When NOT to Use

❌ **Dynamic filtering** with user-provided conditions
❌ **Range queries** (age between 25-65, date ranges)
❌ **Multi-value conditions** (IN clause)
❌ **Null checks** (IS NULL, IS NOT NULL)
❌ **Pattern matching** (LIKE, ILIKE, regex)
❌ **Complex boolean logic** (nested AND/OR)

---

## 🔍 3. insertMany() - Batch Inserts Without Atomic Transactions

### What It Means

The `insertMany()` method uses the **batch layer internally**, which:
- Splits large inserts into chunks to respect PostgreSQL's **65,535 parameter limit**
- Executes each chunk as a **separate query**
- **Does NOT wrap all chunks in a single transaction** by default
- Has **no atomicity guarantee** across chunks

### Implementation Details

```typescript
// From src/client/batch.ts
async function insertMany(table: string, rows: any[], options: InsertManyOptions) {
    const chunkSize = Math.min(
        options.chunkSize ?? 1000,
        Math.floor(65535 / columns.length)
    );

    for (const chunk of chunks) {
        await query(buildInsertQuery(table, chunk), chunkValues);
    }
}
```

Key observations:
- Each chunk is a separate `INSERT` statement
- If one chunk fails, previous chunks may have succeeded
- **No rollback** of already-inserted rows
- Default chunk size is 1000 rows

### What You CANNOT Do

```javascript
// ❌ CANNOT: Insert with atomic transaction across all rows
await db.model('users').insertMany([
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
    { id: 3, name: 'Charlie' }
]); // Each chunk runs separately, not in one transaction

// ❌ CANNOT: Guarantee all-or-nothing behavior
// If row 2 fails with a unique violation, rows 1 and 3 might still be inserted

// ❌ CANNOT: Rollback all on partial failure
await db.model('users').insertMany(largeArray, {
    atomic: true // ❌ Not supported
});
```

### Workarounds

#### Option 1: Wrap in Explicit Transaction

```javascript
// ✅ Recommended: Use explicit transaction
await db.transaction(async (client) => {
    const model = db.model('users');
    // Pass the transaction client to all operations
    await model.insertMany(largeArray, { client });
    // All rows inserted atomically
});
```

#### Option 2: Manual Chunking with Transaction

```javascript
// ✅ Manual chunking with transaction
await db.transaction(async (client) => {
    const chunkSize = 500;
    for (let i = 0; i < largeArray.length; i += chunkSize) {
        const chunk = largeArray.slice(i, i + chunkSize);
        await client.query(
            'INSERT INTO users (name) SELECT unnest($1::text[])',
            [chunk.map(r => r.name)]
        );
    }
});
```

#### Option 3: Use QueryBuilder for Batch Insert

```javascript
// ✅ Use QueryBuilder for better control
await db.from('users')
    .insert(largeArray)
    .execute();
// Note: This may still chunk internally, check implementation
```

### Why It's a Limitation

Without explicit transaction handling:
- **Partial inserts** can occur on failure
- **No atomicity** guarantee for large inserts
- **Inconsistent state** if some rows are inserted and others fail
- **Harder to debug** when inserts fail partway through

### When to Use

✅ **Small bulk inserts** (<1000 rows)
✅ **Non-critical data** where partial inserts are acceptable
✅ **Idempotent operations** where retries won't cause duplicates

### When NOT to Use

❌ **Financial transactions** requiring atomicity (all or nothing)
❌ **Large data migrations** where consistency matters
❌ **Critical data** where partial inserts cause problems
❌ **Operations with unique constraints** that could cause partial failures

---

## 🔍 4. Transactions - Manual Client Passing Required

### What It Means

The Model layer **does not have built-in transaction support**. To use models within a transaction, you must:
1. Manually acquire a client using `db.getClient()`
2. Pass it to **every** model operation via `{ client }` option
3. Manually release the client using `db.releaseClient()`
4. Manually commit or rollback

### Implementation Details

```typescript
// From src/model/model.ts
function insert(data: any, options: any = {}) {
    const client = options.client;
    if (client) {
        return client.query(buildInsertQuery(this.table, data));
    }
    return this.queryFn(buildInsertQuery(this.table, data));
}
```

The Model layer checks for a `client` option and uses it if provided. Otherwise, it uses the default query function which acquires and releases its own client for each operation.

### What You CANNOT Do

```javascript
// ❌ CANNOT: Automatic transaction support
await db.transaction(async () => {
    // Models don't automatically use the transaction client
    const user = await db.model('users').insert({ name: 'Alice' });
    const profile = await db.model('profiles').insert({ user_id: user.id });
    // Each operation uses a different connection, NOT in the transaction!
});

// ❌ CANNOT: Chained operations in a transaction
await db.model('users').insert({ name: 'Alice' })
    .then(() => db.model('profiles').insert({ user_id: 1 }))
    // These are separate connections, not in a transaction
```

### Workarounds

#### Option 1: Pass Client to Each Operation

```javascript
// ✅ Recommended: Manual transaction management with client passing
const client = await db.getClient();
try {
    const user = await db.model('users').insert(
        { name: 'Alice' },
        { client } // ⚠️ Must pass client to each operation
    );

    const profile = await db.model('profiles').insert(
        { user_id: user.id, bio: '...' },
        { client } // ⚠️ Must pass client to each operation
    );

    // Explicit commit (optional, auto-commits on release if no error)
    await client.query('COMMIT');
} catch (error) {
    await client.query('ROLLBACK');
    throw error;
} finally {
    db.releaseClient(client);
}
```

#### Option 2: Use QueryBuilder for Transactional Operations

```javascript
// ✅ Recommended: Use QueryBuilder which automatically uses transaction client
await db.transaction(async (client) => {
    const userResult = await client.query(
        'INSERT INTO users (name) VALUES ($1) RETURNING *',
        ['Alice']
    );
    
    await client.query(
        'INSERT INTO profiles (user_id, bio) VALUES ($1, $2)',
        [userResult.rows[0].id, '...']
    );
    // All operations use the same client/transaction
});
```

#### Option 3: Create a Transaction-Aware Model

```javascript
// ✅ Advanced: Create a helper function
async function withTransaction(db, callback) {
    const client = await db.getClient();
    try {
        const tx = { client };
        await callback(tx);
        await client.query('COMMIT');
    } catch (error) {
        await client.query('ROLLBACK');
        throw error;
    } finally {
        db.releaseClient(client);
    }
}

// Usage
await withTransaction(db, async (tx) => {
    await db.model('users').insert({ name: 'Alice' }, tx);
    await db.model('profiles').insert({ user_id: 1, bio: '...' }, tx);
});
```

### Why It's a Limitation

This makes the Model layer **cumbersome for transactional operations** because:

1. **Easy to forget** to pass the `{ client }` option
2. **Hard to maintain** - must remember for every operation
3. **No automatic rollback** - must manually handle errors
4. **Inconsistent API** - different from QueryBuilder which automatically uses transaction client
5. **More boilerplate** code for transactional workflows

### When to Use

✅ **Non-transactional operations** (single statements)
✅ **Simple scripts** where transactions aren't needed
✅ **Read-only operations** that don't need transactions

### When NOT to Use

❌ **Complex workflows** requiring multiple operations
❌ **Financial applications** needing ACID compliance
❌ **Any operation requiring atomicity** across multiple tables
❌ **Data migrations** where consistency is critical

---

## 🔍 5. Type Safety - Generic TypeScript Without Runtime Validation

### What It Means

The Model layer provides **TypeScript type safety at compile time**, but has **no runtime validation**:

- **No runtime type checking** - Values are not validated before insert/update
- **No schema validation** - Doesn't verify data matches table schema
- **No type coercion** - Doesn't convert between types (e.g., string to number)
- **Generic types only** - Uses TypeScript generics but doesn't enforce them at runtime
- **Type erasure** - TypeScript types are removed at runtime, so the database receives raw JavaScript objects

### Implementation Details

```typescript
// From src/model/model.ts
function createModel<T extends QueryResultRow = QueryResultRow>(table: string): Model<T> {
    return {
        async insert(data: Partial<T>, options?: any): Promise<T | null> {
            // ❌ No validation of 'data' against table schema
            // ❌ No validation that 'data' matches type T
            // ❌ Type assertion to 'any' loses type safety
            return queryFn(buildInsertQuery(table, data as any));
        },
        // ... other methods use the same pattern
    };
}
```

The `data as any` type assertion bypasses TypeScript's type checking at runtime.

### What You CANNOT Do

```javascript
// ❌ CANNOT: Runtime type validation
interface User {
    id: number;
    name: string;
    email: string;
}

const users = db.model<User>('users');

// This compiles fine but will fail at runtime:
await users.insert({
    id: 'not-a-number', // ❌ Wrong type, but NO runtime check
    name: 123,          // ❌ Wrong type, but NO runtime check
    email: null         // ❌ Missing required field, but NO check
});

// ❌ CANNOT: Schema validation
// No built-in way to ensure data matches table schema

// ❌ CANNOT: Type coercion
// Won't automatically convert '5' to 5 for a numeric column
```

### Workarounds

#### Option 1: Use a Validation Library

```javascript
// ✅ Recommended: Use a validation library like Zod, Joi, or Yup
import { z } from 'zod';

const UserSchema = z.object({
    id: z.number().optional(),
    name: z.string().min(1).max(255),
    email: z.string().email(),
    age: z.number().min(0).max(150).optional(),
});

async function insertUser(data: any) {
    // Runtime validation with helpful error messages
    const validated = UserSchema.parse(data);
    return db.model('users').insert(validated);
}

// Usage
try {
    await insertUser({ name: 'Alice', email: 'invalid' });
} catch (error) {
    // Error: "email must be a valid email address"
    console.error(error);
}
```

#### Option 2: Use TypeBox for Runtime + TypeScript Types

```javascript
// ✅ TypeBox provides both TypeScript types and runtime validation
import { Static, Type } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';

const UserSchema = Type.Object({
    id: Type.Optional(Type.Number()),
    name: Type.String({ minLength: 1, maxLength: 255 }),
    email: Type.String({ format: 'email' }),
});

type User = Static<typeof UserSchema>;

async function insertUser(data: unknown): Promise<User> {
    if (!Value.Check(UserSchema, data)) {
        throw new Error('Invalid user data');
    }
    return db.model<User>('users').insert(data);
}
```

#### Option 3: Use PostgreSQL Constraints

```javascript
// ✅ Let PostgreSQL validate (but you get database errors, not app errors)
await db.schema.create.table('users', {
    id: 'SERIAL PRIMARY KEY',
    name: 'VARCHAR(255) NOT NULL',
    email: 'VARCHAR(255) NOT NULL CHECK (email ~* \'^[A-Za-z0-9._%-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}$\')',
    age: 'INTEGER CHECK (age >= 0 AND age <= 150)',
});

// Now invalid data will cause PostgreSQL errors
// But error messages are less user-friendly
```

### Why It's a Limitation

TypeScript types are **erased at runtime**. The Model layer:

1. **Doesn't prevent invalid data** from reaching the database
2. **Doesn't provide helpful error messages** for type mismatches
3. **Doesn't validate required fields** before insert
4. **Doesn't perform type coercion** (string '5' won't become number 5)
5. **Relying on TypeScript alone is unsafe** for production applications

### When to Use

✅ **Trusted internal code** where types are known and validated elsewhere
✅ **Rapid development** where speed matters more than runtime safety
✅ **Simple applications** with minimal data complexity

### When NOT to Use

❌ **API endpoints** accepting user input from external sources
❌ **Public interfaces** where data comes from untrusted sources
❌ **Critical data** where validation is essential
❌ **Applications with complex data requirements**

---

## 🚨 Critical Limitations - Deep Dive

These are the **most impactful limitations** that make the Model layer unsuitable for production applications with any complexity.

---

### 1. No Relationships Support

**What It Means:**
The Model layer has **no concept of table relationships**. You cannot:
- Define `hasMany`, `belongsTo`, `hasOne`, `manyToMany` relationships
- Load related data (eager loading or lazy loading)
- Query across relationships
- Cascade operations to related tables

**Example:**
```javascript
// ❌ CANNOT: Define relationships
const User = db.model('users', {
    hasMany: 'orders' // ❌ Not supported
});

// ❌ CANNOT: Load with relationships
const user = await db.model('users').findById(1, {
    include: ['orders'] // ❌ Not supported
});

// ❌ CANNOT: Cascade delete
await db.model('users').deleteById(1, {
    cascade: true // ❌ Not supported
});
```

**Workaround:**
```javascript
// ✅ Manual relationship handling
class UserService {
    async getUserWithOrders(userId: number) {
        const user = await db.model('users').findById(userId);
        if (!user) throw new Error('User not found');
        
        const orders = await db.query(
            'SELECT * FROM orders WHERE user_id = $1',
            [userId]
        );
        
        return { ...user, orders };
    }
}

// ✅ Or use QueryBuilder for joins
const results = await db.from('users')
    .leftJoin('orders', 'users.id', 'orders.user_id')
    .select('users.*', 'orders.*')
    .execute();
```

**Impact:**
- **More code** to write and maintain
- **N+1 query problem** (multiple queries instead of one efficient join)
- **More complex** error handling
- **No consistency** in relationship handling across the application
- **Harder to refactor** when schema changes

---

### 2. No Join Support

**What It Means:**
The Model layer **cannot perform JOIN operations**. Every query is against a **single table only**.

**Example:**
```javascript
// ❌ CANNOT: Join tables
const results = await db.model('users')
    .join('orders', 'users.id', 'orders.user_id')
    .select('users.*', 'orders.*')
    .execute(); // ❌ Not supported

// ❌ CANNOT: Complex queries with multiple tables
const results = await db.model('users')
    .where({ orders: { total: { $gt: 100 } } }) // ❌ Not supported
    .execute();
```

**Workaround:**
```javascript
// ✅ Use QueryBuilder for joins
const results = await db.from('users')
    .innerJoin('orders', 'users.id', 'orders.user_id')
    .leftJoin('profiles', 'users.id', 'profiles.user_id')
    .select('users.*', 'orders.total', 'profiles.bio')
    .where('orders.total > $1', [100])
    .execute();

// ✅ Or use raw SQL
const results = await db.query(`
    SELECT users.*, orders.total, profiles.bio
    FROM users
    INNER JOIN orders ON users.id = orders.user_id
    LEFT JOIN profiles ON users.id = profiles.user_id
    WHERE orders.total > $1
`, [100]);
```

**Impact:**
- **Cannot use Model layer** for any query requiring joins
- **Must switch to QueryBuilder** or raw SQL
- **Inconsistent API usage** across the application
- **Defeats the purpose** of using the Model layer for complex applications

---

### 3. No Soft Delete Support

**What It Means:**
The Model layer has **no built-in support** for soft delete patterns (marking records as deleted without actually removing them).

**Example:**
```javascript
// ❌ CANNOT: Soft delete
await db.model('users').deleteById(1, {
    soft: true // ❌ Not supported
});

// ❌ CANNOT: Query only non-deleted records by default
const users = await db.model('users').find({}); // ❌ Includes deleted records

// ❌ CANNOT: Toggle soft delete behavior
const users = await db.model('users').find({}, {
    withDeleted: false // ❌ Not supported
});
```

**Workaround:**
```javascript
// ✅ Manual implementation with schema
await db.schema.create.table('users', {
    id: 'SERIAL PRIMARY KEY',
    name: 'VARCHAR(255)',
    deleted_at: 'TIMESTAMP WITH TIME ZONE DEFAULT NULL',
    deleted: 'BOOLEAN DEFAULT FALSE',
});

// Soft delete
await db.model('users').updateById(1, {
    deleted_at: new Date(),
    deleted: true
});

// Query non-deleted
const activeUsers = await db.model('users').find({
    deleted: false
    // or: deleted_at: null
});

// ✅ Create a helper for consistency
class UserService {
    async findActive(where: object = {}) {
        return db.model('users').find({ ...where, deleted: false });
    }
    
    async softDelete(id: number) {
        return db.model('users').updateById(id, {
            deleted_at: new Date(),
            deleted: true
        });
    }
    
    async restore(id: number) {
        return db.model('users').updateById(id, {
            deleted_at: null,
            deleted: false
        });
    }
}
```

**Impact:**
- **Manual implementation** required for each model
- **Inconsistent** soft delete handling across models
- **Error-prone** (easy to forget to add `deleted: false` filter)
- **No built-in scopes** (like `withDeleted()` or `onlyDeleted()`)
- **No automatic timestamps** for delete operations

---

### 4. No Automatic Timestamps

**What It Means:**
The Model layer **does not automatically** set `created_at` and `updated_at` fields. You must:
- Manually add them to your schema
- Manually set them before insert/update

**Example:**
```javascript
// ❌ CANNOT: Automatic timestamps
await db.model('users').insert({
    name: 'Alice'
    // No automatic created_at/updated_at
});

// ❌ CANNOT: Automatic updated_at on update
await db.model('users').updateById(1, {
    name: 'Alice Updated'
    // No automatic updated_at
});
```

**Workaround:**
```javascript
// ✅ Use schema helpers (recommended)
await db.schema.create.table('users', {
    id: 'SERIAL PRIMARY KEY',
    name: 'VARCHAR(255)',
    ...timestamp('created_at', 'updated_at') // From src/column/timestamp.ts
});

// ✅ Or manual timestamps
const user = await db.model('users').insert({
    name: 'Alice',
    created_at: new Date(),
    updated_at: new Date()
});

// ✅ Create a helper for updates
async function updateWithTimestamp(model, id, data) {
    return model.updateById(id, {
        ...data,
        updated_at: new Date()
    });
}
```

**Impact:**
- **Extra boilerplate** for every model
- **Easy to forget** timestamps
- **Inconsistent** timestamp handling across models
- **No automatic updates** on `update()` operations
- **Harder to maintain** consistency

---

### 5. No Data Validation

**What It Means:**
The Model layer **does not validate** data before sending it to PostgreSQL. It relies on:
- PostgreSQL's own validation (NOT NULL, CHECK constraints)
- Your application code to validate

**Example:**
```javascript
// ❌ CANNOT: Built-in validation
await db.model('users').insert({
    email: 'not-an-email', // ❌ No validation, will cause PostgreSQL error
    age: -5,              // ❌ No validation, will cause CHECK constraint error
    name: null            // ❌ No validation, will cause NOT NULL error
});
```

**Workaround:**
```javascript
// ✅ Use a validation library (Zod recommended)
import { z } from 'zod';

const UserSchema = z.object({
    email: z.string().email(),
    age: z.number().min(0).max(150),
    name: z.string().min(1).max(255),
});

class UserService {
    async create(data: any) {
        // Validate and get typed result
        const validated = UserSchema.parse(data);
        return db.model('users').insert(validated);
    }
    
    async update(id: number, data: any) {
        // Partial validation for updates
        const validated = UserSchema.partial().parse(data);
        return db.model('users').updateById(id, validated);
    }
}

// Usage with helpful errors
try {
    await userService.create({ email: 'invalid' });
} catch (error) {
    // Error: "email must be a valid email address"
    console.error(error);
}
```

**Impact:**
- **No safety net** for data quality
- **Database errors** instead of application errors (less user-friendly)
- **Poor error messages** from PostgreSQL
- **Security risks** from malformed data
- **Harder to debug** validation issues

---

### 6. No Hooks (Before/After Save)

**What It Means:**
The Model layer **does not support** lifecycle hooks. You cannot:
- Run code **before** insert/update/delete
- Run code **after** insert/update/delete
- Validate data before save
- Modify data before save
- Trigger side effects (e.g., send emails, update caches)

**Example:**
```javascript
// ❌ CANNOT: Hooks
db.model('users', {
    beforeSave: (data) => {
        data.updated_at = new Date(); // ❌ Not supported
    },
    afterSave: (data) => {
        sendWelcomeEmail(data); // ❌ Not supported
    }
});
```

**Workaround:**
```javascript
// ✅ Create a service layer (recommended pattern)
class UserService {
    async create(data: any) {
        // Pre-save logic
        const now = new Date();
        data.created_at = now;
        data.updated_at = now;
        
        // Validate
        this.validate(data);
        
        // Save
        const user = await db.model('users').insert(data);
        
        // Post-save logic
        await this.sendWelcomeEmail(user);
        await this.updateCache(user);
        
        return user;
    }
    
    async update(id: number, data: any) {
        // Pre-update logic
        data.updated_at = new Date();
        
        // Validate
        this.validatePartial(data);
        
        // Update
        const user = await db.model('users').updateById(id, data);
        
        // Post-update logic
        await this.updateCache(user);
        
        return user;
    }
    
    async delete(id: number) {
        // Pre-delete logic
        await this.logDeletion(id);
        
        // Delete
        const user = await db.model('users').deleteById(id);
        
        // Post-delete logic
        await this.clearCache(id);
        
        return user;
    }
    
    private validate(data: any) {
        // Validation logic
        if (!data.email) throw new Error('Email is required');
    }
    
    private async sendWelcomeEmail(user: any) {
        // Send email logic
    }
    
    private async updateCache(user: any) {
        // Cache update logic
    }
}
```

**Impact:**
- **Boilerplate code** for every model
- **No consistency** in lifecycle handling
- **Harder to maintain** business logic
- **No separation of concerns** (model vs. business logic mixed)
- **Manual error handling** for each hook

---

## 🎯 When Does It Become Limiting?

The Model layer becomes **inadequate** when your application has:

### 1. >5 Tables
As your data model grows, you need:
- ✅ Relationships between tables
- ✅ Joins across tables
- ✅ Transactions spanning multiple tables
- ✅ Complex queries with multiple conditions

### 2. Complex Relationships
When you have:
- ✅ One-to-many relationships (User → Orders)
- ✅ Many-to-many relationships (User ↔ Groups)
- ✅ Polymorphic relationships
- ✅ Nested relationships (User → Orders → OrderItems)

### 3. Business Logic Beyond CRUD
When you need:
- ✅ Data validation
- ✅ Lifecycle hooks (before/after save)
- ✅ Business rules enforcement
- ✅ Complex workflows
- ✅ Audit trails
- ✅ Soft deletes

### 4. Data Integrity Requirements
When you need:
- ✅ Automatic timestamps
- ✅ Default values
- ✅ Runtime type safety
- ✅ Runtime validation
- ✅ Transaction support

### 5. Complex Query Requirements
When you need:
- ✅ Joins
- ✅ Subqueries
- ✅ Complex WHERE conditions
- ✅ Aggregations
- ✅ CTEs (WITH clauses)
- ✅ Window functions

---

## 📊 Model Layer vs. Full ORM Comparison

| Feature | Model Layer | QueryBuilder | Full ORM (Sequelize, TypeORM, Prisma) |
| :--- | :---: | :---: | :---: |
| **Single Table CRUD** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Relationships** | ❌ No | ❌ No | ✅ Yes |
| **Joins** | ❌ No | ✅ Yes | ✅ Yes |
| **Eager Loading** | ❌ No | ⚠️ Manual | ✅ Yes |
| **Soft Deletes** | ❌ No | ⚠️ Manual | ✅ Yes |
| **Timestamps** | ❌ No | ⚠️ Manual | ✅ Yes |
| **Validation** | ❌ No | ❌ No | ✅ Yes |
| **Hooks** | ❌ No | ❌ No | ✅ Yes |
| **Transactions** | ⚠️ Manual | ✅ Automatic | ✅ Automatic |
| **Migrations** | ✅ Yes (separate) | ❌ No | ✅ Yes (integrated) |
| **Query Builder** | ❌ No | ✅ Yes | ✅ Yes |
| **Raw SQL** | ❌ No | ✅ Yes | ✅ Yes |
| **TypeScript Types** | ✅ Basic | ✅ Advanced | ✅ Advanced |
| **Runtime Validation** | ❌ No | ❌ No | ✅ Yes |
| **Performance** | ✅ Fast | ✅ Fast | ⚠️ Slower |
| **Complexity** | ✅ Low | ⚠️ Medium | ❌ High |
| **Learning Curve** | ✅ Low | ⚠️ Medium | ❌ Steep |
| **Boilerplate** | ✅ Minimal | ⚠️ Some | ✅ Minimal |

---

## 💡 Recommendations

### ✅ Use Model Layer For:

1. **Simple CRUD Applications**
   - Admin panels with isolated tables
   - Internal tools with basic data needs
   - Simple APIs with 1-3 tables

2. **Rapid Prototyping**
   - Quick development where speed matters
   - Proof-of-concept applications
   - Throwaway code

3. **Single-Table Operations**
   - Operations that don't need relationships
   - Simple lookups by ID
   - Basic filtering with equality conditions

4. **Trusted Environments**
   - Internal applications with controlled data
   - Applications where data is pre-validated
   - Simple data structures

### ⚠️ Consider Alternatives For:

1. **Applications with 3-5 Tables**
   - Use **Model Layer + QueryBuilder** combination
   - Use Model for simple CRUD
   - Use QueryBuilder for complex queries

2. **Applications Requiring Validation**
   - Use **Model Layer + Validation Library** (Zod, Joi)
   - Add validation layer on top of Model

3. **Applications with Some Relationships**
   - Use **Model Layer + Manual Joins**
   - Handle relationships manually in service layer

### ❌ Use Full ORM For:

1. **Complex Applications** (5+ tables with relationships)
   - E-commerce platforms
   - Social networks
   - Content management systems

2. **Production Applications**
   - Where data integrity is critical
   - Where validation is essential
   - Where maintainability matters

3. **Applications Requiring:**
   - Relationships (hasMany, belongsTo)
   - Joins
   - Hooks
   - Soft deletes
   - Automatic timestamps
   - Runtime validation
   - Automatic transactions

---

## 🔧 Workarounds for Current Limitations

### 1. For Relationships

**Problem:** Cannot define or query relationships.

**Solution:** Manual relationship handling in service layer.

```javascript
// Service layer with manual relationships
class UserService {
    constructor(db) {
        this.db = db;
        this.users = db.model('users');
        this.orders = db.model('orders');
    }
    
    async getUserWithOrders(userId) {
        const user = await this.users.findById(userId);
        if (!user) return null;
        
        const orders = await this.orders.find({ user_id: userId });
        return { ...user, orders };
    }
    
    async getUsersWithOrderCounts() {
        const users = await this.users.find({});
        const usersWithCounts = await Promise.all(
            users.map(async user => {
                const count = await this.orders.count({ user_id: user.id });
                return { ...user, order_count: count };
            })
        );
        return usersWithCounts;
    }
}
```

### 2. For Complex Conditions

**Problem:** Cannot express complex WHERE conditions.

**Solution:** Use QueryBuilder for complex queries.

```javascript
// Use QueryBuilder for complex conditions
class UserService {
    async findActiveUsersInAgeRange(minAge, maxAge) {
        return db.from('users')
            .where('status = $1 AND age BETWEEN $2 AND $3')
            .execute(['active', minAge, maxAge]);
    }
    
    async findUsersByRoleOrStatus(roles, status) {
        return db.from('users')
            .where('role IN ($1) OR status = $2')
            .execute([roles, status]);
    }
}
```

### 3. For Transactions

**Problem:** Manual client passing required.

**Solution:** Create transaction helper or use QueryBuilder.

```javascript
// Transaction helper
async function withModelTransaction(db, callback) {
    const client = await db.getClient();
    try {
        const tx = { client };
        await callback(tx);
        await client.query('COMMIT');
    } catch (error) {
        await client.query('ROLLBACK');
        throw error;
    } finally {
        db.releaseClient(client);
    }
}

// Usage
await withModelTransaction(db, async (tx) => {
    await db.model('users').insert({ name: 'Alice' }, tx);
    await db.model('profiles').insert({ user_id: 1, bio: '...' }, tx);
});

// Or use QueryBuilder directly
await db.transaction(async (client) => {
    await client.query('INSERT INTO users (name) VALUES ($1)', ['Alice']);
    await client.query('INSERT INTO profiles (user_id) VALUES ($1)', [1]);
});
```

### 4. For Validation

**Problem:** No built-in validation.

**Solution:** Use validation library (Zod recommended).

```javascript
import { z } from 'zod';

// Define schemas
const UserCreateSchema = z.object({
    name: z.string().min(1).max(255),
    email: z.string().email(),
    age: z.number().min(0).max(150).optional(),
});

const UserUpdateSchema = UserCreateSchema.partial();

// Service with validation
class UserService {
    async create(data) {
        const validated = UserCreateSchema.parse(data);
        return this.users.insert(validated);
    }
    
    async update(id, data) {
        const validated = UserUpdateSchema.parse(data);
        return this.users.updateById(id, validated);
    }
}
```

### 5. For Soft Deletes

**Problem:** No built-in soft delete support.

**Solution:** Manual implementation with helper methods.

```javascript
// Base model with soft delete support
class SoftDeleteModel {
    constructor(model, deletedField = 'deleted', deletedAtField = 'deleted_at') {
        this.model = model;
        this.deletedField = deletedField;
        this.deletedAtField = deletedAtField;
    }
    
    async find(where = {}) {
        return this.model.find({ ...where, [this.deletedField]: false });
    }
    
    async findOne(where = {}) {
        return this.model.findOne({ ...where, [this.deletedField]: false });
    }
    
    async findById(id) {
        return this.model.findById(id, { [this.deletedField]: false });
    }
    
    async insert(data) {
        return this.model.insert(data);
    }
    
    async updateById(id, data) {
        return this.model.updateById(id, {
            ...data,
            [this.deletedAtField]: new Date()
        });
    }
    
    async softDelete(id) {
        return this.updateById(id, {
            [this.deletedField]: true,
            [this.deletedAtField]: new Date()
        });
    }
    
    async restore(id) {
        return this.updateById(id, {
            [this.deletedField]: false,
            [this.deletedAtField]: null
        });
    }
    
    async findWithDeleted(where = {}) {
        return this.model.find(where);
    }
}

// Usage
const users = new SoftDeleteModel(db.model('users'));
const activeUsers = await users.find();
const deletedUsers = await users.findWithDeleted({ deleted: true });
await users.softDelete(1);
await users.restore(1);
```

### 6. For Timestamps

**Problem:** No automatic timestamps.

**Solution:** Use schema helpers or middleware.

```javascript
// Use schema helpers when creating tables
await db.schema.create.table('users', {
    id: 'SERIAL PRIMARY KEY',
    name: 'VARCHAR(255)',
    ...timestamp('created_at', 'updated_at') // From src/column/timestamp.ts
});

// Or create a timestamp middleware
class TimestampModel {
    constructor(model) {
        this.model = model;
    }
    
    async insert(data) {
        const now = new Date();
        return this.model.insert({
            ...data,
            created_at: now,
            updated_at: now
        });
    }
    
    async updateById(id, data) {
        return this.model.updateById(id, {
            ...data,
            updated_at: new Date()
        });
    }
    
    // Delegate other methods
    find(where) { return this.model.find(where); }
    findById(id) { return this.model.findById(id); }
    // ... etc
}

// Usage
const users = new TimestampModel(db.model('users'));
await users.insert({ name: 'Alice' }); // Automatically sets timestamps
```

### 7. For Hooks

**Problem:** No lifecycle hooks.

**Solution:** Create service layer with hooks.

```javascript
// Base service with hooks
class ModelService {
    constructor(model) {
        this.model = model;
        this.hooks = {
            beforeCreate: [],
            afterCreate: [],
            beforeUpdate: [],
            afterUpdate: [],
            beforeDelete: [],
            afterDelete: []
        };
    }
    
    on(event, callback) {
        this.hooks[event].push(callback);
    }
    
    async runHooks(event, data, context = {}) {
        for (const hook of this.hooks[event]) {
            await hook(data, context);
        }
    }
    
    async create(data) {
        await this.runHooks('beforeCreate', data);
        const result = await this.model.insert(data);
        await this.runHooks('afterCreate', result);
        return result;
    }
    
    async updateById(id, data) {
        await this.runHooks('beforeUpdate', { id, ...data });
        const result = await this.model.updateById(id, data);
        await this.runHooks('afterUpdate', result);
        return result;
    }
    
    async deleteById(id) {
        await this.runHooks('beforeDelete', { id });
        const result = await this.model.deleteById(id);
        await this.runHooks('afterDelete', result);
        return result;
    }
}

// Usage
const userService = new ModelService(db.model('users'));

// Add hooks
userService.on('beforeCreate', (data) => {
    data.created_at = new Date();
    data.updated_at = new Date();
});

userService.on('afterCreate', async (user) => {
    await sendWelcomeEmail(user);
});

userService.on('beforeUpdate', (data) => {
    data.updated_at = new Date();
});

// Use service
await userService.create({ name: 'Alice' });
```

---

## 🎯 Final Verdict

The **Model layer in `@oneunit/database` is a lightweight CRUD abstraction**, **not a full ORM**.

| Use Case | Recommendation | Suitability Score |
| :--- | :--- | :---: |
| Simple CRUD app (1-2 tables) | ✅ Use Model layer | 10/10 |
| Admin panel (simple internal tool) | ✅ Use Model layer | 9/10 |
| API with 1-3 tables | ✅ Use Model layer | 8/10 |
| API with 3-5 tables | ⚠️ Use Model + QueryBuilder | 6/10 |
| API with 5+ tables | ❌ Use full ORM | 3/10 |
| Complex business logic | ❌ Use full ORM or QueryBuilder | 2/10 |
| E-commerce application | ❌ Use full ORM | 1/10 |
| Social network | ❌ Use full ORM | 1/10 |
| Financial application | ❌ Use QueryBuilder + transactions | 2/10 |

### Bottom Line:

The Model layer is **excellent for simple applications** but **lacks the features** needed for complex, production-grade applications with relationships, validation, and business logic.

**For simple use cases:** The Model layer provides a clean, lightweight abstraction over raw SQL.

**For complex use cases:** Use QueryBuilder directly or consider a full ORM like Prisma, TypeORM, or Sequelize.

**For production applications:** Consider whether the workarounds for Model layer limitations are acceptable, or if a full ORM would be more maintainable and productive.

---

## 📚 Additional Resources

- [README.md](README.md) - Usage documentation
- [ARCHITECTURE.md](ARCHITECTURE.md) - Architecture overview
- [QUERY_BUILDER_IMPROVEMENTS.md](QUERY_BUILDER_IMPROVEMENTS.md) - QueryBuilder features
- [IMPLEMENTATION_NOTES.md](IMPLEMENTATION_NOTES.md) - Implementation details
- [CHANGELOG.md](CHANGELOG.md) - Version history

---

## 📝 Changelog

| Version | Date | Changes |
| :--- | :--- | :--- |
| 1.0 | 2026-11-01 | Initial comprehensive documentation of Model layer limitations |
