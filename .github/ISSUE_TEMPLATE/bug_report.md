---
name: Bug Report
about: Report a bug or unexpected behavior in @oneunit/database
title: "[BUG] "
labels: ["bug"]
assignees: ""

---

## Describe the bug

A clear and concise description of what the bug is.

## Steps to reproduce

1. ...
2. ...
3. ...

## Expected behavior

What you expected to happen.

## Actual behavior

What actually happened.

## Environment

- **Package version**: `@oneunit/database@x.y.z` (run `npm list @oneunit/database`)
- **Node version**: `node --version` output
- **OS**: e.g. macOS 15, Ubuntu 24.04, Windows 11
- **Database**: PostgreSQL version (e.g. PostgreSQL 15.4)

## Code sample

Provide a minimal, reproducible example:

```javascript
import { createDatabase } from "@oneunit/database";

const db = createDatabase({
    connectionString: "postgresql://localhost:5432/app",
});

async function repro() {
    // Your code here
}

repro().catch(console.error);
```

## Error output

Paste any error message, stack trace, or logs:

```
<paste error or stack trace here>
```

## Additional context

Add any other context about the problem here (e.g. sharding strategy, replication mode, resharding state, custom error handlers).

## Checklist

- [ ] I have read the [README](https://github.com/oneunitjs/database/blob/main/packages/database/README.md)
- [ ] I have checked existing issues for duplicates
- [ ] I am using the latest version of `@oneunit/database`
- [ ] I have provided a minimal reproduction
