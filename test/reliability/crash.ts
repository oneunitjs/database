import { spawn } from "node:child_process";
import path from "node:path";

const RUNNER = path.resolve(process.cwd(), "test/reliability/crash-runner.mjs");
const TSX = path.resolve(process.cwd(), "node_modules/.bin/tsx");

export { spawn };
export { RUNNER };

export interface CrashRunnerResult {
    exitCode: number | null;
    killed: boolean;
    stderr: string;
}

/**
 * Spawn the crash runner in a separate Node process, wait `delayMs`, then
 * SIGKILL it. Waits for the process to fully exit before resolving.
 */
export async function spawnAndKill(
    args: Record<string, string>,
    delayMs: number,
): Promise<CrashRunnerResult> {
    const argv = Object.entries(args).map(([k, v]) => `${k}=${v}`);
    const child = spawn(process.execPath, [RUNNER, ...argv], {
        stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr.on("data", (d) => {
        stderr += d.toString();
    });

    const exitPromise = new Promise<{ code: number | null; killed: boolean }>((resolve) => {
        child.on("exit", (code) => resolve({ code, killed: false }));
    });

    await new Promise((resolve) => setTimeout(resolve, delayMs));

    let killed = false;
    if (!child.exitCode && !child.killed) {
        try {
            child.kill("SIGKILL");
            killed = true;
        } catch {
            // already dead
        }
    }

    const { code } = await exitPromise;
    await new Promise((resolve) => setTimeout(resolve, 500)); // let PG notice backend death

    return { exitCode: code, killed, stderr };
}

/**
 * Spawn the crash runner and let it run to completion (no kill).
 */
export async function spawnAndWait(args: Record<string, string>): Promise<CrashRunnerResult> {
    const argv = Object.entries(args).map(([k, v]) => `${k}=${v}`);
    const child = spawn(process.execPath, [RUNNER, ...argv], {
        stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr.on("data", (d) => {
        stderr += d.toString();
    });
    const code = await new Promise<number | null>((resolve) => {
        child.on("exit", resolve);
    });
    await new Promise((resolve) => setTimeout(resolve, 500));
    return { exitCode: code, killed: false, stderr };
}

/**
 * Spawn a tsx-based runner script, capture its stdout, and await exit.
 * Returns the parsed first JSON line on stdout (or null) plus exit code.
 */
export async function spawnTsxAndCapture(script: string, args: string[], timeoutMs = 60_000, env?: Record<string, string>): Promise<{
    code: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
    parsed: any;
}> {
    const argv = args.map((v) => String(v));
    const child = spawn(process.execPath, [TSX, script, ...argv], {
        stdio: ["ignore", "pipe", "pipe"],
        env: env ?? process.env,
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));

    let timedOut = false;
    const timer = setTimeout(() => {
        timedOut = true;
        try {
            child.kill("SIGKILL");
        } catch {
            // ignore
        }
    }, timeoutMs);

    const code = await new Promise<number | null>((resolve) => child.on("exit", resolve));
    clearTimeout(timer);

    let parsed: any = null;
    const firstLine = stdout.trim().split("\n")[0];
    try {
        parsed = firstLine ? JSON.parse(firstLine) : null;
    } catch {
        parsed = null;
    }

    return { code, stdout, stderr, timedOut, parsed };
}
