// Copyright 2026 Tauric Research
// SPDX-License-Identifier: Apache-2.0
/**
 * HTTP client for the local AkShare Python bridge. When no URL is configured, lazily launches
 * `scripts/akshare_server.py` on loopback with an idle timeout and cleans it up on process exit.
 */
import { type ChildProcess, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { message } from "../../contracts/index.js";
import { ANSWER_BYTES, REQUEST_MS } from "../../contracts/limits.js";
import { bounded } from "../bounded.js";
import { DataError, type DataErrorKind } from "../errors.js";

const SOURCE = "AkShare";
const DEFAULT_PORT = 18080;
const STARTUP_TIMEOUT_MS = 15_000;
const KNOWN_KINDS = new Set<DataErrorKind>([
  "unsupported",
  "unavailable",
  "rate_limited",
  "quota",
  "rejected",
  "not_configured",
  "no_data",
  "invalid",
]);

type ClientOptions = {
  baseURL?: string;
  fetch?: typeof globalThis.fetch;
  pythonBin?: string;
  scriptPath?: string;
};

export class AkShareClient {
  private readonly fetchImpl: typeof globalThis.fetch;
  private readonly configuredUrl: string | undefined;
  private readonly pythonBin: string;
  private readonly scriptPath: string;
  private activeUrl: string | undefined;
  private starting: Promise<string> | undefined;
  private child: ChildProcess | undefined;

  constructor(options: ClientOptions = {}) {
    this.fetchImpl = options.fetch ?? fetch;
    this.configuredUrl = options.baseURL?.replace(/\/+$/, "");
    this.pythonBin = options.pythonBin ?? "python";
    this.scriptPath = options.scriptPath ?? defaultScriptPath();
  }

  async get<T>(path: string, params: Record<string, string | undefined>, signal?: AbortSignal): Promise<T> {
    const base = await this.resolveBaseUrl(signal);
    const search = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== "") search.set(key, value);
    }
    const query = search.toString();
    const url = `${base}${path}${query ? `?${query}` : ""}`;
    let response: Response;
    try {
      response = await this.fetchImpl(url, {
        signal: AbortSignal.any([AbortSignal.timeout(REQUEST_MS.akshare), ...(signal ? [signal] : [])]),
      });
    } catch (error) {
      if (signal?.aborted) throw error;
      this.activeUrl = undefined;
      throw new DataError("unavailable", `AkShare bridge could not be reached (${String(error)}).`, {
        source: SOURCE,
      });
    }

    const body = await bounded(
      response,
      ANSWER_BYTES,
      () =>
        new DataError("unavailable", message("data.answerTooLong", { source: SOURCE, bytes: ANSWER_BYTES }), {
          source: SOURCE,
        }),
    )
      .json()
      .catch((error: unknown) => {
        if (error instanceof DataError) throw error;
        return undefined;
      });

    if (!response.ok) {
      const payload = (body ?? {}) as { error?: string; message?: string };
      const kind: DataErrorKind = KNOWN_KINDS.has(payload.error as DataErrorKind)
        ? (payload.error as DataErrorKind)
        : response.status === 429
          ? "rate_limited"
          : response.status === 404
            ? "no_data"
            : "unavailable";
      throw new DataError(kind, payload.message ?? `AkShare answered HTTP ${response.status}.`, {
        source: SOURCE,
        status: response.status,
      });
    }
    if (body === undefined) {
      throw new DataError("unavailable", "AkShare returned an unreadable response.", { source: SOURCE });
    }
    return body as T;
  }

  private async resolveBaseUrl(signal?: AbortSignal): Promise<string> {
    if (this.configuredUrl) return this.configuredUrl;
    if (this.activeUrl) return this.activeUrl;
    if (this.fetchImpl !== fetch) return `http://127.0.0.1:${DEFAULT_PORT}`;
    if (!this.starting) {
      this.starting = this.ensureLocalBridge(signal).finally(() => {
        this.starting = undefined;
      });
    }
    return this.starting;
  }

  private async ensureLocalBridge(signal?: AbortSignal): Promise<string> {
    const defaultUrl = `http://127.0.0.1:${DEFAULT_PORT}`;
    if (await this.isHealthy(defaultUrl)) {
      this.activeUrl = defaultUrl;
      return defaultUrl;
    }
    if (!existsSync(this.scriptPath)) {
      throw new DataError(
        "not_configured",
        `AkShare bridge script not found at ${this.scriptPath}; set TRADINGAGENTS_AKSHARE_URL to a running AkShare bridge.`,
        { source: SOURCE },
      );
    }
    const url = await this.spawnBridge(signal);
    this.activeUrl = url;
    return url;
  }

  private async isHealthy(baseUrl: string): Promise<boolean> {
    try {
      const res = await this.fetchImpl(`${baseUrl}/health`, { signal: AbortSignal.timeout(800) });
      return res.ok;
    } catch {
      return false;
    }
  }

  private spawnBridge(signal?: AbortSignal): Promise<string> {
    return new Promise((resolve, reject) => {
      const child = spawn(
        this.pythonBin,
        [this.scriptPath, "--host", "127.0.0.1", "--port", "0", "--idle-seconds", "600"],
        { stdio: ["ignore", "pipe", "pipe"], windowsHide: true },
      );
      this.child = child;
      let stdout = "";
      let stderr = "";
      let settled = false;

      const cleanup = () => {
        if (this.child === child) this.child = undefined;
        try {
          child.kill();
        } catch {
          // already exited
        }
      };
      const onExit = () => cleanup();
      process.once("beforeExit", onExit);
      process.once("exit", onExit);

      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        process.off("beforeExit", onExit);
        process.off("exit", onExit);
        cleanup();
        reject(new DataError("unavailable", "AkShare bridge timed out starting up.", { source: SOURCE }));
      }, STARTUP_TIMEOUT_MS);

      const onAbort = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        process.off("beforeExit", onExit);
        process.off("exit", onExit);
        cleanup();
        reject(signal?.reason ?? new Error("Aborted"));
      };
      signal?.addEventListener("abort", onAbort, { once: true });

      child.stdout?.on("data", (chunk: Buffer) => {
        stdout += chunk.toString("utf8");
        const match = /LISTENING (http:\/\/[^\s]+)/.exec(stdout);
        if (match && !settled) {
          settled = true;
          clearTimeout(timer);
          signal?.removeEventListener("abort", onAbort);
          // Unref child and stdio pipes so Node's event loop does not wait for the idle Python process.
          child.stdout?.removeAllListeners("data");
          child.stderr?.removeAllListeners("data");
          child.stdout?.resume();
          child.stderr?.resume();
          (child.stdout as unknown as { unref?: () => void } | null)?.unref?.();
          (child.stderr as unknown as { unref?: () => void } | null)?.unref?.();
          child.unref();
          resolve(match[1] as string);
        }
      });

      child.stderr?.on("data", (chunk: Buffer) => {
        stderr += chunk.toString("utf8");
      });

      child.once("error", (error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        process.off("beforeExit", onExit);
        process.off("exit", onExit);
        signal?.removeEventListener("abort", onAbort);
        reject(
          new DataError(
            "unavailable",
            `Could not start Python AkShare bridge (${error.message}). Ensure Python and akshare are installed.`,
            { source: SOURCE },
          ),
        );
      });

      child.once("exit", (code) => {
        process.off("beforeExit", onExit);
        process.off("exit", onExit);
        if (this.child === child) {
          this.child = undefined;
          this.activeUrl = undefined;
        }
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        const detail = stderr.trim() || `exit code ${code ?? "unknown"}`;
        reject(new DataError("unavailable", `AkShare bridge failed to start (${detail}).`, { source: SOURCE }));
      });
    });
  }
}

function defaultScriptPath(): string {
  const candidates = [
    fileURLToPath(new URL("../../../scripts/akshare_server.py", import.meta.url)),
    `${process.cwd()}/scripts/akshare_server.py`,
  ];
  return candidates.find((path) => existsSync(path)) ?? (candidates[0] as string);
}
