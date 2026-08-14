import { describe, expect, test } from "bun:test";
import { lstat, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

import {
	type CommandResult,
	type CommandRunner,
	CommandRunnerError,
	HerdrServerError,
	type PaneProcessInfo,
} from "../src/herdr.ts";
import { runLegacyLockMigration } from "../src/migrate-lock-command.ts";
import { ProtocolStoreError, RunStore } from "../src/store.ts";
import {
	makeManifest,
	makeTempDirectory,
	removeTempDirectory,
} from "./helpers.ts";

const MANIFEST_MUTEX_DIRECTORY = ".manifest-lock.sqlite";
const ARCHIVE_NAME = ".manifest-lock.sqlite.v0.1-file-20260814T142317Z";
const FIXED_NOW = new Date("2026-08-14T14:23:17.000Z");

function result(
	stdout: string,
	exitCode = 0,
	extra: Partial<CommandResult> = {},
): CommandResult {
	return { stdout, stderr: "", exitCode, ...extra };
}

function runnerFor(script: {
	pgrep?: CommandResult | Error;
	lsof?: CommandResult | Error;
	lsofCalls?: string[][];
}): CommandRunner {
	return async (command, args) => {
		if (command === "lsof") script.lsofCalls?.push([...args]);
		const next = command === "pgrep" ? script.pgrep : script.lsof;
		if (next === undefined) {
			throw new CommandRunnerError(`${command} unavailable`);
		}
		if (next instanceof Error) throw next;
		if (next.exitCode !== 0) {
			throw new CommandRunnerError(`${command} failed`, next);
		}
		return next;
	};
}

async function writeRunningManifest(root: string): Promise<void> {
	const store = new RunStore(root);
	const starting = makeManifest({
		runId: "20260814T000000000Z-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		repoPath: "/tmp/omp-fleet-monitored-repo",
	});
	await store.createRun(starting);
	await store.writeManifest({
		...starting,
		lifecycle: "running",
		supervisorTabId: "supervisor-tab",
		supervisorPaneId: "supervisor-pane",
		supervisorCommand: "bun sidecar.ts --run-id test",
	});
}

describe("runLegacyLockMigration", () => {
	test("refuses a leftover file when pgrep reports any sidecar.ts process", async () => {
		const cases = [
			{
				stdout: "4411 bun /x/@ericjuta/omp-fleet/src/sidecar.ts --run-id abc\n",
				pid: "4411",
			},
			{
				stdout:
					"5522 bun /tmp/omp-fleet.feature-x/src/sidecar.ts --run-id def\n",
				pid: "5522",
			},
		];
		for (const probe of cases) {
			const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
			try {
				await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
					mode: 0o600,
				});
				await expect(
					runLegacyLockMigration(root, {
						now: FIXED_NOW,
						runner: runnerFor({
							pgrep: result(probe.stdout),
							lsof: result("", 1),
						}),
						herdr: {
							inspectPane: async () => ({ kind: "empty" }),
						},
					}),
				).rejects.toThrow(new RegExp(`sidecarPids=${probe.pid}`));
				expect(
					(await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile(),
				).toBe(true);
			} finally {
				await removeTempDirectory(root);
			}
		}
	});

	test("fails closed when pgrep sidecar output is nonempty but has no PID", async () => {
		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
				mode: 0o600,
			});
			await expect(
				runLegacyLockMigration(root, {
					now: FIXED_NOW,
					runner: runnerFor({
						pgrep: result("bun /tmp/omp-fleet.feature-x/src/sidecar.ts\n"),
						lsof: result("", 1),
					}),
					herdr: {
						inspectPane: async () => ({ kind: "empty" }),
					},
				}),
			).rejects.toThrow(/could not parse sidecar PIDs/);
			expect((await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile()).toBe(
				true,
			);
		} finally {
			await removeTempDirectory(root);
		}
	});

	test("fails closed when probe status and output contradict each other", async () => {
		const cases: Array<{
			pgrep: CommandResult;
			lsof: CommandResult;
			error: RegExp;
		}> = [
			{
				pgrep: result(""),
				lsof: result("", 1),
				error: /sidecar PIDs query returned contradictory output/,
			},
			{
				pgrep: result(
					"4411 bun /tmp/omp-fleet/src/sidecar.ts --run-id abc\n",
					1,
				),
				lsof: result("", 1),
				error: /sidecar PIDs query returned contradictory output/,
			},
			{
				pgrep: result("", 1),
				lsof: result("4411\n", 1),
				error: /lock holder PIDs query returned contradictory output/,
			},
			{
				pgrep: result("", 1),
				lsof: result(""),
				error: /lock holder PIDs query returned contradictory output/,
			},
			{
				pgrep: result("", 1),
				lsof: result("12 extra\n"),
				error: /could not parse lock holder PIDs/,
			},
		];
		for (const probe of cases) {
			const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
			try {
				await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
					mode: 0o600,
				});
				await expect(
					runLegacyLockMigration(root, {
						now: FIXED_NOW,
						runner: runnerFor(probe),
						herdr: {
							inspectPane: async () => ({ kind: "empty" }),
						},
					}),
				).rejects.toThrow(probe.error);
				expect(
					(await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile(),
				).toBe(true);
			} finally {
				await removeTempDirectory(root);
			}
		}
	});

	test("queries lock holders with lsof -t -- and refuses exit codes above 1", async () => {
		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
				mode: 0o600,
			});
			const lsofCalls: string[][] = [];
			await expect(
				runLegacyLockMigration(root, {
					now: FIXED_NOW,
					runner: runnerFor({
						pgrep: result("", 1),
						lsof: result("lsof: status error\n", 2),
						lsofCalls,
					}),
					herdr: {
						inspectPane: async () => {
							throw new HerdrServerError("pane_not_found", "missing");
						},
					},
				}),
			).rejects.toThrow(/could not query lock holders/);
			expect(lsofCalls).toEqual([
				["-t", "--", join(root, MANIFEST_MUTEX_DIRECTORY)],
			]);
			expect((await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile()).toBe(
				true,
			);
		} finally {
			await removeTempDirectory(root);
		}
	});

	test("fails closed when supervisor inspect is not pane_not_found", async () => {
		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			await writeRunningManifest(root);
			await rm(join(root, MANIFEST_MUTEX_DIRECTORY), { recursive: true });
			await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
				mode: 0o600,
			});
			await expect(
				runLegacyLockMigration(root, {
					now: FIXED_NOW,
					runner: runnerFor({
						pgrep: result("", 1),
						lsof: result("", 1),
					}),
					herdr: {
						inspectPane: async () => {
							throw new Error("inspect timed out");
						},
					},
				}),
			).rejects.toThrow(/could not inspect a recorded supervisor pane/);
			expect((await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile()).toBe(
				true,
			);
		} finally {
			await removeTempDirectory(root);
		}
	});

	test("fails closed when a generic supervisor error mentions pane_not_found", async () => {
		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			await writeRunningManifest(root);
			await rm(join(root, MANIFEST_MUTEX_DIRECTORY), { recursive: true });
			await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
				mode: 0o600,
			});
			await expect(
				runLegacyLockMigration(root, {
					now: FIXED_NOW,
					runner: runnerFor({
						pgrep: result("", 1),
						lsof: result("", 1),
					}),
					herdr: {
						inspectPane: async () => {
							throw new Error("pane_not_found");
						},
					},
				}),
			).rejects.toThrow(/could not inspect a recorded supervisor pane/);
			expect((await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile()).toBe(
				true,
			);
		} finally {
			await removeTempDirectory(root);
		}
	});

	test("fails closed when sidecar PID query cannot run", async () => {
		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
				mode: 0o600,
			});
			await expect(
				runLegacyLockMigration(root, {
					now: FIXED_NOW,
					runner: runnerFor({ lsof: result("", 1) }),
				}),
			).rejects.toBeInstanceOf(ProtocolStoreError);
		} finally {
			await removeTempDirectory(root);
		}
	});

	test("fails closed when a probe returns stderr, truncation, or a timeout", async () => {
		const cases: Array<{ pgrep?: CommandResult; lsof?: CommandResult }> = [
			{
				pgrep: result("", 0, { stderr: "pgrep: warning\n" }),
				lsof: result("", 1),
			},
			{ pgrep: result("", 0, { stdoutTruncated: true }), lsof: result("", 1) },
			{ pgrep: result("", 0, { timedOut: true }), lsof: result("", 1) },
			{
				pgrep: result("", 1),
				lsof: result("", 1, { stderr: "lsof: warning\n" }),
			},
			{
				pgrep: result("", 1),
				lsof: result("12\n", 0, { stderrTruncated: true }),
			},
		];
		for (const probe of cases) {
			const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
			try {
				await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
					mode: 0o600,
				});
				await expect(
					runLegacyLockMigration(root, {
						now: FIXED_NOW,
						runner: runnerFor(probe),
						herdr: {
							inspectPane: async () => {
								throw new HerdrServerError("pane_not_found", "missing");
							},
						},
					}),
				).rejects.toBeInstanceOf(ProtocolStoreError);
				expect(
					(await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile(),
				).toBe(true);
			} finally {
				await removeTempDirectory(root);
			}
		}
	});

	test("fails closed when run inventory contains an invalid manifest", async () => {
		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
				mode: 0o600,
			});
			const poison = join(
				root,
				"20260814T000000000Z-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			);
			await mkdir(poison, { mode: 0o700 });
			await writeFile(join(poison, "manifest.json"), "{not-json\n", {
				mode: 0o600,
			});
			await expect(
				runLegacyLockMigration(root, {
					now: FIXED_NOW,
					runner: runnerFor({
						pgrep: result("", 1),
						lsof: result("", 1),
					}),
					herdr: {
						inspectPane: async () => {
							throw new HerdrServerError("pane_not_found", "missing");
						},
					},
				}),
			).rejects.toThrow(/invalid manifest/);
			expect((await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile()).toBe(
				true,
			);
		} finally {
			await removeTempDirectory(root);
		}
	});

	test("does nothing for an existing private mutex directory", async () => {
		const unusedNow = new Date(Number.NaN);
		let runnerCalls = 0;
		let herdrCalls = 0;

		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			const mutexPath = join(root, MANIFEST_MUTEX_DIRECTORY);
			await mkdir(mutexPath, { mode: 0o700 });
			const migration = await runLegacyLockMigration(root, {
				now: unusedNow,
				runner: async () => {
					runnerCalls += 1;
					throw new Error("runner must not be called");
				},
				herdr: {
					inspectPane: async () => {
						herdrCalls += 1;
						throw new Error("herdr must not be called");
					},
				},
			});
			expect(migration).toEqual({
				archivedPath: undefined,
				sidecarPids: [],
				lockHolders: [],
				liveSupervisorPanes: [],
			});
			expect(runnerCalls).toBe(0);
			expect(herdrCalls).toBe(0);
			const mutex = await lstat(mutexPath);
			expect(mutex.isDirectory()).toBe(true);
			expect(mutex.mode & 0o777).toBe(0o700);
		} finally {
			await removeTempDirectory(root);
		}
	});

	test("fails closed when a probe returns whitespace-only stderr", async () => {
		const cases: Array<{ pgrep: CommandResult; lsof: CommandResult }> = [
			{
				pgrep: result("", 1, { stderr: " \t\n" }),
				lsof: result("", 1),
			},
			{
				pgrep: result("", 1),
				lsof: result("", 1, { stderr: " \t\n" }),
			},
		];
		for (const probe of cases) {
			const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
			try {
				await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
					mode: 0o600,
				});
				await expect(
					runLegacyLockMigration(root, {
						now: FIXED_NOW,
						runner: runnerFor(probe),
						herdr: {
							inspectPane: async () => ({ kind: "empty" }),
						},
					}),
				).rejects.toBeInstanceOf(ProtocolStoreError);
				expect(
					(await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile(),
				).toBe(true);
			} finally {
				await removeTempDirectory(root);
			}
		}
	});

	test("archives only after live sidecar, lock, and pane lists are empty", async () => {
		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			await mkdir(root, { recursive: true });
			await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
				mode: 0o600,
			});
			const lsofCalls: string[][] = [];
			const archived = await runLegacyLockMigration(root, {
				now: FIXED_NOW,
				runner: runnerFor({
					pgrep: result("", 1),
					lsof: result("", 1),
					lsofCalls,
				}),
				herdr: {
					inspectPane: async (): Promise<PaneProcessInfo> => {
						throw new HerdrServerError("pane_not_found", "missing");
					},
				},
			});
			expect(lsofCalls).toEqual([
				["-t", "--", join(root, MANIFEST_MUTEX_DIRECTORY)],
			]);
			expect(archived.archivedPath).toBe(join(root, ARCHIVE_NAME));
			expect(archived.sidecarPids).toEqual([]);
			expect(archived.lockHolders).toEqual([]);
			expect(archived.liveSupervisorPanes).toEqual([]);
			expect(
				(await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isDirectory(),
			).toBe(true);
			expect(await readFile(join(root, ARCHIVE_NAME), "utf8")).toBe("legacy\n");
		} finally {
			await removeTempDirectory(root);
		}
	});

	test("refuses when lsof reports a live lock holder PID", async () => {
		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
				mode: 0o600,
			});
			await expect(
				runLegacyLockMigration(root, {
					now: FIXED_NOW,
					runner: runnerFor({
						pgrep: result("", 1),
						lsof: result("7788\n"),
					}),
					herdr: {
						inspectPane: async () => {
							throw new HerdrServerError("pane_not_found", "missing");
						},
					},
				}),
			).rejects.toThrow(
				/refused: sidecarPids=none lockHolders=7788 liveSupervisorPanes=none/,
			);
			expect((await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile()).toBe(
				true,
			);
			expect(await readFile(join(root, MANIFEST_MUTEX_DIRECTORY), "utf8")).toBe(
				"legacy\n",
			);
			await expect(lstat(join(root, ARCHIVE_NAME))).rejects.toThrow();
		} finally {
			await removeTempDirectory(root);
		}
	});

	test("refuses when a recorded supervisor pane inspects successfully", async () => {
		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			await writeRunningManifest(root);
			await rm(join(root, MANIFEST_MUTEX_DIRECTORY), { recursive: true });
			await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
				mode: 0o600,
			});
			await expect(
				runLegacyLockMigration(root, {
					now: FIXED_NOW,
					runner: runnerFor({
						pgrep: result("", 1),
						lsof: result("", 1),
					}),
					herdr: {
						inspectPane: async (): Promise<PaneProcessInfo> => ({
							kind: "empty",
						}),
					},
				}),
			).rejects.toThrow(
				/refused: sidecarPids=none lockHolders=none liveSupervisorPanes=supervisor-pane/,
			);
			expect((await lstat(join(root, MANIFEST_MUTEX_DIRECTORY))).isFile()).toBe(
				true,
			);
			expect(await readFile(join(root, MANIFEST_MUTEX_DIRECTORY), "utf8")).toBe(
				"legacy\n",
			);
			await expect(lstat(join(root, ARCHIVE_NAME))).rejects.toThrow();
		} finally {
			await removeTempDirectory(root);
		}
	});

	test("archives when a running manifest pane is gone with pane_not_found", async () => {
		const root = await makeTempDirectory("omp-fleet-migrate-cmd-");
		try {
			await writeRunningManifest(root);
			await rm(join(root, MANIFEST_MUTEX_DIRECTORY), { recursive: true });
			await writeFile(join(root, MANIFEST_MUTEX_DIRECTORY), "legacy\n", {
				mode: 0o600,
			});
			const migration = await runLegacyLockMigration(root, {
				now: FIXED_NOW,
				runner: runnerFor({
					pgrep: result("", 1),
					lsof: result("", 1),
				}),
				herdr: {
					inspectPane: async (): Promise<PaneProcessInfo> => {
						throw new HerdrServerError("pane_not_found", "missing");
					},
				},
			});
			expect(migration).toEqual({
				archivedPath: join(root, ARCHIVE_NAME),
				sidecarPids: [],
				lockHolders: [],
				liveSupervisorPanes: [],
			});
			const mutex = await lstat(join(root, MANIFEST_MUTEX_DIRECTORY));
			expect(mutex.isDirectory()).toBe(true);
			expect(mutex.mode & 0o777).toBe(0o700);
			expect(await readFile(join(root, ARCHIVE_NAME), "utf8")).toBe("legacy\n");
		} finally {
			await removeTempDirectory(root);
		}
	});
});
