import { lstat } from "node:fs/promises";
import { resolve } from "node:path";

import {
	bunCommandRunner,
	type CommandResult,
	type CommandRunner,
	HerdrClient,
	HerdrServerError,
} from "./herdr.ts";
import {
	migrateLegacyManifestLockFile,
	ProtocolStoreError,
	RunStore,
} from "./store.ts";
import { isTerminalLifecycle } from "./types.ts";

export interface LegacyLockMigrationDeps {
	readonly runner?: CommandRunner;
	readonly herdr?: Pick<HerdrClient, "inspectPane">;
	readonly now?: Date;
}

export interface LegacyLockMigrationResult {
	readonly archivedPath: string | undefined;
	readonly sidecarPids: readonly string[];
	readonly lockHolders: readonly string[];
	readonly liveSupervisorPanes: readonly string[];
}

async function runCaptured(
	runner: CommandRunner,
	command: string,
	args: readonly string[],
): Promise<CommandResult | undefined> {
	try {
		return await runner(command, args);
	} catch (error) {
		if (
			error instanceof Error &&
			"result" in error &&
			error.result !== undefined
		) {
			return error.result as CommandResult;
		}
		return undefined;
	}
}

function parsePidOutput(
	text: string,
	label: string,
	requireBarePid: boolean,
): string[] {
	const pids: string[] = [];
	for (const line of text.split("\n")) {
		if (line.length === 0) continue;
		const match = requireBarePid
			? /^(\d+)$/.exec(line)
			: /^(\d+)(?:\s|$)/.exec(line);
		if (match?.[1] === undefined) {
			throw new ProtocolStoreError(
				`legacy lock migration could not parse ${label}`,
			);
		}
		pids.push(match[1]);
	}
	return pids;
}

function requireCleanProbe(
	result: CommandResult | undefined,
	label: string,
): CommandResult {
	if (result === undefined) {
		throw new ProtocolStoreError(
			`legacy lock migration could not query ${label}`,
		);
	}
	if (result.timedOut === true) {
		throw new ProtocolStoreError(
			`legacy lock migration ${label} query timed out`,
		);
	}
	if (result.stdoutTruncated === true || result.stderrTruncated === true) {
		throw new ProtocolStoreError(
			`legacy lock migration ${label} query was truncated`,
		);
	}
	if (result.stderr.length > 0) {
		throw new ProtocolStoreError(
			`legacy lock migration ${label} query returned stderr`,
		);
	}
	if (result.exitCode !== 0 && result.exitCode !== 1) {
		throw new ProtocolStoreError(
			`legacy lock migration could not query ${label}`,
		);
	}
	return result;
}

function parseProbePids(
	result: CommandResult,
	label: string,
	requireBarePid: boolean,
): string[] {
	if (result.exitCode === 1) {
		if (result.stdout.length === 0) return [];
	} else if (result.stdout.length > 0) {
		const pids = parsePidOutput(result.stdout, label, requireBarePid);
		if (pids.length > 0) return pids;
	}
	throw new ProtocolStoreError(
		`legacy lock migration ${label} query returned contradictory output`,
	);
}

export async function collectLegacyLockLiveEvidence(
	storeRoot: string,
	deps: LegacyLockMigrationDeps = {},
): Promise<
	Pick<
		LegacyLockMigrationResult,
		"sidecarPids" | "lockHolders" | "liveSupervisorPanes"
	>
> {
	const runner = deps.runner ?? bunCommandRunner;
	const root = new RunStore(storeRoot).root;
	const lockPath = resolve(root, ".manifest-lock.sqlite");

	const pgrep = requireCleanProbe(
		await runCaptured(runner, "pgrep", ["-af", "sidecar.ts"]),
		"sidecar PIDs",
	);
	const sidecarPids = parseProbePids(pgrep, "sidecar PIDs", false);

	const lsof = requireCleanProbe(
		await runCaptured(runner, "lsof", ["-t", "--", lockPath]),
		"lock holders",
	);
	const lockHolders = parseProbePids(lsof, "lock holder PIDs", true);

	const store = new RunStore(root);
	const herdr = deps.herdr ?? new HerdrClient(runner);
	const liveSupervisorPanes: string[] = [];
	for (const manifest of await store.listRuns({ failOnInvalid: true })) {
		if (
			isTerminalLifecycle(manifest.lifecycle) ||
			manifest.supervisorPaneId === undefined
		) {
			continue;
		}
		try {
			await herdr.inspectPane(manifest.supervisorPaneId, manifest.workspaceId);
			liveSupervisorPanes.push(manifest.supervisorPaneId);
		} catch (error) {
			const paneMissing =
				error instanceof HerdrServerError && error.code === "pane_not_found";
			if (!paneMissing) {
				throw new ProtocolStoreError(
					"legacy lock migration could not inspect a recorded supervisor pane",
				);
			}
		}
	}

	return { sidecarPids, lockHolders, liveSupervisorPanes };
}

export async function runLegacyLockMigration(
	storeRoot: string,
	deps: LegacyLockMigrationDeps = {},
): Promise<LegacyLockMigrationResult> {
	const root = new RunStore(storeRoot).root;
	const lockPath = resolve(root, ".manifest-lock.sqlite");
	try {
		const entry = await lstat(lockPath);
		if (entry.isDirectory() && !entry.isSymbolicLink()) {
			const archivedPath = await migrateLegacyManifestLockFile(
				storeRoot,
				{
					noSidecarPids: true,
					noLockHolders: true,
					missingSupervisorPanes: true,
				},
				deps.now,
			);
			return {
				archivedPath,
				sidecarPids: [],
				lockHolders: [],
				liveSupervisorPanes: [],
			};
		}
	} catch (error) {
		if (
			!(error instanceof Error) ||
			!("code" in error) ||
			error.code !== "ENOENT"
		) {
			throw error;
		}
	}
	const evidence = await collectLegacyLockLiveEvidence(storeRoot, deps);
	if (
		evidence.sidecarPids.length > 0 ||
		evidence.lockHolders.length > 0 ||
		evidence.liveSupervisorPanes.length > 0
	) {
		throw new ProtocolStoreError(
			`legacy lock migration refused: sidecarPids=${evidence.sidecarPids.join(",") || "none"} lockHolders=${evidence.lockHolders.join(",") || "none"} liveSupervisorPanes=${evidence.liveSupervisorPanes.join(",") || "none"}`,
		);
	}
	const archivedPath = await migrateLegacyManifestLockFile(
		storeRoot,
		{
			noSidecarPids: true,
			noLockHolders: true,
			missingSupervisorPanes: true,
		},
		deps.now,
	);
	return { archivedPath, ...evidence };
}
