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

const SIDECAR_NEEDLES = [
	"omp-fleet/src/sidecar.ts",
	"@ericjuta/omp-fleet/src/sidecar.ts",
];

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

function parsePidLines(text: string): string[] {
	const pids: string[] = [];
	for (const line of text.split("\n")) {
		const match = /^(\d+)\b/.exec(line.trim());
		if (match?.[1] !== undefined) pids.push(match[1]);
	}
	return pids;
}

function isDocumentedNoMatch(exitCode: number): boolean {
	return exitCode === 1;
}

function requireCleanProbe(
	result: CommandResult | undefined,
	label: string,
	allowNoMatch: boolean,
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
	if (result.stderr.trim().length > 0) {
		throw new ProtocolStoreError(
			`legacy lock migration ${label} query returned stderr`,
		);
	}
	if (
		result.exitCode !== 0 &&
		!(allowNoMatch && isDocumentedNoMatch(result.exitCode))
	) {
		throw new ProtocolStoreError(
			`legacy lock migration could not query ${label}`,
		);
	}
	return result;
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
		true,
	);
	const sidecarPids = pgrep.stdout
		.split("\n")
		.map((line) => line.trim())
		.filter(
			(line) =>
				line.length > 0 &&
				SIDECAR_NEEDLES.some((needle) => line.includes(needle)),
		)
		.flatMap((line) => parsePidLines(line));

	const lsof = requireCleanProbe(
		await runCaptured(runner, "lsof", ["-t", "--", lockPath]),
		"lock holders",
		true,
	);
	const lockHolders = isDocumentedNoMatch(lsof.exitCode)
		? []
		: parsePidLines(lsof.stdout);

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
				(error instanceof HerdrServerError &&
					error.code === "pane_not_found") ||
				(error instanceof Error && /\bpane_not_found\b/.test(error.message));
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
