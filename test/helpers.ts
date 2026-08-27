import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { FleetAttachment, RunManifest, RunState } from "../src/types.ts";

const DEFAULT_RUN_ID = "20260811T000000000Z-0123456789abcdef0123456789abcdef";
const DEFAULT_CREATED_AT = "2026-08-11T00:00:00.000Z";
const DEFAULT_DURATION_SECONDS = 21_600;

export async function makeTempDirectory(
	prefix = "omp-fleet-test-",
): Promise<string> {
	return await mkdtemp(join(tmpdir(), prefix));
}

export async function removeTempDirectory(path: string): Promise<void> {
	await rm(path, { recursive: true, force: true });
}

export function makeManifest(
	overrides: Partial<RunManifest> = {},
): RunManifest {
	const manifest: RunManifest = {
		schemaVersion: 1,
		pluginVersion: "0.1.0",
		runId: DEFAULT_RUN_ID,
		lifecycle: "starting",
		workspaceId: "workspace-test",
		repoPath: "/tmp/omp-fleet-monitored-repo",
		coordinatorPaneId: "pane-coordinator",
		workerPrefix: "worker-",
		durationSeconds: DEFAULT_DURATION_SECONDS,
		pollSeconds: 30,
		createdAt: DEFAULT_CREATED_AT,
		updatedAt: DEFAULT_CREATED_AT,
		deadlineAt: "2026-08-11T06:00:00.000Z",
		...overrides,
	};

	if (overrides.createdAt !== undefined && overrides.updatedAt === undefined) {
		manifest.updatedAt = overrides.createdAt;
	}
	if (
		overrides.deadlineAt === undefined &&
		(overrides.createdAt !== undefined ||
			overrides.durationSeconds !== undefined)
	) {
		manifest.deadlineAt = new Date(
			Date.parse(manifest.createdAt) + manifest.durationSeconds * 1_000,
		).toISOString();
	}

	return manifest;
}

export function makeState(overrides: Partial<RunState> = {}): RunState {
	return {
		schemaVersion: 1,
		runId: DEFAULT_RUN_ID,
		updatedAt: DEFAULT_CREATED_AT,
		agents: [],
		reports: [],
		...overrides,
	};
}
export function makeAttachment(
	overrides: Partial<FleetAttachment> = {},
): FleetAttachment {
	return {
		schemaVersion: 1,
		sessionId: "session-test",
		runId: DEFAULT_RUN_ID,
		workerPrefix: "worker-",
		coordinatorHandle: "agent-0123456789ab",
		deadlineAt: "2026-08-11T06:00:00.000Z",
		lifecycle: "running",
		observationHealth: "current",
		workerCount: 2,
		reportCount: 1,
		cursor: 0,
		...overrides,
	};
}
