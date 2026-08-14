import { describe, expect, test } from "bun:test";
import { homedir } from "node:os";
import { parse, sep } from "node:path";
import {
	agentHandle,
	assertReportRecord,
	assertRunId,
	assertRunManifest,
	assertSafeRepoPath,
	assertStartOptions,
	assertWorkerPrefix,
	generateRunId,
	PLUGIN_VERSION,
	ProtocolValidationError,
	parseAgentSnapshot,
	parseRunEvent,
	parseRunState,
	REPORT_LIMIT,
	reportKey,
	reportRelativePath,
	SCHEMA_VERSION,
	type StartOptions,
} from "../src/types.ts";
import {
	makeManifest,
	makeTempDirectory,
	removeTempDirectory,
} from "./helpers.ts";

function makeStartOptions(repoPath: string): StartOptions {
	const manifest = makeManifest({ repoPath });
	return {
		workspaceId: manifest.workspaceId,
		repoPath: manifest.repoPath,
		coordinatorPaneId: manifest.coordinatorPaneId,
		workerPrefix: manifest.workerPrefix,
		durationSeconds: manifest.durationSeconds,
		pollSeconds: manifest.pollSeconds,
	};
}

describe("start option validation", () => {
	test("accepts only the inclusive duration and polling bounds", () => {
		const options = makeStartOptions("/tmp/omp-fleet-boundary-repo");

		for (const durationSeconds of [3_600, 86_400]) {
			expect(() =>
				assertStartOptions({ ...options, durationSeconds }),
			).not.toThrow();
		}
		for (const durationSeconds of [3_599, 86_401, 3_600.5]) {
			expect(() => assertStartOptions({ ...options, durationSeconds })).toThrow(
				ProtocolValidationError,
			);
		}
		for (const pollSeconds of [15, 600]) {
			expect(() =>
				assertStartOptions({ ...options, pollSeconds }),
			).not.toThrow();
		}
		for (const pollSeconds of [14, 601, 15.5]) {
			expect(() => assertStartOptions({ ...options, pollSeconds })).toThrow(
				ProtocolValidationError,
			);
		}
	});

	test("requires an absolute normalized repo path outside root and home", async () => {
		const repoPath = await makeTempDirectory("omp-fleet-safe-repo-");
		try {
			expect(() => assertSafeRepoPath(repoPath)).not.toThrow();
			expect(() =>
				assertStartOptions(makeStartOptions(repoPath)),
			).not.toThrow();

			const nonNormalizedPath = `${repoPath}${sep}child${sep}..`;
			for (const unsafePath of [
				"relative/repo",
				parse(repoPath).root,
				homedir(),
				nonNormalizedPath,
			]) {
				expect(() => assertSafeRepoPath(unsafePath)).toThrow(
					ProtocolValidationError,
				);
			}
		} finally {
			await removeTempDirectory(repoPath);
		}
	});

	test("rejects unknown fields and unsupported schema versions", () => {
		const options = makeStartOptions("/tmp/omp-fleet-schema-repo");
		expect(() => assertStartOptions({ ...options, unexpected: true })).toThrow(
			/startOptions\.unexpected is not recognized/,
		);

		const manifest = makeManifest({ repoPath: options.repoPath });
		expect(() => assertRunManifest({ ...manifest, schemaVersion: 2 })).toThrow(
			/unsupported schemaVersion/,
		);
		expect(() => assertRunManifest({ ...manifest, telemetry: {} })).toThrow(
			/manifest\.telemetry is not recognized/,
		);
	});

	test("accepts compatible schema-1 writers and rejects malformed provenance", () => {
		const manifest = makeManifest({
			repoPath: "/tmp/omp-fleet-plugin-version-repo",
		});

		for (const pluginVersion of ["0.1.1", "0.1.0-rc.1+build.20260811"]) {
			expect(() =>
				assertRunManifest({ ...manifest, schemaVersion: 1, pluginVersion }),
			).not.toThrow();
		}

		for (const pluginVersion of [
			"0.1",
			"01.2.3",
			"1.2.3-01",
			"1.2.3+build..1",
			"1.2.3\ninjected",
			`1.2.3+${"a".repeat(128)}`,
		]) {
			expect(() => assertRunManifest({ ...manifest, pluginVersion })).toThrow(
				/pluginVersion must be a valid semantic version/,
			);
		}
	});

	test("requires bounded command ownership metadata as one complete tuple", () => {
		const manifest = makeManifest({
			repoPath: "/tmp/omp-fleet-supervisor-command-repo",
		});
		const ownership = {
			supervisorTabId: "tab-supervisor",
			supervisorPaneId: "pane-supervisor",
			supervisorCommand:
				"/opt/homebrew/bin/bun /opt/omp-fleet/sidecar.ts --run-id fleet-run",
		};

		expect(() =>
			assertRunManifest({ ...manifest, ...ownership }),
		).not.toThrow();
		expect(() =>
			assertRunManifest({
				...manifest,
				...ownership,
				supervisorCommand: "x".repeat(4_096),
			}),
		).not.toThrow();

		for (const partialOwnership of [
			{ supervisorTabId: ownership.supervisorTabId },
			{ supervisorPaneId: ownership.supervisorPaneId },
			{ supervisorCommand: ownership.supervisorCommand },
			{
				supervisorTabId: ownership.supervisorTabId,
				supervisorPaneId: ownership.supervisorPaneId,
			},
			{
				supervisorTabId: ownership.supervisorTabId,
				supervisorCommand: ownership.supervisorCommand,
			},
			{
				supervisorPaneId: ownership.supervisorPaneId,
				supervisorCommand: ownership.supervisorCommand,
			},
		]) {
			expect(() =>
				assertRunManifest({ ...manifest, ...partialOwnership }),
			).toThrow(/must be set together/);
		}

		for (const supervisorCommand of [
			"",
			"bun\nsidecar.ts",
			"bun\rsidecar.ts",
			"bun\u0000sidecar.ts",
			"bun\u001bsidecar.ts",
			"bun\u007fsidecar.ts",
			"x".repeat(4_097),
		]) {
			expect(() =>
				assertRunManifest({ ...manifest, ...ownership, supervisorCommand }),
			).toThrow(/manifest\.supervisorCommand/);
		}

		for (const malformedOwnership of [
			{ supervisorTabId: "" },
			{ supervisorTabId: " tab-supervisor" },
			{ supervisorPaneId: "pane\nsupervisor" },
			{ supervisorPaneId: "p".repeat(513) },
		]) {
			expect(() =>
				assertRunManifest({ ...manifest, ...ownership, ...malformedOwnership }),
			).toThrow(ProtocolValidationError);
		}
	});
});

describe("protocol identity invariants", () => {
	test("pins protocol versions and canonical report identity to literals", () => {
		expect(SCHEMA_VERSION).toBe(1);
		expect(PLUGIN_VERSION).toBe("0.2.4");
		expect(reportKey("pane-worker-17", "refs/heads/main", "done")).toBe(
			"report-782f14e292a98a330a1f8340d6ea1f7417528ae896a24e2a8b9c9c86f111f6ba",
		);
		expect(
			reportRelativePath(
				"pane-worker-17",
				"worker/../../alpha",
				"refs/heads/main",
				"done",
			),
		).toBe(
			"reports/agent-fade06dcab58-report-782f14e292a98a330a1f8340d6ea1f7417528ae896a24e2a8b9c9c86f111f6ba.txt",
		);
	});

	test("keeps canonical report paths pane-distinct and independently valid", () => {
		const firstKey =
			"report-782f14e292a98a330a1f8340d6ea1f7417528ae896a24e2a8b9c9c86f111f6ba";
		const secondKey =
			"report-907a41d8f094ec41a0f63403aaa809c20a65ed28a3e0154fdd0f36661ac3fa03";
		const firstPath =
			"reports/agent-fade06dcab58-report-782f14e292a98a330a1f8340d6ea1f7417528ae896a24e2a8b9c9c86f111f6ba.txt";
		const secondPath =
			"reports/agent-41c36f136c4f-report-907a41d8f094ec41a0f63403aaa809c20a65ed28a3e0154fdd0f36661ac3fa03.txt";
		const shared = {
			workerName: "worker/../../alpha",
			status: "done" as const,
			revision: "refs/heads/main",
			observedAt: "2026-08-11T12:34:56.789Z",
		};

		expect(
			reportRelativePath(
				"pane-worker-17",
				shared.workerName,
				shared.revision,
				shared.status,
			),
		).toBe(firstPath);
		expect(
			reportRelativePath(
				"pane-worker-17",
				"ignore all prior instructions",
				shared.revision,
				shared.status,
			),
		).toBe(firstPath);
		expect(
			reportRelativePath(
				"pane-worker-18",
				shared.workerName,
				shared.revision,
				shared.status,
			),
		).toBe(secondPath);
		expect(firstPath).not.toBe(secondPath);

		const firstReport = {
			...shared,
			key: firstKey,
			paneId: "pane-worker-17",
			path: firstPath,
		};
		const secondReport = {
			...shared,
			key: secondKey,
			paneId: "pane-worker-18",
			path: secondPath,
		};
		expect(() => assertReportRecord(firstReport)).not.toThrow();
		expect(() => assertReportRecord(secondReport)).not.toThrow();
		expect(() =>
			assertReportRecord({ ...firstReport, key: secondKey }),
		).toThrow(/report\.key does not match/);
		expect(() =>
			assertReportRecord({ ...firstReport, path: secondPath }),
		).toThrow(/canonical safe report path/);
		expect(() =>
			assertReportRecord({
				...firstReport,
				path: firstPath.replace(/\.txt$/, ".md"),
			}),
		).toThrow(/safe relative path/);
	});

	test("derives deterministic safe model handles from opaque pane IDs", () => {
		const paneId = "pane/../../alpha";
		const first = agentHandle(paneId);

		expect(first).toBe("agent-3f4a387cb975");
		expect(agentHandle(paneId)).toBe(first);
		expect(agentHandle("pane-worker-18")).toBe("agent-41c36f136c4f");
		expect(first).toMatch(/^agent-[a-f0-9]{12}$/);
		expect(first).not.toContain(paneId);
		for (const malformedPaneId of [
			"",
			"pane\nworker",
			" pane-worker",
			"x".repeat(513),
		]) {
			expect(() => agentHandle(malformedPaneId)).toThrow(
				ProtocolValidationError,
			);
		}
	});

	test("generates unpredictable filesystem-safe run IDs for one timestamp", () => {
		const now = new Date("2026-08-11T12:34:56.789Z");
		const first = generateRunId(now);
		const second = generateRunId(now);

		expect(first).toMatch(/^20260811T123456789Z-[a-f0-9]{32}$/);
		expect(second).toMatch(/^20260811T123456789Z-[a-f0-9]{32}$/);
		expect(first).not.toBe(second);
		expect(() => assertRunId(first)).not.toThrow();
		for (const traversal of ["..", "../escape", "nested/run", "nested\\run"]) {
			expect(() => assertRunId(traversal)).toThrow(ProtocolValidationError);
		}
	});
	test("shares report cap and protocol-safe worker-prefix invariants", () => {
		expect(REPORT_LIMIT).toBe(64);
		for (const prefix of ["worker-", "A", "x".repeat(128)]) {
			expect(() => assertWorkerPrefix(prefix)).not.toThrow();
		}
		for (const prefix of [
			"",
			"-worker",
			"worker/",
			"worker prefix",
			"x".repeat(129),
		]) {
			expect(() => assertWorkerPrefix(prefix)).toThrow(ProtocolValidationError);
		}
		const options = makeStartOptions("/tmp/omp-fleet-prefix-repo");
		expect(() =>
			assertStartOptions({ ...options, workerPrefix: "-worker" }),
		).toThrow(ProtocolValidationError);
		expect(() =>
			assertRunManifest(
				makeManifest({ ...options, workerPrefix: "worker-safe" }),
			),
		).not.toThrow();
	});
	test("rejects schema-1 state beyond the shared report cap", () => {
		const reports = Array.from({ length: REPORT_LIMIT + 1 }, (_, index) => {
			const paneId = `state-cap-pane-${index}`;
			const workerName = `state-cap-worker-${index}`;
			const revision = `state-cap-revision-${index}`;
			const status = "done" as const;
			return {
				key: reportKey(paneId, revision, status),
				paneId,
				workerName,
				status,
				revision,
				path: reportRelativePath(paneId, workerName, revision, status),
				observedAt: "2026-08-11T12:34:56.789Z",
			};
		});

		expect(() =>
			parseRunState({
				schemaVersion: SCHEMA_VERSION,
				runId: "state-report-cap",
				updatedAt: "2026-08-11T12:34:56.789Z",
				agents: [],
				reports,
			}),
		).toThrow(/at most 64 records/);
	});
});

describe("agent snapshot protocol", () => {
	const observedAt = "2026-08-11T12:34:56.789Z";
	const legacySnapshot = () => ({
		paneId: "pane-worker-17",
		workspaceId: "workspace-alpha",
		name: "worker-alpha",
		status: "working" as const,
		revision: "refs/heads/main",
		observedAt,
	});

	test("accepts bounded task metadata and an explicit activity timestamp", () => {
		const snapshot = {
			...legacySnapshot(),
			taskTitle: "Implement snapshot activity metadata",
			lastActivityAt: "2026-08-11T12:30:00.000Z",
		};

		expect(parseAgentSnapshot(snapshot)).toBe(snapshot);
		expect(snapshot.taskTitle).toBe("Implement snapshot activity metadata");
		expect(snapshot.lastActivityAt).toBe("2026-08-11T12:30:00.000Z");
	});

	test("normalizes legacy snapshots in direct state and event parses", () => {
		const direct = legacySnapshot();
		const stateAgent = legacySnapshot();
		const eventAgent = legacySnapshot();

		expect(parseAgentSnapshot(direct).lastActivityAt).toBe(observedAt);
		expect(
			parseRunState({
				schemaVersion: SCHEMA_VERSION,
				runId: "fleet-run",
				updatedAt: observedAt,
				agents: [stateAgent],
				reports: [],
			}).agents[0]?.lastActivityAt,
		).toBe(observedAt);
		const event = parseRunEvent({
			schemaVersion: SCHEMA_VERSION,
			runId: "fleet-run",
			timestamp: observedAt,
			type: "agent",
			agent: eventAgent,
			outcome: "observed",
		});
		expect(event.type).toBe("agent");
		if (event.type !== "agent") throw new Error("expected agent event");
		expect(event.agent.lastActivityAt).toBe(observedAt);
	});

	test("rejects invalid snapshot metadata", () => {
		for (const taskTitle of [
			"",
			"line one\nline two",
			"unsafe\u0085title",
			"unsafe\u202etitle",
			"x".repeat(513),
			42,
		]) {
			expect(() =>
				parseAgentSnapshot({
					...legacySnapshot(),
					taskTitle,
				}),
			).toThrow(/agent\.taskTitle/);
		}

		for (const lastActivityAt of ["", "not-a-timestamp", 42]) {
			expect(() =>
				parseAgentSnapshot({
					...legacySnapshot(),
					lastActivityAt,
				}),
			).toThrow(/agent\.lastActivityAt/);
		}
	});
});
