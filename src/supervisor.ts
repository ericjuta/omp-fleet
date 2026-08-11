import type { HerdrAgent, HerdrClient } from "./herdr.ts";
import type { RunStore } from "./store.ts";
import {
	type AgentSnapshot,
	isHarvestStatus,
	isTerminalLifecycle,
	type ReportRecord,
	type RunEvent,
	type RunLifecycle,
	type RunManifest,
	type RunState,
	reportKey,
	reportRelativePath,
	SCHEMA_VERSION,
} from "./types.ts";

const REPORT_LINES = 200;
const MAX_REPORTS_PER_RUN = 64;
const MAX_HERDR_TIMEOUT_MILLISECONDS = 15_000;
const STARTING_LIFECYCLE = ["starting"] as const;
const ACTIVE_LIFECYCLES = ["starting", "running"] as const;
const STOPPING_LIFECYCLE = ["stopping"] as const;

export type SupervisorClock = () => Date;
export type SupervisorSleep = (
	milliseconds: number,
	signal: AbortSignal,
) => Promise<void>;

export interface SupervisorOptions {
	manifest: RunManifest;
	signal?: AbortSignal;
}

export interface SupervisorDependencies {
	store: Pick<
		RunStore,
		| "readManifest"
		| "readState"
		| "writeState"
		| "transitionManifest"
		| "appendEvent"
		| "writeReport"
	> &
		Partial<Pick<RunStore, "readEvents">>;
	herdr: Pick<HerdrClient, "listAgents" | "readPane">;
	now?: SupervisorClock;
	sleep?: SupervisorSleep;
}

class FatalSupervisorError extends Error {}

export function sleepUntil(
	milliseconds: number,
	signal: AbortSignal,
): Promise<void> {
	if (milliseconds <= 0 || signal.aborted) {
		return Promise.resolve();
	}

	const { promise, resolve } = Promise.withResolvers<void>();
	const finish = (): void => {
		clearTimeout(timer);
		signal.removeEventListener("abort", finish);
		resolve();
	};
	const timer = setTimeout(finish, milliseconds);
	signal.addEventListener("abort", finish, { once: true });
	return promise;
}

function iso(date: Date): string {
	if (Number.isNaN(date.getTime())) {
		throw new FatalSupervisorError("invalid supervisor clock");
	}
	return date.toISOString();
}
function operationTimeout(
	deadline: number,
	now: SupervisorClock,
): number | undefined {
	const current = now().getTime();
	if (Number.isNaN(current)) {
		throw new FatalSupervisorError("invalid supervisor clock");
	}
	const remaining = deadline - current;
	if (remaining <= 0) {
		return undefined;
	}
	return Math.min(MAX_HERDR_TIMEOUT_MILLISECONDS, remaining);
}

function compareSnapshots(left: AgentSnapshot, right: AgentSnapshot): number {
	if (left.name !== right.name) {
		return left.name < right.name ? -1 : 1;
	}
	if (left.paneId === right.paneId) {
		return 0;
	}
	return left.paneId < right.paneId ? -1 : 1;
}

function sameObservation(
	left: AgentSnapshot | undefined,
	right: AgentSnapshot,
): boolean {
	return (
		left !== undefined &&
		left.paneId === right.paneId &&
		left.workspaceId === right.workspaceId &&
		left.name === right.name &&
		left.status === right.status &&
		left.revision === right.revision
	);
}

function ownedSnapshots(
	agents: readonly HerdrAgent[],
	manifest: RunManifest,
	observedAt: string,
): AgentSnapshot[] {
	const byPane = new Map<string, AgentSnapshot>();
	for (const agent of agents) {
		if (
			agent.workspaceId !== manifest.workspaceId ||
			!agent.name.startsWith(manifest.workerPrefix) ||
			agent.paneId === manifest.coordinatorPaneId ||
			agent.paneId === manifest.supervisorPaneId
		) {
			continue;
		}
		byPane.set(agent.paneId, {
			paneId: agent.paneId,
			workspaceId: agent.workspaceId,
			name: agent.name,
			status: agent.status,
			revision: agent.revision,
			observedAt,
		});
	}
	return [...byPane.values()].sort(compareSnapshots);
}

function lifecycleEvent(manifest: RunManifest, timestamp: string): RunEvent {
	return {
		schemaVersion: SCHEMA_VERSION,
		runId: manifest.runId,
		type: "lifecycle",
		timestamp,
		lifecycle: manifest.lifecycle,
		...(manifest.lastError === undefined
			? {}
			: { lastError: manifest.lastError }),
	};
}

function observationEvent(
	manifest: RunManifest,
	snapshot: AgentSnapshot,
): RunEvent {
	return {
		schemaVersion: SCHEMA_VERSION,
		runId: manifest.runId,
		type: "agent",
		timestamp: snapshot.observedAt,
		agent: snapshot,
		outcome: "observed",
	};
}

function readFailureEvent(
	manifest: RunManifest,
	snapshot: AgentSnapshot,
	timestamp: string,
): RunEvent {
	return {
		schemaVersion: SCHEMA_VERSION,
		runId: manifest.runId,
		type: "agent",
		timestamp,
		agent: snapshot,
		outcome: "readFailed",
		lastError: "pane read failed",
	};
}

function reportEvent(manifest: RunManifest, report: ReportRecord): RunEvent {
	return {
		schemaVersion: SCHEMA_VERSION,
		runId: manifest.runId,
		type: "report",
		timestamp: report.observedAt,
		report,
	};
}

async function required<T>(
	message: string,
	operation: () => Promise<T>,
): Promise<T> {
	try {
		return await operation();
	} catch {
		throw new FatalSupervisorError(message);
	}
}

async function reconcileReportEvents(
	store: SupervisorDependencies["store"],
	manifest: RunManifest,
	state: RunState,
): Promise<void> {
	const readEvents = store.readEvents?.bind(store);
	if (readEvents === undefined || state.reports.length === 0) {
		return;
	}
	const events = await required("event read failed", () =>
		readEvents(manifest.runId),
	);
	const reported = new Set(
		events
			.filter((event) => event.type === "report")
			.map((event) => event.report.key),
	);
	for (const report of state.reports) {
		if (reported.has(report.key)) {
			continue;
		}
		await required("event append failed", () =>
			store.appendEvent(manifest.runId, reportEvent(manifest, report)),
		);
		reported.add(report.key);
	}
}

async function persistState(
	store: SupervisorDependencies["store"],
	state: RunState,
): Promise<RunState> {
	const durable = await required("state read failed", () =>
		store.readState(state.runId),
	);
	const reports = [...state.reports];
	const reportKeys = new Set(reports.map((report) => report.key));
	for (const report of durable.reports) {
		if (!reportKeys.has(report.key)) {
			reports.push(report);
			reportKeys.add(report.key);
		}
	}

	let noticeCursor = state.noticeCursor;
	if (
		durable.noticeCursor !== undefined &&
		(noticeCursor === undefined || durable.noticeCursor > noticeCursor)
	) {
		noticeCursor = durable.noticeCursor;
	}
	const merged: RunState = {
		schemaVersion: state.schemaVersion,
		runId: state.runId,
		updatedAt:
			Date.parse(durable.updatedAt) > Date.parse(state.updatedAt)
				? durable.updatedAt
				: state.updatedAt,
		agents: state.agents,
		reports,
		...(noticeCursor === undefined ? {} : { noticeCursor }),
	};
	await required("state write failed", () => store.writeState(merged));
	return merged;
}

function nextManifest(
	manifest: RunManifest,
	lifecycle: RunLifecycle,
	at: string,
	lastError?: string,
): RunManifest {
	const base: RunManifest = {
		...manifest,
		lifecycle,
		updatedAt: at,
		...(isTerminalLifecycle(lifecycle) ? { stoppedAt: at } : {}),
	};
	return lastError === undefined ? base : { ...base, lastError };
}

async function readDurableManifest(
	store: SupervisorDependencies["store"],
	runId: string,
): Promise<RunManifest> {
	return await required("manifest read failed", () =>
		store.readManifest(runId),
	);
}

async function persistLifecycle(
	store: SupervisorDependencies["store"],
	manifest: RunManifest,
	lifecycle: RunLifecycle,
	at: string,
	lastError?: string,
	bestEffortEvent = false,
): Promise<RunManifest> {
	let durable = await readDurableManifest(store, manifest.runId);
	while (true) {
		if (isTerminalLifecycle(durable.lifecycle)) {
			return durable;
		}

		const resolvedLifecycle =
			durable.lifecycle === "stopping" ? "stopped" : lifecycle;
		if (durable.lifecycle === resolvedLifecycle && lastError === undefined) {
			return durable;
		}
		const allowedFrom =
			durable.lifecycle === "stopping"
				? STOPPING_LIFECYCLE
				: resolvedLifecycle === "running"
					? STARTING_LIFECYCLE
					: ACTIVE_LIFECYCLES;
		const next = nextManifest(
			durable,
			resolvedLifecycle,
			at,
			resolvedLifecycle === "failed" ? lastError : undefined,
		);
		const current = await required("manifest transition failed", () =>
			store.transitionManifest(manifest.runId, allowedFrom, next),
		);
		if (current !== next) {
			if (current.lifecycle === "stopping") {
				durable = current;
				continue;
			}
			return current;
		}
		try {
			await store.appendEvent(next.runId, lifecycleEvent(next, at));
		} catch {
			if (!bestEffortEvent) {
				throw new FatalSupervisorError("event append failed");
			}
		}
		return next;
	}
}

async function appendChangedObservations(
	store: SupervisorDependencies["store"],
	manifest: RunManifest,
	previous: readonly AgentSnapshot[],
	current: readonly AgentSnapshot[],
): Promise<void> {
	const previousByPane = new Map(
		previous.map((snapshot) => [snapshot.paneId, snapshot]),
	);
	for (const snapshot of current) {
		if (sameObservation(previousByPane.get(snapshot.paneId), snapshot)) {
			continue;
		}
		await required("event append failed", () =>
			store.appendEvent(manifest.runId, observationEvent(manifest, snapshot)),
		);
	}
}

async function sample(
	manifest: RunManifest,
	state: RunState,
	dependencies: SupervisorDependencies,
	observedAt: string,
	deadline: number,
	now: SupervisorClock,
	signal: AbortSignal,
): Promise<RunState> {
	if (signal.aborted) {
		return state;
	}
	const listTimeout = operationTimeout(deadline, now);
	if (listTimeout === undefined) {
		return state;
	}
	let agents: HerdrAgent[];
	try {
		agents = await dependencies.herdr.listAgents(
			manifest.workspaceId,
			listTimeout,
		);
	} catch {
		if (signal.aborted || operationTimeout(deadline, now) === undefined) {
			return state;
		}
		throw new FatalSupervisorError("agent sampling failed");
	}
	if (signal.aborted) {
		return state;
	}
	const snapshots = ownedSnapshots(agents, manifest, observedAt);
	await appendChangedObservations(
		dependencies.store,
		manifest,
		state.agents,
		snapshots,
	);

	let nextState: RunState = {
		...state,
		updatedAt: observedAt,
		agents: snapshots,
	};
	if (signal.aborted) {
		return await persistState(dependencies.store, nextState);
	}
	const harvested = new Set(state.reports.map((report) => report.key));

	for (const snapshot of snapshots) {
		if (signal.aborted || nextState.reports.length >= MAX_REPORTS_PER_RUN) {
			break;
		}
		const status = snapshot.status;
		if (!isHarvestStatus(status)) {
			continue;
		}

		const key = reportKey(snapshot.paneId, snapshot.revision, status);
		if (harvested.has(key)) {
			continue;
		}
		const readTimeout = operationTimeout(deadline, now);
		if (readTimeout === undefined) {
			break;
		}

		let output: string;
		try {
			output = await dependencies.herdr.readPane(
				snapshot.paneId,
				manifest.workspaceId,
				REPORT_LINES,
				readTimeout,
			);
		} catch {
			if (signal.aborted || operationTimeout(deadline, now) === undefined) {
				break;
			}
			await required("event append failed", () =>
				dependencies.store.appendEvent(
					manifest.runId,
					readFailureEvent(manifest, snapshot, observedAt),
				),
			);
			continue;
		}
		if (signal.aborted) {
			break;
		}

		const pending: ReportRecord = {
			key,
			paneId: snapshot.paneId,
			workerName: snapshot.name,
			status,
			revision: snapshot.revision,
			path: reportRelativePath(
				snapshot.paneId,
				snapshot.name,
				snapshot.revision,
				status,
			),
			observedAt,
		};
		const report = await required("report write failed", () =>
			dependencies.store.writeReport(manifest.runId, pending, output),
		);
		harvested.add(report.key);
		nextState = { ...nextState, reports: [...nextState.reports, report] };
		nextState = await persistState(dependencies.store, nextState);
		await required("event append failed", () =>
			dependencies.store.appendEvent(
				manifest.runId,
				reportEvent(manifest, report),
			),
		);
	}

	return await persistState(dependencies.store, nextState);
}

/**
 * Runs one bounded, read-only supervisor. Agent states are observations only;
 * completion is based solely on the persisted deadline.
 */
export async function runSupervisor(
	options: SupervisorOptions,
	dependencies: SupervisorDependencies,
): Promise<RunManifest> {
	const now = dependencies.now ?? (() => new Date());
	const sleep = dependencies.sleep ?? sleepUntil;
	const signal = options.signal ?? new AbortController().signal;
	let manifest = { ...options.manifest };
	let deadline: number | undefined;

	try {
		manifest = await readDurableManifest(dependencies.store, manifest.runId);
		if (isTerminalLifecycle(manifest.lifecycle)) {
			return manifest;
		}
		if (
			manifest.supervisorPaneId === undefined ||
			manifest.supervisorPaneId.length === 0
		) {
			throw new FatalSupervisorError("supervisor pane is missing");
		}
		if (!Number.isFinite(manifest.pollSeconds) || manifest.pollSeconds <= 0) {
			throw new FatalSupervisorError("invalid polling interval");
		}

		const declaredDeadline = Date.parse(manifest.deadlineAt);
		const boundedDeadline =
			Date.parse(manifest.createdAt) + manifest.durationSeconds * 1_000;
		if (
			!Number.isFinite(declaredDeadline) ||
			!Number.isFinite(boundedDeadline)
		) {
			throw new FatalSupervisorError("invalid supervisor deadline");
		}
		deadline = Math.min(declaredDeadline, boundedDeadline);

		let state = await required("state read failed", () =>
			dependencies.store.readState(manifest.runId),
		);
		await reconcileReportEvents(dependencies.store, manifest, state);

		manifest = await readDurableManifest(dependencies.store, manifest.runId);
		if (isTerminalLifecycle(manifest.lifecycle)) {
			return manifest;
		}
		if (manifest.lifecycle === "stopping") {
			return await persistLifecycle(
				dependencies.store,
				manifest,
				"stopped",
				iso(now()),
			);
		}
		if (signal.aborted) {
			return await persistLifecycle(
				dependencies.store,
				manifest,
				"stopped",
				iso(now()),
			);
		}
		const initialTime = now();
		if (initialTime.getTime() >= deadline) {
			return await persistLifecycle(
				dependencies.store,
				manifest,
				"completed",
				iso(initialTime),
			);
		}
		if (manifest.lifecycle === "starting") {
			manifest = await persistLifecycle(
				dependencies.store,
				manifest,
				"running",
				iso(initialTime),
			);
			if (isTerminalLifecycle(manifest.lifecycle)) {
				return manifest;
			}
		}

		while (true) {
			if (signal.aborted) {
				return await persistLifecycle(
					dependencies.store,
					manifest,
					"stopped",
					iso(now()),
				);
			}

			manifest = await readDurableManifest(dependencies.store, manifest.runId);
			if (isTerminalLifecycle(manifest.lifecycle)) {
				return manifest;
			}
			if (manifest.lifecycle === "stopping") {
				return await persistLifecycle(
					dependencies.store,
					manifest,
					"stopped",
					iso(now()),
				);
			}

			const sampleTime = now();
			if (sampleTime.getTime() >= deadline) {
				return await persistLifecycle(
					dependencies.store,
					manifest,
					"completed",
					iso(sampleTime),
				);
			}

			state = await sample(
				manifest,
				state,
				dependencies,
				iso(sampleTime),
				deadline,
				now,
				signal,
			);
			if (signal.aborted) {
				continue;
			}

			const afterSample = now();
			if (Number.isNaN(afterSample.getTime())) {
				throw new FatalSupervisorError("invalid supervisor clock");
			}
			const remaining = deadline - afterSample.getTime();
			if (remaining <= 0) {
				continue;
			}
			try {
				await sleep(Math.min(manifest.pollSeconds * 1_000, remaining), signal);
			} catch {
				if (!signal.aborted) {
					throw new FatalSupervisorError("supervisor sleep failed");
				}
			}
		}
	} catch (error) {
		const terminalTime = now();
		const terminalAt = iso(terminalTime);
		if (signal.aborted) {
			return await persistLifecycle(
				dependencies.store,
				manifest,
				"stopped",
				terminalAt,
			);
		}
		const message =
			error instanceof FatalSupervisorError
				? error.message
				: "supervisor failed";
		return await persistLifecycle(
			dependencies.store,
			manifest,
			"failed",
			terminalAt,
			message,
			true,
		);
	}
}
