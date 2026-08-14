import type { HerdrAgent, HerdrClient } from "./herdr.ts";
import type { RunStore } from "./store.ts";
import {
	type AgentSnapshot,
	isHarvestStatus,
	isTerminalLifecycle,
	REPORT_LIMIT,
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

export type DurablePublicationStore = Pick<
	RunStore,
	"readState" | "writeState" | "appendEvent"
> &
	Partial<Pick<RunStore, "readEvents" | "listStoredReports">>;

export interface SupervisorDependencies {
	store: DurablePublicationStore &
		Pick<RunStore, "readManifest" | "transitionManifest" | "writeReport">;
	herdr: Pick<HerdrClient, "listAgents" | "readPane">;
	now?: SupervisorClock;
	sleep?: SupervisorSleep;
}

class FatalSupervisorError extends Error {}

function isTypedCancellation(error: unknown): boolean {
	return error instanceof Error && error.name === "AbortError";
}

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
		left.revision === right.revision &&
		left.taskTitle === right.taskTitle
	);
}

function ownedSnapshots(
	agents: readonly HerdrAgent[],
	manifest: RunManifest,
	observedAt: string,
	previous: readonly AgentSnapshot[],
): AgentSnapshot[] {
	const previousByPane = new Map(
		previous.map((snapshot) => [snapshot.paneId, snapshot]),
	);
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
		const prior = previousByPane.get(agent.paneId);
		const activityChanged =
			prior === undefined ||
			prior.status !== agent.status ||
			prior.revision !== agent.revision ||
			prior.taskTitle !== agent.taskTitle;
		const snapshot: AgentSnapshot = {
			paneId: agent.paneId,
			workspaceId: agent.workspaceId,
			name: agent.name,
			status: agent.status,
			revision: agent.revision,
			observedAt,
			lastActivityAt: activityChanged
				? observedAt
				: (prior.lastActivityAt ?? prior.observedAt),
		};
		if (agent.taskTitle !== undefined) {
			snapshot.taskTitle = agent.taskTitle;
		}
		byPane.set(agent.paneId, snapshot);
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

function sameLifecycleEvent(left: RunEvent, right: RunEvent): boolean {
	return (
		left.type === "lifecycle" &&
		right.type === "lifecycle" &&
		left.schemaVersion === right.schemaVersion &&
		left.runId === right.runId &&
		left.timestamp === right.timestamp &&
		left.lifecycle === right.lifecycle &&
		left.lastError === right.lastError
	);
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
	} catch (error) {
		if (isTypedCancellation(error)) {
			throw error;
		}
		throw new FatalSupervisorError(message);
	}
}

async function reconcileReportEvents(
	store: DurablePublicationStore,
	manifest: RunManifest,
	state: RunState,
): Promise<void> {
	const readEvents = store.readEvents?.bind(store);
	if (readEvents === undefined) {
		return;
	}
	const events = await required("event read failed", () =>
		readEvents(manifest.runId),
	);
	const reportedKeys = new Set<string>();
	const reportedPaths = new Set<string>();
	for (const event of events) {
		if (event.type !== "report") {
			continue;
		}
		const match = state.reports.find(
			(report) =>
				report.key === event.report.key || report.path === event.report.path,
		);
		if (
			match === undefined ||
			event.timestamp !== event.report.observedAt ||
			!sameReportRecord(match, event.report)
		) {
			throw new FatalSupervisorError(
				"state report conflicts with report event",
			);
		}
		reportedKeys.add(event.report.key);
		reportedPaths.add(event.report.path);
	}
	for (const report of state.reports) {
		if (reportedKeys.has(report.key) || reportedPaths.has(report.path)) {
			continue;
		}
		await required("event append failed", () =>
			store.appendEvent(manifest.runId, reportEvent(manifest, report)),
		);
		reportedKeys.add(report.key);
		reportedPaths.add(report.path);
	}
}
function sameReportRecord(left: ReportRecord, right: ReportRecord): boolean {
	return (
		left.key === right.key &&
		left.paneId === right.paneId &&
		left.workerName === right.workerName &&
		left.status === right.status &&
		left.revision === right.revision &&
		left.path === right.path &&
		left.observedAt === right.observedAt
	);
}
async function reconcileStoredReports(
	store: DurablePublicationStore,
	state: RunState,
	requireStoredAgreement = false,
): Promise<RunState> {
	const listStoredReports = store.listStoredReports?.bind(store);
	if (listStoredReports === undefined) {
		return state;
	}
	const stored = await required("stored report read failed", () =>
		listStoredReports(state.runId),
	);
	const reports = [...state.reports];
	const byKey = new Map(reports.map((report) => [report.key, report]));
	const byPath = new Map(reports.map((report) => [report.path, report]));
	let changed = false;
	for (const report of stored) {
		const keyMatch = byKey.get(report.key);
		const pathMatch = byPath.get(report.path);
		if (keyMatch === undefined && pathMatch === undefined) {
			reports.push(report);
			byKey.set(report.key, report);
			byPath.set(report.path, report);
			changed = true;
			continue;
		}
		if (
			keyMatch !== undefined &&
			keyMatch === pathMatch &&
			sameReportRecord(keyMatch, report)
		) {
			continue;
		}
		throw new FatalSupervisorError(
			"stored report metadata conflicts with state",
		);
	}
	if (requireStoredAgreement) {
		const storedByKey = new Map(stored.map((report) => [report.key, report]));
		const storedByPath = new Map(stored.map((report) => [report.path, report]));
		for (const report of reports) {
			const keyed = storedByKey.get(report.key);
			const pathed = storedByPath.get(report.path);
			if (
				keyed === undefined ||
				pathed === undefined ||
				!sameReportRecord(keyed, report) ||
				!sameReportRecord(pathed, report)
			) {
				throw new FatalSupervisorError(
					"stored report metadata conflicts with state",
				);
			}
		}
	}
	return changed ? await persistState(store, { ...state, reports }) : state;
}
async function reconcileLifecycleEvent(
	store: DurablePublicationStore,
	manifest: RunManifest,
): Promise<void> {
	if (manifest.lifecycle === "starting") {
		return;
	}
	const readEvents = store.readEvents?.bind(store);
	if (readEvents === undefined) {
		return;
	}
	const events = await required("event read failed", () =>
		readEvents(manifest.runId),
	);
	const expected = lifecycleEvent(manifest, manifest.updatedAt);
	if (isTerminalLifecycle(manifest.lifecycle)) {
		const tail = events.at(-1);
		if (tail !== undefined && sameLifecycleEvent(tail, expected)) {
			return;
		}
	} else if (
		events.some(
			(event) =>
				event.type === "lifecycle" &&
				event.lifecycle === manifest.lifecycle &&
				event.timestamp === manifest.updatedAt,
		)
	) {
		return;
	}
	await required("event append failed", () =>
		store.appendEvent(manifest.runId, expected),
	);
}
async function guaranteeLifecycleEvent(
	store: DurablePublicationStore,
	manifest: RunManifest,
): Promise<void> {
	if (store.readEvents === undefined) {
		if (
			isTerminalLifecycle(manifest.lifecycle) ||
			manifest.lifecycle === "stopping"
		) {
			throw new FatalSupervisorError("event append failed");
		}
		return;
	}
	await reconcileLifecycleEvent(store, manifest);
}

async function reconcileDurablePublication(
	store: DurablePublicationStore,
	manifest: RunManifest,
	state: RunState,
): Promise<RunState> {
	if (
		isTerminalLifecycle(manifest.lifecycle) &&
		store.listStoredReports === undefined
	) {
		throw new FatalSupervisorError("stored report read failed");
	}
	const reconciled = await reconcileStoredReports(
		store,
		state,
		isTerminalLifecycle(manifest.lifecycle),
	);
	await reconcileReportEvents(store, manifest, reconciled);
	await guaranteeLifecycleEvent(store, manifest);
	return reconciled;
}

export async function requireDurableConvergence(
	store: DurablePublicationStore,
	manifest: RunManifest,
): Promise<RunState> {
	const state = await required("state read failed", () =>
		store.readState(manifest.runId),
	);
	return await reconcileDurablePublication(store, manifest, state);
}

async function persistState(
	store: DurablePublicationStore,
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
): Promise<RunManifest> {
	let durable = await readDurableManifest(store, manifest.runId);
	let ensured: { lifecycle: RunLifecycle; updatedAt: string } | undefined;
	const ensureEvent = async (target: RunManifest): Promise<void> => {
		if (
			ensured?.lifecycle === target.lifecycle &&
			ensured.updatedAt === target.updatedAt
		) {
			return;
		}
		await guaranteeLifecycleEvent(store, target);
		ensured = {
			lifecycle: target.lifecycle,
			updatedAt: target.updatedAt,
		};
	};
	while (true) {
		if (isTerminalLifecycle(durable.lifecycle)) {
			await requireDurableConvergence(store, durable);
			return durable;
		}
		if (durable.lifecycle === "stopping") {
			await ensureEvent(durable);
		}

		const resolvedLifecycle =
			durable.lifecycle === "stopping" ? "stopped" : lifecycle;
		if (durable.lifecycle === resolvedLifecycle && lastError === undefined) {
			await ensureEvent(durable);
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
			durable = current;
			if (isTerminalLifecycle(durable.lifecycle)) {
				await requireDurableConvergence(store, durable);
				return durable;
			}
			await ensureEvent(durable);
			if (durable.lifecycle === "stopping") {
				continue;
			}
			return durable;
		}
		if (isTerminalLifecycle(next.lifecycle)) {
			try {
				await requireDurableConvergence(store, next);
			} catch {
				await requireDurableConvergence(store, next);
			}
			return next;
		}
		try {
			await store.appendEvent(next.runId, lifecycleEvent(next, at));
		} catch {
			if (store.readEvents === undefined) {
				await required("event append failed", () =>
					store.appendEvent(next.runId, lifecycleEvent(next, at)),
				);
			} else {
				await reconcileLifecycleEvent(store, next);
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
	const snapshots = ownedSnapshots(agents, manifest, observedAt, state.agents);
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
		if (signal.aborted || nextState.reports.length >= REPORT_LIMIT) {
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
	let state: RunState;

	try {
		manifest = await readDurableManifest(dependencies.store, manifest.runId);
		state = await requireDurableConvergence(dependencies.store, manifest);
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

		manifest = await readDurableManifest(dependencies.store, manifest.runId);
		state = await requireDurableConvergence(dependencies.store, manifest);
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
			state = await requireDurableConvergence(dependencies.store, manifest);
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
		if (isTypedCancellation(error)) {
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
		);
	}
}
