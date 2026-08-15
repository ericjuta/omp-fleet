import { createHash, randomBytes } from "node:crypto";
import { homedir } from "node:os";
import { isAbsolute, normalize, parse, resolve } from "node:path";

export const SCHEMA_VERSION = 1 as const;
export const PLUGIN_VERSION = "0.2.5" as const;
export const REPORT_LIMIT = 64 as const;

export const RUN_LIFECYCLES = [
	"starting",
	"running",
	"stopping",
	"stopped",
	"completed",
	"failed",
] as const;

export type RunLifecycle = (typeof RUN_LIFECYCLES)[number];

export const AGENT_STATUSES = [
	"idle",
	"working",
	"blocked",
	"done",
	"exited",
	"unknown",
] as const;

export type AgentStatus = (typeof AGENT_STATUSES)[number];
export type HarvestStatus = Extract<AgentStatus, "blocked" | "done">;

export interface StartOptions {
	workspaceId: string;
	repoPath: string;
	coordinatorPaneId: string;
	workerPrefix: string;
	durationSeconds: number;
	pollSeconds: number;
}

export interface RunManifest extends StartOptions {
	schemaVersion: typeof SCHEMA_VERSION;
	pluginVersion: string;
	runId: string;
	lifecycle: RunLifecycle;
	supervisorTabId?: string;
	supervisorPaneId?: string;
	supervisorCommand?: string;
	createdAt: string;
	updatedAt: string;
	deadlineAt: string;
	stoppedAt?: string;
	lastError?: string;
}

export interface AgentSnapshot {
	paneId: string;
	workspaceId: string;
	name: string;
	status: AgentStatus;
	revision: string;
	observedAt: string;
	taskTitle?: string;
	lastActivityAt: string;
}

export interface ReportRecord {
	key: string;
	paneId: string;
	workerName: string;
	status: HarvestStatus;
	revision: string;
	path: string;
	observedAt: string;
}

export interface RunState {
	schemaVersion: typeof SCHEMA_VERSION;
	runId: string;
	updatedAt: string;
	agents: AgentSnapshot[];
	reports: ReportRecord[];
	noticeCursor?: number;
}

interface RunEventBase {
	schemaVersion: typeof SCHEMA_VERSION;
	runId: string;
	timestamp: string;
}

export interface LifecycleRunEvent extends RunEventBase {
	type: "lifecycle";
	lifecycle: RunLifecycle;
	lastError?: string;
}

export type AgentEventOutcome = "observed" | "readFailed";

export interface AgentRunEvent extends RunEventBase {
	type: "agent";
	agent: AgentSnapshot;
	outcome: AgentEventOutcome;
	lastError?: string;
}

export interface ReportRunEvent extends RunEventBase {
	type: "report";
	report: ReportRecord;
}

export type RunEvent = LifecycleRunEvent | AgentRunEvent | ReportRunEvent;

export class ProtocolValidationError extends Error {
	override readonly name = "ProtocolValidationError";
}

export type UnknownRecord = Record<string, unknown>;

const ISO_TIMESTAMP =
	/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/;
const RUN_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const REPORT_PATH = /^reports\/agent-[a-f0-9]{12}-report-[a-f0-9]{64}\.txt$/;
export function containsControlCharacter(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const codeUnit = value.charCodeAt(index);
		if (
			codeUnit <= 0x1f ||
			(codeUnit >= 0x7f && codeUnit <= 0x9f) ||
			codeUnit === 0x061c ||
			codeUnit === 0x200e ||
			codeUnit === 0x200f ||
			codeUnit === 0x2028 ||
			codeUnit === 0x2029 ||
			(codeUnit >= 0x202a && codeUnit <= 0x202e) ||
			(codeUnit >= 0x2066 && codeUnit <= 0x2069)
		) {
			return true;
		}
	}
	return false;
}
const REPORT_KEY = /^report-[a-f0-9]{64}$/;
const MAX_PLUGIN_VERSION_LENGTH = 128;
const PLUGIN_VERSION_PATTERN =
	/^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-(?:(?:0|[1-9]\d*)|(?:\d*[A-Za-z-][0-9A-Za-z-]*))(?:\.(?:(?:0|[1-9]\d*)|(?:\d*[A-Za-z-][0-9A-Za-z-]*)))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?![\s\S])/;
const MAX_SUPERVISOR_COMMAND_LENGTH = 4_096;

const START_OPTION_FIELDS = [
	"workspaceId",
	"repoPath",
	"coordinatorPaneId",
	"workerPrefix",
	"durationSeconds",
	"pollSeconds",
] as const;
const MANIFEST_REQUIRED_FIELDS = [
	"schemaVersion",
	"pluginVersion",
	"runId",
	"lifecycle",
	...START_OPTION_FIELDS,
	"createdAt",
	"updatedAt",
	"deadlineAt",
] as const;
const MANIFEST_OPTIONAL_FIELDS = [
	"supervisorTabId",
	"supervisorPaneId",
	"supervisorCommand",
	"stoppedAt",
	"lastError",
] as const;
const SNAPSHOT_REQUIRED_FIELDS = [
	"paneId",
	"workspaceId",
	"name",
	"status",
	"revision",
	"observedAt",
] as const;
const SNAPSHOT_OPTIONAL_FIELDS = ["taskTitle", "lastActivityAt"] as const;
const REPORT_FIELDS = [
	"key",
	"paneId",
	"workerName",
	"status",
	"revision",
	"path",
	"observedAt",
] as const;
const STATE_REQUIRED_FIELDS = [
	"schemaVersion",
	"runId",
	"updatedAt",
	"agents",
	"reports",
] as const;
const EVENT_BASE_FIELDS = [
	"schemaVersion",
	"runId",
	"timestamp",
	"type",
] as const;

export function isUnknownRecord(value: unknown): value is UnknownRecord {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readRecord(
	value: unknown,
	label: string,
	required: readonly string[],
	optional: readonly string[] = [],
): UnknownRecord {
	if (!isUnknownRecord(value)) {
		throw new ProtocolValidationError(`${label} must be an object`);
	}

	for (const field of required) {
		if (!Object.hasOwn(value, field)) {
			throw new ProtocolValidationError(`${label}.${field} is required`);
		}
	}

	for (const field of Object.keys(value)) {
		if (!required.includes(field) && !optional.includes(field)) {
			throw new ProtocolValidationError(`${label}.${field} is not recognized`);
		}
	}

	return value;
}

function assertBoundedText(
	value: unknown,
	label: string,
	maximumLength: number,
): asserts value is string {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > maximumLength ||
		containsControlCharacter(value)
	) {
		throw new ProtocolValidationError(
			`${label} must be non-empty metadata text`,
		);
	}
}

function assertOptionalBoundedText(
	value: unknown,
	label: string,
	maximumLength: number,
): asserts value is string | undefined {
	if (value !== undefined) {
		assertBoundedText(value, label, maximumLength);
	}
}

const DISPLAY_TASK_TITLE_MAX_LENGTH = 64;

/** Quote, bound, and path-neutralize untrusted task metadata for one-line output. */
export function formatTaskTitleForDisplay(value: string): string {
	assertBoundedText(value, "taskTitle", 512);
	let display = value;
	if (display.length > DISPLAY_TASK_TITLE_MAX_LENGTH) {
		let end = DISPLAY_TASK_TITLE_MAX_LENGTH - 3;
		const preceding = display.charCodeAt(end - 1);
		const following = display.charCodeAt(end);
		if (
			preceding >= 0xd800 &&
			preceding <= 0xdbff &&
			following >= 0xdc00 &&
			following <= 0xdfff
		) {
			end -= 1;
		}
		display = `${display.slice(0, end)}...`;
	}
	const quoted = JSON.stringify(display);
	return quoted.includes("/") ? quoted.replaceAll("/", "\\u002f") : quoted;
}

function assertIntegerInRange(
	value: unknown,
	label: string,
	minimum: number,
	maximum: number,
): asserts value is number {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value < minimum ||
		value > maximum
	) {
		throw new ProtocolValidationError(
			`${label} must be an integer from ${minimum} through ${maximum}`,
		);
	}
}

export function assertSchemaVersion(
	value: unknown,
): asserts value is typeof SCHEMA_VERSION {
	if (value !== SCHEMA_VERSION) {
		throw new ProtocolValidationError(
			`unsupported schemaVersion; expected ${SCHEMA_VERSION}`,
		);
	}
}

export function assertPluginVersion(value: unknown): asserts value is string {
	if (
		typeof value !== "string" ||
		value.length > MAX_PLUGIN_VERSION_LENGTH ||
		containsControlCharacter(value) ||
		!PLUGIN_VERSION_PATTERN.test(value)
	) {
		throw new ProtocolValidationError(
			"pluginVersion must be a valid semantic version",
		);
	}
}

export function assertRunLifecycle(
	value: unknown,
): asserts value is RunLifecycle {
	switch (value) {
		case "starting":
		case "running":
		case "stopping":
		case "stopped":
		case "completed":
		case "failed":
			return;
		default:
			throw new ProtocolValidationError("invalid run lifecycle");
	}
}

export function assertAgentStatus(
	value: unknown,
): asserts value is AgentStatus {
	switch (value) {
		case "idle":
		case "working":
		case "blocked":
		case "done":
		case "exited":
		case "unknown":
			return;
		default:
			throw new ProtocolValidationError("invalid agent status");
	}
}

export function normalizeAgentStatus(value: unknown): AgentStatus {
	if (typeof value !== "string") {
		return "unknown";
	}

	switch (value.trim().toLowerCase()) {
		case "idle":
		case "waiting":
			return "idle";
		case "working":
		case "active":
		case "busy":
		case "running":
			return "working";
		case "blocked":
		case "stuck":
			return "blocked";
		case "done":
		case "complete":
		case "completed":
			return "done";
		case "exited":
		case "dead":
		case "stopped":
			return "exited";
		default:
			return "unknown";
	}
}

export function isHarvestStatus(status: AgentStatus): status is HarvestStatus {
	return status === "blocked" || status === "done";
}

export function isTerminalLifecycle(lifecycle: RunLifecycle): boolean {
	return (
		lifecycle === "stopped" ||
		lifecycle === "completed" ||
		lifecycle === "failed"
	);
}

export function assertOpaqueId(
	value: unknown,
	label = "identifier",
): asserts value is string {
	assertBoundedText(value, label, 512);
	if (value.trim() !== value) {
		throw new ProtocolValidationError(
			`${label} must not have surrounding whitespace`,
		);
	}
}

export function assertWorkerPrefix(value: unknown): asserts value is string {
	if (typeof value !== "string" || !RUN_ID.test(value)) {
		throw new ProtocolValidationError("workerPrefix is not protocol-safe");
	}
}

export function assertRunId(value: unknown): asserts value is string {
	if (typeof value !== "string" || !RUN_ID.test(value)) {
		throw new ProtocolValidationError("runId is not filesystem-safe");
	}
}

export function assertIsoTimestamp(
	value: unknown,
	label = "timestamp",
): asserts value is string {
	if (
		typeof value !== "string" ||
		!ISO_TIMESTAMP.test(value) ||
		!Number.isFinite(Date.parse(value))
	) {
		throw new ProtocolValidationError(`${label} must be an ISO timestamp`);
	}
}

export function assertSafeRepoPath(
	value: unknown,
	label = "repoPath",
): asserts value is string {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		containsControlCharacter(value)
	) {
		throw new ProtocolValidationError(`${label} must be an absolute path`);
	}
	if (
		!isAbsolute(value) ||
		normalize(value) !== value ||
		resolve(value) !== value
	) {
		throw new ProtocolValidationError(
			`${label} must be an absolute normalized path`,
		);
	}

	const root = parse(value).root;
	if (value === root || value === resolve(homedir())) {
		throw new ProtocolValidationError(
			`${label} must not be the filesystem root or home`,
		);
	}
}

export function assertStartOptions(
	value: unknown,
): asserts value is StartOptions {
	const record = readRecord(value, "startOptions", START_OPTION_FIELDS);
	assertOpaqueId(record["workspaceId"], "startOptions.workspaceId");
	assertSafeRepoPath(record["repoPath"], "startOptions.repoPath");
	assertOpaqueId(record["coordinatorPaneId"], "startOptions.coordinatorPaneId");
	assertWorkerPrefix(record["workerPrefix"]);
	assertIntegerInRange(
		record["durationSeconds"],
		"startOptions.durationSeconds",
		60 * 60,
		24 * 60 * 60,
	);
	assertIntegerInRange(
		record["pollSeconds"],
		"startOptions.pollSeconds",
		15,
		600,
	);
}

export function assertRunManifest(
	value: unknown,
): asserts value is RunManifest {
	const record = readRecord(
		value,
		"manifest",
		MANIFEST_REQUIRED_FIELDS,
		MANIFEST_OPTIONAL_FIELDS,
	);
	assertSchemaVersion(record["schemaVersion"]);
	assertPluginVersion(record["pluginVersion"]);
	assertRunId(record["runId"]);
	assertRunLifecycle(record["lifecycle"]);

	const startOptions: unknown = {
		workspaceId: record["workspaceId"],
		repoPath: record["repoPath"],
		coordinatorPaneId: record["coordinatorPaneId"],
		workerPrefix: record["workerPrefix"],
		durationSeconds: record["durationSeconds"],
		pollSeconds: record["pollSeconds"],
	};
	assertStartOptions(startOptions);

	if (record["supervisorTabId"] !== undefined) {
		assertOpaqueId(record["supervisorTabId"], "manifest.supervisorTabId");
	}
	if (record["supervisorPaneId"] !== undefined) {
		assertOpaqueId(record["supervisorPaneId"], "manifest.supervisorPaneId");
	}
	assertOptionalBoundedText(
		record["supervisorCommand"],
		"manifest.supervisorCommand",
		MAX_SUPERVISOR_COMMAND_LENGTH,
	);
	const hasSupervisorTabId = record["supervisorTabId"] !== undefined;
	const hasSupervisorPaneId = record["supervisorPaneId"] !== undefined;
	const hasSupervisorCommand = record["supervisorCommand"] !== undefined;
	if (
		hasSupervisorTabId !== hasSupervisorPaneId ||
		hasSupervisorTabId !== hasSupervisorCommand
	) {
		throw new ProtocolValidationError(
			"manifest supervisorTabId, supervisorPaneId, and supervisorCommand must be set together",
		);
	}

	assertIsoTimestamp(record["createdAt"], "manifest.createdAt");
	assertIsoTimestamp(record["updatedAt"], "manifest.updatedAt");
	assertIsoTimestamp(record["deadlineAt"], "manifest.deadlineAt");
	if (Date.parse(record["updatedAt"]) < Date.parse(record["createdAt"])) {
		throw new ProtocolValidationError("manifest.updatedAt precedes createdAt");
	}
	const expectedDeadline =
		Date.parse(record["createdAt"]) + startOptions.durationSeconds * 1_000;
	if (Date.parse(record["deadlineAt"]) !== expectedDeadline) {
		throw new ProtocolValidationError(
			"manifest.deadlineAt must equal createdAt plus durationSeconds",
		);
	}

	if (record["stoppedAt"] !== undefined) {
		assertIsoTimestamp(record["stoppedAt"], "manifest.stoppedAt");
		if (Date.parse(record["stoppedAt"]) < Date.parse(record["createdAt"])) {
			throw new ProtocolValidationError(
				"manifest.stoppedAt precedes createdAt",
			);
		}
	}
	assertOptionalBoundedText(record["lastError"], "manifest.lastError", 4_096);
}

export function parseRunManifest(value: unknown): RunManifest {
	assertRunManifest(value);
	return value;
}

export function assertAgentSnapshot(
	value: unknown,
): asserts value is AgentSnapshot {
	const record = readRecord(
		value,
		"agent",
		SNAPSHOT_REQUIRED_FIELDS,
		SNAPSHOT_OPTIONAL_FIELDS,
	);
	assertOpaqueId(record["paneId"], "agent.paneId");
	assertOpaqueId(record["workspaceId"], "agent.workspaceId");
	assertBoundedText(record["name"], "agent.name", 512);
	assertAgentStatus(record["status"]);
	assertBoundedText(record["revision"], "agent.revision", 512);
	assertIsoTimestamp(record["observedAt"], "agent.observedAt");
	assertOptionalBoundedText(record["taskTitle"], "agent.taskTitle", 512);
	if (record["lastActivityAt"] === undefined) {
		record["lastActivityAt"] = record["observedAt"];
	} else {
		assertIsoTimestamp(record["lastActivityAt"], "agent.lastActivityAt");
	}
}

export function parseAgentSnapshot(value: unknown): AgentSnapshot {
	assertAgentSnapshot(value);
	return value;
}

export function assertReportRelativePath(
	value: unknown,
): asserts value is string {
	if (typeof value !== "string" || !REPORT_PATH.test(value)) {
		throw new ProtocolValidationError(
			"report.path must be a safe relative path directly under reports/",
		);
	}
}

export function assertReportRecord(
	value: unknown,
): asserts value is ReportRecord {
	const record = readRecord(value, "report", REPORT_FIELDS);
	if (typeof record["key"] !== "string" || !REPORT_KEY.test(record["key"])) {
		throw new ProtocolValidationError("report.key is invalid");
	}
	assertOpaqueId(record["paneId"], "report.paneId");
	assertBoundedText(record["workerName"], "report.workerName", 512);
	assertAgentStatus(record["status"]);
	if (!isHarvestStatus(record["status"])) {
		throw new ProtocolValidationError(
			"reports are allowed only for blocked or done agents",
		);
	}
	assertBoundedText(record["revision"], "report.revision", 512);
	assertReportRelativePath(record["path"]);
	assertIsoTimestamp(record["observedAt"], "report.observedAt");

	const expectedKey = reportKey(
		record["paneId"],
		record["revision"],
		record["status"],
	);
	if (record["key"] !== expectedKey) {
		throw new ProtocolValidationError(
			"report.key does not match its pane, revision, and status",
		);
	}
	const expectedPath = reportRelativePath(
		record["paneId"],
		record["workerName"],
		record["revision"],
		record["status"],
	);
	if (record["path"] !== expectedPath) {
		throw new ProtocolValidationError(
			"report.path is not the canonical safe report path",
		);
	}
}

export function parseReportRecord(value: unknown): ReportRecord {
	assertReportRecord(value);
	return value;
}

export function assertRunState(value: unknown): asserts value is RunState {
	const record = readRecord(value, "state", STATE_REQUIRED_FIELDS, [
		"noticeCursor",
	]);
	assertSchemaVersion(record["schemaVersion"]);
	assertRunId(record["runId"]);
	assertIsoTimestamp(record["updatedAt"], "state.updatedAt");

	const agentsValue: unknown = record["agents"];
	if (!Array.isArray(agentsValue)) {
		throw new ProtocolValidationError("state.agents must be an array");
	}
	const paneIds = new Set<string>();
	for (const element of agentsValue) {
		const agent: unknown = element;
		assertAgentSnapshot(agent);
		if (paneIds.has(agent.paneId)) {
			throw new ProtocolValidationError(
				"state.agents contains a duplicate paneId",
			);
		}
		paneIds.add(agent.paneId);
	}

	const reportsValue: unknown = record["reports"];
	if (!Array.isArray(reportsValue)) {
		throw new ProtocolValidationError("state.reports must be an array");
	}
	if (reportsValue.length > REPORT_LIMIT) {
		throw new ProtocolValidationError(
			`state.reports must contain at most ${REPORT_LIMIT} records`,
		);
	}
	const reportKeys = new Set<string>();
	const reportPaths = new Set<string>();
	for (const element of reportsValue) {
		const report: unknown = element;
		assertReportRecord(report);
		if (reportKeys.has(report.key)) {
			throw new ProtocolValidationError(
				"state.reports contains a duplicate key",
			);
		}
		if (reportPaths.has(report.path)) {
			throw new ProtocolValidationError(
				"state.reports contains a duplicate path",
			);
		}
		reportKeys.add(report.key);
		reportPaths.add(report.path);
	}

	if (record["noticeCursor"] !== undefined) {
		assertIntegerInRange(
			record["noticeCursor"],
			"state.noticeCursor",
			0,
			Number.MAX_SAFE_INTEGER,
		);
	}
}

export function parseRunState(value: unknown): RunState {
	assertRunState(value);
	return value;
}

export function assertRunEvent(value: unknown): asserts value is RunEvent {
	if (!isUnknownRecord(value)) {
		throw new ProtocolValidationError("event must be an object");
	}
	const eventType: unknown = value["type"];
	let record: UnknownRecord;

	switch (eventType) {
		case "lifecycle":
			record = readRecord(
				value,
				"event",
				[...EVENT_BASE_FIELDS, "lifecycle"],
				["lastError"],
			);
			assertRunLifecycle(record["lifecycle"]);
			assertOptionalBoundedText(record["lastError"], "event.lastError", 4_096);
			break;
		case "agent":
			record = readRecord(
				value,
				"event",
				[...EVENT_BASE_FIELDS, "agent", "outcome"],
				["lastError"],
			);
			assertAgentSnapshot(record["agent"]);
			if (
				record["outcome"] !== "observed" &&
				record["outcome"] !== "readFailed"
			) {
				throw new ProtocolValidationError("event.outcome is invalid");
			}
			assertOptionalBoundedText(record["lastError"], "event.lastError", 4_096);
			if (
				record["outcome"] === "readFailed" &&
				record["lastError"] === undefined
			) {
				throw new ProtocolValidationError(
					"a readFailed event requires lastError metadata",
				);
			}
			break;
		case "report":
			record = readRecord(value, "event", [...EVENT_BASE_FIELDS, "report"]);
			assertReportRecord(record["report"]);
			break;
		default:
			throw new ProtocolValidationError("event.type is invalid");
	}

	assertSchemaVersion(record["schemaVersion"]);
	assertRunId(record["runId"]);
	assertIsoTimestamp(record["timestamp"], "event.timestamp");
}

export function parseRunEvent(value: unknown): RunEvent {
	assertRunEvent(value);
	return value;
}

export function generateRunId(now = new Date()): string {
	const timestamp = now.toISOString().replace(/[-:.]/g, "");
	return `${timestamp}-${randomBytes(16).toString("hex")}`;
}

export function reportKey(
	paneId: string,
	revision: string,
	status: HarvestStatus,
): string {
	assertOpaqueId(paneId, "paneId");
	assertBoundedText(revision, "revision", 512);
	if (!isHarvestStatus(status)) {
		throw new ProtocolValidationError("report status must be blocked or done");
	}
	const digest = createHash("sha256")
		.update(JSON.stringify([paneId, revision, status]))
		.digest("hex");
	return `report-${digest}`;
}

export function agentHandle(paneId: string): string {
	assertOpaqueId(paneId, "paneId");
	const digest = createHash("sha256").update(paneId).digest("hex").slice(0, 12);
	return `agent-${digest}`;
}

export function reportRelativePath(
	paneId: string,
	workerName: string,
	revision: string,
	status: HarvestStatus,
): string {
	assertBoundedText(workerName, "workerName", 512);
	const handle = agentHandle(paneId);
	const key = reportKey(paneId, revision, status);
	return `reports/${handle}-${key}.txt`;
}
