import { createHash } from "node:crypto";
import { isAbsolute } from "node:path";

import {
	type AgentStatus,
	assertOpaqueId as assertProtocolOpaqueId,
	containsControlCharacter,
	isUnknownRecord,
	normalizeAgentStatus,
} from "./types";

const HERDR_BINARY = "herdr";
const DEFAULT_TIMEOUT_MS = 15_000;
const DEFAULT_MAX_OUTPUT_BYTES = 1_048_576;
const MAX_OUTPUT_BYTES = 16_777_216;
const DEFAULT_READ_LINES = 200;
const MAX_READ_LINES = 10_000;
const MAX_PANE_COMMAND_LENGTH = 4_096;

function containsForbiddenArgumentCharacter(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const codeUnit = value.charCodeAt(index);
		if (codeUnit === 0 || codeUnit === 0x0a || codeUnit === 0x0d) {
			return true;
		}
	}
	return false;
}

export interface CommandOptions {
	readonly cwd?: string;
	readonly env?: Readonly<Record<string, string>>;
	readonly timeoutMs?: number;
	readonly maxOutputBytes?: number;
}

export interface CommandResult {
	readonly stdout: string;
	readonly stderr: string;
	readonly exitCode: number;
	readonly timedOut?: boolean;
	readonly stdoutTruncated?: boolean;
	readonly stderrTruncated?: boolean;
}

export type CommandRunner = (
	command: string,
	args: readonly string[],
	options?: CommandOptions,
) => Promise<CommandResult>;

export interface HerdrAgent {
	readonly paneId: string;
	readonly workspaceId: string;
	readonly name: string;
	readonly status: AgentStatus;
	readonly revision: string;
}

export interface CreatedSupervisorTab {
	readonly tabId: string;
	readonly paneId: string;
}
export interface PaneProcessInfo {
	readonly command: string | undefined;
}

export interface CreateSupervisorTabInput {
	readonly workspaceId: string;
	readonly cwd: string;
	readonly label: string;
	readonly env?: Readonly<Record<string, string>>;
}

export class CommandRunnerError extends Error {
	readonly result: CommandResult | undefined;

	constructor(message: string, result?: CommandResult) {
		super(message);
		this.name = "CommandRunnerError";
		this.result = result;
	}
}

export class HerdrServerError extends Error {
	readonly code: string;

	constructor(code: string, message: string) {
		super(`Herdr server error (${code}): ${message}`);
		this.name = "HerdrServerError";
		this.code = code;
	}
}

export class HerdrAdapterError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "HerdrAdapterError";
	}
}

interface CapturedOutput {
	readonly text: string;
	readonly truncated: boolean;
}

function commandOptionInteger(
	value: number | undefined,
	fallback: number,
	maximum: number,
): number {
	const resolved = value ?? fallback;
	if (!Number.isSafeInteger(resolved) || resolved < 1 || resolved > maximum) {
		throw new CommandRunnerError("Command runner options are invalid");
	}
	return resolved;
}

function boundedHerdrTimeout(timeoutMs: number | undefined): number {
	if (timeoutMs === undefined) return DEFAULT_TIMEOUT_MS;
	if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) {
		throw new HerdrAdapterError("Herdr command timeout is invalid");
	}
	return Math.min(timeoutMs, DEFAULT_TIMEOUT_MS);
}

function assertProcessArgument(value: string): void {
	if (containsForbiddenArgumentCharacter(value)) {
		throw new CommandRunnerError(
			"Command argument contains a forbidden control character",
		);
	}
}

async function captureBounded(
	stream: ReadableStream<Uint8Array>,
	maximumBytes: number,
): Promise<CapturedOutput> {
	const reader = stream.getReader();
	const chunks: Uint8Array[] = [];
	let retainedBytes = 0;
	let observedBytes = 0;

	try {
		for (;;) {
			const { done, value } = await reader.read();
			if (done) break;
			observedBytes += value.byteLength;

			const remaining = maximumBytes - retainedBytes;
			if (remaining > 0) {
				const length = Math.min(remaining, value.byteLength);
				chunks.push(value.slice(0, length));
				retainedBytes += length;
			}
		}
	} finally {
		reader.releaseLock();
	}

	const bytes = new Uint8Array(retainedBytes);
	let offset = 0;
	for (const chunk of chunks) {
		bytes.set(chunk, offset);
		offset += chunk.byteLength;
	}

	return {
		text: new TextDecoder().decode(bytes),
		truncated: observedBytes > retainedBytes,
	};
}

/**
 * Default no-shell command runner. Its diagnostics intentionally omit the
 * executable, argv, cwd, environment, and captured output.
 */
export const bunCommandRunner: CommandRunner = async (
	command,
	args,
	options = {},
) => {
	assertProcessArgument(command);
	for (const argument of args) assertProcessArgument(argument);
	if (options.cwd !== undefined) assertProcessArgument(options.cwd);
	if (options.env !== undefined) {
		for (const [key, value] of Object.entries(options.env)) {
			assertProcessArgument(key);
			assertProcessArgument(value);
		}
	}

	const timeoutMs = commandOptionInteger(
		options.timeoutMs,
		DEFAULT_TIMEOUT_MS,
		2_147_483_647,
	);
	const maximumBytes = commandOptionInteger(
		options.maxOutputBytes,
		DEFAULT_MAX_OUTPUT_BYTES,
		MAX_OUTPUT_BYTES,
	);

	const env =
		options.env === undefined
			? undefined
			: Object.assign({}, process.env, options.env);

	let subprocess: Bun.Subprocess<"ignore", "pipe", "pipe">;
	try {
		subprocess = Bun.spawn([command, ...args], {
			...(options.cwd === undefined ? {} : { cwd: options.cwd }),
			...(env === undefined ? {} : { env }),
			stdin: "ignore",
			stdout: "pipe",
			stderr: "pipe",
		});
	} catch {
		throw new CommandRunnerError("Command could not be started");
	}

	let timedOut = false;
	const timeoutTimer = setTimeout(() => {
		timedOut = true;
		try {
			subprocess.kill("SIGKILL");
		} catch {
			// The process may have exited between the timer firing and the signal.
		}
	}, timeoutMs);

	let stdout: CapturedOutput;
	let stderr: CapturedOutput;
	let exitCode: number;
	try {
		[stdout, stderr, exitCode] = await Promise.all([
			captureBounded(
				subprocess.stdout as ReadableStream<Uint8Array>,
				maximumBytes,
			),
			captureBounded(
				subprocess.stderr as ReadableStream<Uint8Array>,
				maximumBytes,
			),
			subprocess.exited,
		]);
	} catch {
		try {
			subprocess.kill("SIGKILL");
		} catch {
			// The process has already exited.
		}
		throw new CommandRunnerError("Command execution failed");
	} finally {
		clearTimeout(timeoutTimer);
	}

	const result: CommandResult = {
		stdout: stdout.text,
		stderr: stderr.text,
		exitCode,
		timedOut,
		stdoutTruncated: stdout.truncated,
		stderrTruncated: stderr.truncated,
	};

	if (timedOut) {
		throw new CommandRunnerError(
			`Command timed out after ${timeoutMs}ms`,
			result,
		);
	}
	if (exitCode !== 0) {
		throw new CommandRunnerError(
			`Command failed with exit code ${exitCode}`,
			result,
		);
	}
	return result;
};

function assertShellValue(value: string): void {
	if (containsForbiddenArgumentCharacter(value)) {
		throw new HerdrAdapterError(
			"Shell argument contains a forbidden control character",
		);
	}
}

/** Quote one POSIX shell word. This is only for constructing the pane command. */
export function shellQuoteArg(value: string): string {
	assertShellValue(value);
	return `'${value.replaceAll("'", `'"'"'`)}'`;
}

/** Build the one command-string argument accepted by `herdr pane run`. */
export function buildPaneCommand(
	executable: string,
	args: readonly string[],
): string {
	if (executable.length === 0) {
		throw new HerdrAdapterError("Pane command executable is required");
	}
	return [shellQuoteArg(executable), ...args.map(shellQuoteArg)].join(" ");
}
function normalizedPaneCommand(
	value: unknown,
	expectedPaneId: string,
): PaneProcessInfo {
	if (!isUnknownRecord(value) || value["pane_id"] !== expectedPaneId) {
		throw new HerdrAdapterError(
			"Herdr process response did not match the requested pane",
		);
	}

	const processes = value["foreground_processes"];
	if (!Array.isArray(processes) || processes.length !== 1) {
		return { command: undefined };
	}
	const process = processes[0];
	if (!isUnknownRecord(process)) return { command: undefined };
	if (
		!Number.isSafeInteger(process["pid"]) ||
		(process["pid"] as number) < 1 ||
		typeof process["name"] !== "string" ||
		process["name"].length === 0
	) {
		return { command: undefined };
	}

	const argv = process["argv"];
	if (!Array.isArray(argv) || argv.length === 0) {
		return { command: undefined };
	}
	let rawLength = 0;
	for (const argument of argv) {
		if (typeof argument !== "string") return { command: undefined };
		rawLength += argument.length;
		if (rawLength > MAX_PANE_COMMAND_LENGTH) {
			return { command: undefined };
		}
	}

	try {
		const command = buildPaneCommand(
			argv[0] as string,
			argv.slice(1) as string[],
		);
		return command.length <= MAX_PANE_COMMAND_LENGTH
			? { command }
			: { command: undefined };
	} catch {
		return { command: undefined };
	}
}

/** Require a complete, canonical process argv and an exact command match. */
export function paneProcessOwnsCommand(
	processInfo: PaneProcessInfo,
	expectedCommand: string,
): boolean {
	return (
		expectedCommand.length > 0 &&
		expectedCommand.length <= MAX_PANE_COMMAND_LENGTH &&
		!containsControlCharacter(expectedCommand) &&
		processInfo.command === expectedCommand
	);
}

type JsonRecord = Record<string, unknown>;

function own(record: JsonRecord, key: string): boolean {
	return Object.hasOwn(record, key);
}

function replaceControlCharactersWithSpaces(value: string): string {
	let pieces: string[] | undefined;
	let segmentStart = 0;
	for (let index = 0; index < value.length; index += 1) {
		const codeUnit = value.charCodeAt(index);
		if (codeUnit > 0x1f && codeUnit !== 0x7f) continue;
		pieces ??= [];
		pieces.push(value.slice(segmentStart, index), " ");
		segmentStart = index + 1;
	}
	if (pieces === undefined) return value;
	pieces.push(value.slice(segmentStart));
	return pieces.join("");
}

function conciseText(
	value: string,
	redactions: readonly string[] = [],
): string {
	let result = value;
	for (const secret of [...redactions].sort((a, b) => b.length - a.length)) {
		if (secret.length > 0) result = result.split(secret).join("[redacted]");
	}
	result = replaceControlCharactersWithSpaces(result)
		.replace(/\s+/g, " ")
		.trim();
	if (result.length === 0) return "request failed";
	return result.length <= 240 ? result : `${result.slice(0, 237)}...`;
}

function errorCode(value: unknown): string {
	if (typeof value !== "string" || !/^[A-Za-z0-9_.:-]{1,80}$/.test(value)) {
		return "server_error";
	}
	return value;
}

/** Decode a Herdr `{ id, result }` or `{ id, error }` JSON response. */
export function decodeHerdrEnvelope(
	text: string,
	redactions: readonly string[] = [],
): unknown {
	let decoded: unknown;
	try {
		decoded = JSON.parse(text.trim());
	} catch {
		throw new HerdrAdapterError("Herdr returned invalid JSON");
	}
	if (!isUnknownRecord(decoded)) {
		throw new HerdrAdapterError("Herdr returned an invalid response envelope");
	}

	if (own(decoded, "error")) {
		const body = decoded.error;
		const record: JsonRecord = isUnknownRecord(body) ? body : {};
		const rawCode = errorCode(record.code);
		const code = redactions.some(
			(secret) => secret.length > 0 && rawCode.includes(secret),
		)
			? "server_error"
			: rawCode;
		const message = conciseText(
			typeof record.message === "string" ? record.message : "request failed",
			redactions,
		);
		throw new HerdrServerError(code, message);
	}
	if (!own(decoded, "result")) {
		throw new HerdrAdapterError("Herdr returned an invalid response envelope");
	}
	return decoded.result;
}

function stringField(
	record: JsonRecord,
	keys: readonly string[],
): string | undefined {
	for (const key of keys) {
		const value = record[key];
		if (typeof value === "string" && value.length > 0) return value;
	}
	return undefined;
}

function scalarField(
	records: readonly JsonRecord[],
	keys: readonly string[],
): string | undefined {
	for (const record of records) {
		for (const key of keys) {
			const value = record[key];
			if (typeof value === "string" && value.length > 0) return value;
			if (typeof value === "number" && Number.isFinite(value)) {
				return String(value);
			}
		}
	}
	return undefined;
}

function recordField(
	record: JsonRecord,
	keys: readonly string[],
): JsonRecord | undefined {
	for (const key of keys) {
		const value = record[key];
		if (isUnknownRecord(value)) return value;
	}
	return undefined;
}

function assertOpaqueId(value: string, label: string): string {
	try {
		assertProtocolOpaqueId(value, label);
	} catch {
		throw new HerdrAdapterError(`${label} is invalid`);
	}
	return value;
}

function stableFingerprint(value: string): string {
	return createHash("sha256").update(value).digest("hex");
}

function revisionPart(value: string): string {
	const encoded = encodeURIComponent(value);
	return encoded.length <= 160 ? encoded : `hash:${stableFingerprint(value)}`;
}

function deriveRevision(agent: JsonRecord, status: AgentStatus): string {
	const lifecycle = recordField(agent, ["lifecycle", "agent_lifecycle"]);
	const state = recordField(agent, ["state", "agent_state"]);
	const session = recordField(agent, [
		"agent_session",
		"agentSession",
		"session",
	]);
	const metadataRecords = [lifecycle, state, agent].filter(
		(value): value is JsonRecord => value !== undefined,
	);

	const sequence = scalarField(metadataRecords, [
		"state_change_seq",
		"stateChangeSeq",
		"status_sequence",
		"statusSequence",
		"lifecycle_sequence",
		"lifecycleSequence",
		"sequence",
		"seq",
	]);
	const lifecycleRevision = scalarField(metadataRecords, [
		"lifecycle_revision",
		"lifecycleRevision",
		"status_revision",
		"statusRevision",
	]);
	const directRevision = scalarField([agent], ["revision"]);

	let sessionFingerprint: string | undefined;
	if (session !== undefined) {
		const sessionParts = [
			scalarField([session], ["source"]),
			scalarField([session], ["agent"]),
			scalarField([session], ["kind"]),
			scalarField(
				[session],
				[
					"value",
					"id",
					"session_id",
					"sessionId",
					"path",
					"session_path",
					"sessionPath",
				],
			),
		].filter((value): value is string => value !== undefined);
		if (sessionParts.length > 0) {
			sessionFingerprint = stableFingerprint(sessionParts.join("\u001f"));
		}
	}

	const revision = sequence ?? lifecycleRevision ?? directRevision;
	if (sessionFingerprint !== undefined && revision !== undefined) {
		return `session:${sessionFingerprint}:sequence:${revisionPart(revision)}`;
	}
	if (revision !== undefined) return revisionPart(revision);
	if (sessionFingerprint !== undefined) return `session:${sessionFingerprint}`;
	return `status:${status}`;
}

function statusValue(agent: JsonRecord): unknown {
	for (const key of ["agent_status", "agentStatus", "status"]) {
		if (own(agent, key)) return agent[key];
	}
	const lifecycle = recordField(agent, ["lifecycle", "agent_lifecycle"]);
	if (lifecycle !== undefined) {
		for (const key of ["status", "state", "agent_status", "agentStatus"]) {
			if (own(lifecycle, key)) return lifecycle[key];
		}
	}
	const state = recordField(agent, ["state", "agent_state"]);
	return state === undefined ? undefined : (state.status ?? state.state);
}

export function normalizeHerdrAgent(value: unknown): HerdrAgent {
	if (!isUnknownRecord(value)) {
		throw new HerdrAdapterError("Herdr returned an invalid agent record");
	}

	const paneId = stringField(value, ["pane_id", "paneId"]);
	const workspaceId = stringField(value, ["workspace_id", "workspaceId"]);
	if (paneId === undefined || workspaceId === undefined) {
		throw new HerdrAdapterError("Herdr agent record is missing an opaque ID");
	}
	assertOpaqueId(paneId, "Agent pane ID");
	assertOpaqueId(workspaceId, "Agent workspace ID");

	const name =
		stringField(value, [
			"name",
			"agent_name",
			"agentName",
			"label",
			"display_agent",
			"displayAgent",
			"agent",
		]) ?? paneId;
	const status = normalizeAgentStatus(statusValue(value));

	return {
		paneId,
		workspaceId,
		name: conciseText(name),
		status,
		revision: deriveRevision(value, status),
	};
}

function agentArray(value: unknown, depth = 0): unknown[] | undefined {
	if (depth > 4) return undefined;
	if (Array.isArray(value)) return value;
	if (!isUnknownRecord(value)) return undefined;

	for (const key of ["agents", "items"]) {
		const candidate = value[key];
		if (Array.isArray(candidate)) return candidate;
	}
	for (const key of ["data", "payload", "result", "list"]) {
		if (!own(value, key)) continue;
		const nested = agentArray(value[key], depth + 1);
		if (nested !== undefined) return nested;
	}
	return undefined;
}

export function normalizeHerdrAgents(value: unknown): HerdrAgent[] {
	const records = agentArray(value);
	if (records === undefined) {
		throw new HerdrAdapterError("Herdr response did not contain an agent list");
	}
	return records.map(normalizeHerdrAgent);
}

function structuredErrorFromResult(
	result: CommandResult,
	redactions: readonly string[],
	includeStdout: boolean,
): HerdrServerError | undefined {
	const outputs = includeStdout
		? [result.stderr, result.stdout]
		: [result.stderr];
	for (const output of outputs) {
		if (output.trim().length === 0) continue;
		try {
			decodeHerdrEnvelope(output, redactions);
		} catch (error) {
			if (error instanceof HerdrServerError) return error;
		}
	}
	return undefined;
}

function resultRecord(value: unknown): JsonRecord {
	if (!isUnknownRecord(value)) {
		throw new HerdrAdapterError("Herdr returned an invalid result");
	}
	return value;
}

function returnedId(
	container: JsonRecord,
	nestedKeys: readonly string[],
	idKeys: readonly string[],
): string | undefined {
	const direct = stringField(container, idKeys);
	if (direct !== undefined) return direct;
	const nested = recordField(container, nestedKeys);
	return nested === undefined ? undefined : stringField(nested, idKeys);
}

function validateEnvironment(
	env: Readonly<Record<string, string>> | undefined,
	workspaceId: string,
): Array<readonly [string, string]> {
	const values = new Map<string, string>();
	for (const [key, value] of Object.entries(env ?? {})) {
		if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
			throw new HerdrAdapterError("Invalid environment variable name");
		}
		if (containsForbiddenArgumentCharacter(value)) {
			throw new HerdrAdapterError(
				"Environment value contains a forbidden control character",
			);
		}
		values.set(key, value);
	}

	for (const [key, required] of [
		["HERDR_ENV", "1"],
		["HERDR_WORKSPACE_ID", workspaceId],
	] as const) {
		const supplied = values.get(key);
		if (supplied !== undefined && supplied !== required) {
			throw new HerdrAdapterError(
				"Reserved Herdr environment override is not allowed",
			);
		}
		values.set(key, required);
	}

	return [...values.entries()].sort(([left], [right]) => {
		if (left < right) return -1;
		if (left > right) return 1;
		return 0;
	});
}

export class HerdrClient {
	readonly #runner: CommandRunner;

	constructor(runner: CommandRunner = bunCommandRunner) {
		this.#runner = runner;
	}

	async #execute(
		args: readonly string[],
		workspaceId?: string,
		redactions: readonly string[] = [],
		includeStdoutErrors = true,
		timeoutMs?: number,
	): Promise<CommandResult> {
		for (const argument of args) {
			if (containsForbiddenArgumentCharacter(argument)) {
				throw new HerdrAdapterError(
					"Herdr argument contains a forbidden control character",
				);
			}
		}

		const boundedTimeoutMs = boundedHerdrTimeout(timeoutMs);
		const options: CommandOptions =
			workspaceId === undefined
				? { timeoutMs: boundedTimeoutMs }
				: {
						env: { HERDR_WORKSPACE_ID: workspaceId },
						timeoutMs: boundedTimeoutMs,
					};

		let result: CommandResult;
		try {
			result = await this.#runner(HERDR_BINARY, args, options);
		} catch (error) {
			if (error instanceof HerdrServerError) throw error;
			if (error instanceof CommandRunnerError && error.result !== undefined) {
				if (error.result.timedOut) {
					throw new HerdrAdapterError("Herdr command timed out");
				}
				const serverError = structuredErrorFromResult(
					error.result,
					redactions,
					includeStdoutErrors,
				);
				if (serverError !== undefined) throw serverError;
				throw new HerdrAdapterError(
					`Herdr command failed (exit ${error.result.exitCode})`,
				);
			}
			throw new HerdrAdapterError("Herdr command could not be executed");
		}

		if (result.timedOut) throw new HerdrAdapterError("Herdr command timed out");
		if (!Number.isInteger(result.exitCode) || result.exitCode !== 0) {
			const serverError = structuredErrorFromResult(
				result,
				redactions,
				includeStdoutErrors,
			);
			if (serverError !== undefined) throw serverError;
			const code = Number.isInteger(result.exitCode)
				? result.exitCode
				: "unknown";
			throw new HerdrAdapterError(`Herdr command failed (exit ${code})`);
		}
		return result;
	}

	async #json(
		args: readonly string[],
		workspaceId?: string,
		redactions: readonly string[] = [],
		timeoutMs?: number,
	): Promise<unknown> {
		const result = await this.#execute(
			args,
			workspaceId,
			redactions,
			true,
			timeoutMs,
		);
		if (result.stdoutTruncated) {
			throw new HerdrAdapterError("Herdr response exceeded the output limit");
		}
		return decodeHerdrEnvelope(result.stdout, redactions);
	}

	async assertAvailable(): Promise<void> {
		try {
			await this.#execute(["--version"]);
		} catch {
			throw new HerdrAdapterError("Herdr CLI is unavailable");
		}
	}

	async listAgents(
		workspaceId: string,
		timeoutMs?: number,
	): Promise<HerdrAgent[]> {
		assertOpaqueId(workspaceId, "Workspace ID");
		const result = await this.#json(
			["agent", "list"],
			workspaceId,
			undefined,
			timeoutMs,
		);
		return normalizeHerdrAgents(result).filter(
			(agent) => agent.workspaceId === workspaceId,
		);
	}

	async createSupervisorTab(
		input: CreateSupervisorTabInput,
	): Promise<CreatedSupervisorTab> {
		const workspaceId = assertOpaqueId(input.workspaceId, "Workspace ID");
		if (!isAbsolute(input.cwd)) {
			throw new HerdrAdapterError("Supervisor cwd must be absolute");
		}
		if (containsForbiddenArgumentCharacter(input.cwd)) {
			throw new HerdrAdapterError(
				"Supervisor cwd contains a forbidden control character",
			);
		}
		if (input.label.trim().length === 0) {
			throw new HerdrAdapterError("Supervisor tab label is required");
		}
		if (containsForbiddenArgumentCharacter(input.label)) {
			throw new HerdrAdapterError(
				"Supervisor tab label contains a forbidden control character",
			);
		}

		const environment = validateEnvironment(input.env, workspaceId);
		const args: string[] = [
			"tab",
			"create",
			"--workspace",
			workspaceId,
			"--cwd",
			input.cwd,
			"--label",
			input.label,
			"--no-focus",
		];
		for (const [key, value] of environment) {
			args.push("--env", `${key}=${value}`);
		}

		const redactions = Object.values(input.env ?? {});
		const result = resultRecord(
			await this.#json(args, workspaceId, redactions),
		);
		const tabId = returnedId(
			result,
			["tab", "created_tab", "createdTab"],
			["tab_id", "tabId"],
		);
		const paneId = returnedId(
			result,
			["root_pane", "rootPane", "pane"],
			["pane_id", "paneId"],
		);
		if (tabId === undefined || paneId === undefined) {
			throw new HerdrAdapterError(
				"Herdr tab response did not include opaque IDs",
			);
		}
		assertOpaqueId(tabId, "Supervisor tab ID");
		assertOpaqueId(paneId, "Supervisor pane ID");

		const tab = recordField(result, ["tab", "created_tab", "createdTab"]);
		const pane = recordField(result, ["root_pane", "rootPane", "pane"]);
		for (const returnedWorkspaceId of [
			tab === undefined
				? undefined
				: stringField(tab, ["workspace_id", "workspaceId"]),
			pane === undefined
				? undefined
				: stringField(pane, ["workspace_id", "workspaceId"]),
		]) {
			if (
				returnedWorkspaceId !== undefined &&
				returnedWorkspaceId !== workspaceId
			) {
				throw new HerdrAdapterError(
					"Herdr created the tab in a different workspace",
				);
			}
		}

		return { tabId, paneId };
	}

	async closeTab(tabId: string, workspaceId: string): Promise<void> {
		assertOpaqueId(tabId, "Tab ID");
		assertOpaqueId(workspaceId, "Workspace ID");
		await this.#execute(["tab", "close", tabId], workspaceId);
	}

	async runInPane(
		paneId: string,
		command: string,
		workspaceId?: string,
	): Promise<void> {
		assertOpaqueId(paneId, "Pane ID");
		if (command.trim().length === 0) {
			throw new HerdrAdapterError("Pane command is required");
		}
		assertShellValue(command);
		if (workspaceId !== undefined) assertOpaqueId(workspaceId, "Workspace ID");

		// `pane run` joins argv with spaces before submitting it to the pane. Passing
		// the already safely-built command as exactly one argv item preserves it.
		await this.#execute(["pane", "run", paneId, command], workspaceId);
	}

	async readPane(
		paneId: string,
		workspaceId?: string,
		lines = DEFAULT_READ_LINES,
		timeoutMs?: number,
	): Promise<string> {
		assertOpaqueId(paneId, "Pane ID");
		if (workspaceId !== undefined) assertOpaqueId(workspaceId, "Workspace ID");
		if (!Number.isSafeInteger(lines) || lines < 1 || lines > MAX_READ_LINES) {
			throw new HerdrAdapterError("Pane read line count is invalid");
		}

		const result = await this.#execute(
			[
				"pane",
				"read",
				paneId,
				"--source",
				"recent-unwrapped",
				"--lines",
				String(lines),
				"--format",
				"text",
			],
			workspaceId,
			undefined,
			false,
			timeoutMs,
		);
		if (result.stdoutTruncated) {
			throw new HerdrAdapterError(
				"Herdr pane output exceeded the output limit",
			);
		}
		return result.stdout;
	}

	async inspectPane(
		paneId: string,
		workspaceId?: string,
	): Promise<PaneProcessInfo> {
		assertOpaqueId(paneId, "Pane ID");
		if (workspaceId !== undefined) assertOpaqueId(workspaceId, "Workspace ID");
		const result = resultRecord(
			await this.#json(["pane", "process-info", "--pane", paneId], workspaceId),
		);
		return normalizedPaneCommand(result["process_info"], paneId);
	}

	async interruptPane(paneId: string, workspaceId?: string): Promise<void> {
		assertOpaqueId(paneId, "Pane ID");
		if (workspaceId !== undefined) assertOpaqueId(workspaceId, "Workspace ID");
		await this.#execute(["pane", "send-keys", paneId, "C-c"], workspaceId);
	}
}
