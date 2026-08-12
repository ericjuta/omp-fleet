import { describe, expect, test } from "bun:test";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
	buildPaneCommand,
	bunCommandRunner,
	type CommandOptions,
	type CommandResult,
	type CommandRunner,
	CommandRunnerError,
	decodeHerdrEnvelope,
	HerdrAdapterError,
	HerdrClient,
	HerdrServerError,
	normalizeHerdrAgent,
	normalizeHerdrAgents,
	paneProcessOwnsCommand,
} from "../src/herdr.ts";
import { makeTempDirectory, removeTempDirectory } from "./helpers.ts";

interface RecordedCommand {
	readonly command: string;
	readonly args: string[];
	readonly options: CommandOptions | undefined;
}

type FakeResponse = CommandResult | Error;

function commandResult(overrides: Partial<CommandResult> = {}): CommandResult {
	return {
		stdout: "",
		stderr: "",
		exitCode: 0,
		...overrides,
	};
}

function makeFakeRunner(...responses: readonly FakeResponse[]): {
	readonly calls: RecordedCommand[];
	readonly runner: CommandRunner;
} {
	const calls: RecordedCommand[] = [];
	const pending = [...responses];
	const runner: CommandRunner = async (command, args, options) => {
		calls.push({ command, args: [...args], options });
		const response = pending.shift();
		if (response === undefined) {
			throw new Error("Unexpected fake Herdr command");
		}
		if (response instanceof Error) throw response;
		return response;
	};
	return { calls, runner };
}

async function catchRejection(action: () => Promise<unknown>): Promise<Error> {
	try {
		await action();
	} catch (error) {
		if (error instanceof Error) return error;
		throw new Error("Action rejected with a non-Error value");
	}
	throw new Error("Expected action to reject");
}

function catchThrown(action: () => unknown): Error {
	try {
		action();
	} catch (error) {
		if (error instanceof Error) return error;
		throw new Error("Action threw a non-Error value");
	}
	throw new Error("Expected action to throw");
}

describe("bunCommandRunner", () => {
	test("passes shell metacharacters as exact argv without executing them", async () => {
		const directory = await makeTempDirectory("omp-fleet-herdr-runner-");
		const probePath = join(directory, "argv-probe.ts");
		const markerPath = join(directory, "shell-join-marker");
		const literalArguments = [
			"plain",
			"two words",
			"$HOME",
			"'single quotes'",
			'"double quotes"',
			`$(touch ${markerPath})`,
			`; touch ${markerPath}`,
		];

		try {
			await writeFile(
				probePath,
				"process.stdout.write(JSON.stringify(Bun.argv.slice(2)));\n",
				"utf8",
			);
			const result = await bunCommandRunner(
				process.execPath,
				[probePath, ...literalArguments],
				{ timeoutMs: 5_000, maxOutputBytes: 65_536 },
			);

			expect(JSON.parse(result.stdout)).toEqual(literalArguments);
			expect(result.stderr).toBe("");
			expect(await Bun.file(markerPath).exists()).toBe(false);
		} finally {
			await removeTempDirectory(directory);
		}
	});
});

describe("Herdr JSON responses", () => {
	test("decodes result envelopes without exposing the request envelope", () => {
		expect(
			decodeHerdrEnvelope(
				'{"id":"request-7","result":{"tab_id":"tab:opaque/7","ok":true}}',
			),
		).toEqual({ tab_id: "tab:opaque/7", ok: true });
	});

	test("turns server-error envelopes into bounded, redacted adapter errors", () => {
		const error = catchThrown(() =>
			decodeHerdrEnvelope(
				'{"id":"request-9","error":{"code":"permission_denied","message":"token top-secret-value was rejected\\nretry"}}',
				["top-secret-value"],
			),
		);

		expect(error).toBeInstanceOf(HerdrServerError);
		expect(error.message).toBe(
			"Herdr server error (permission_denied): token [redacted] was rejected retry",
		);
		expect(error.message).not.toContain("top-secret-value");
	});
});

describe("Herdr agent normalization", () => {
	test("normalizes plausible nested list wrappers while preserving opaque IDs and working", () => {
		const agents = normalizeHerdrAgents({
			payload: {
				list: {
					items: [
						{
							pane_id: "pane://worker/$alpha?slot=1",
							workspace_id: "workspace://acme/dev?branch=x",
							agent_name: "worker-alpha",
							agent_status: "working",
							state_change_seq: 42,
						},
						{
							paneId: "pane:{beta}#2",
							workspaceId: "workspace://acme/dev?branch=x",
							displayAgent: "worker-beta",
							lifecycle: {
								state: "COMPLETED",
								lifecycle_revision: "rev 2",
							},
						},
					],
				},
			},
		});

		expect(agents).toEqual([
			{
				paneId: "pane://worker/$alpha?slot=1",
				workspaceId: "workspace://acme/dev?branch=x",
				name: "worker-alpha",
				status: "working",
				revision: "42",
			},
			{
				paneId: "pane:{beta}#2",
				workspaceId: "workspace://acme/dev?branch=x",
				name: "worker-beta",
				status: "done",
				revision: "rev%202",
			},
		]);
	});

	test("extracts sanitized task titles only from explicit task-title fields", () => {
		const snakeCase = normalizeHerdrAgent({
			pane_id: "pane-alpha",
			workspace_id: "workspace-alpha",
			name: "worker-alpha",
			status: "working",
			task_title: "  Stabilize   queue ownership  ",
		});
		const camelCase = normalizeHerdrAgent({
			paneId: "pane-beta",
			workspaceId: "workspace-alpha",
			agentName: "worker-beta",
			status: "blocked",
			taskTitle: "Review bounded report output",
		});

		expect(snakeCase.taskTitle).toBe("Stabilize queue ownership");
		expect(camelCase.taskTitle).toBe("Review bounded report output");
	});

	test("omits invalid task titles and never treats worker names as task titles", () => {
		const workerOnly = normalizeHerdrAgent({
			pane_id: "pane-worker",
			workspace_id: "workspace-alpha",
			name: "worker-alpha",
			agent_name: "agent-alpha",
			worker_name: "worker-alpha",
			workerName: "worker-alpha",
			display_name: "Worker Alpha",
			displayName: "Worker Alpha",
			displayAgent: "Worker Alpha",
			status: "working",
		});
		const invalidTitles = [
			normalizeHerdrAgent({
				pane_id: "pane-control",
				workspace_id: "workspace-alpha",
				task_title: "unsafe\ntitle",
			}),
			normalizeHerdrAgent({
				pane_id: "pane-overlong",
				workspace_id: "workspace-alpha",
				taskTitle: "x".repeat(513),
			}),
			normalizeHerdrAgent({
				pane_id: "pane-invalid",
				workspace_id: "workspace-alpha",
				task_title: 42,
			}),
			normalizeHerdrAgent({
				pane_id: "pane-bidi",
				workspace_id: "workspace-alpha",
				task_title: "unsafe\u202etitle",
			}),
		];

		expect(workerOnly.name).toBe("worker-alpha");
		expect(workerOnly).not.toHaveProperty("taskTitle");
		for (const agent of invalidTitles) {
			expect(agent).not.toHaveProperty("taskTitle");
		}
	});

	test("derives stable literal revisions and changes them only for revision inputs", () => {
		const first = normalizeHerdrAgent({
			pane_id: "pane-alpha",
			workspace_id: "workspace-alpha",
			name: "worker-alpha",
			status: "working",
			observed_at: "2026-08-11T01:00:00.000Z",
		});
		const sameLifecycleLater = normalizeHerdrAgent({
			observed_at: "2026-08-11T02:00:00.000Z",
			status: "working",
			name: "worker-alpha",
			workspace_id: "workspace-alpha",
			pane_id: "pane-alpha",
			unrelated: { changed: true },
		});
		const sequenceSeven = normalizeHerdrAgent({
			pane_id: "pane-alpha",
			workspace_id: "workspace-alpha",
			name: "worker-alpha",
			status: "working",
			state: { state_change_seq: 7 },
		});
		const sequenceEight = normalizeHerdrAgent({
			pane_id: "pane-alpha",
			workspace_id: "workspace-alpha",
			name: "worker-alpha",
			status: "working",
			state: { state_change_seq: 8 },
		});

		expect(first.revision).toBe("status:working");
		expect(sameLifecycleLater.revision).toBe("status:working");
		expect(sequenceSeven.revision).toBe("7");
		expect(sequenceEight.revision).toBe("8");
	});
});

describe("HerdrClient", () => {
	test("lists only the requested workspace through a fixed no-shell argv", async () => {
		const workspaceId = "workspace:team/$alpha?slot=1";
		const fake = makeFakeRunner(
			commandResult({
				stdout:
					'{"id":"list-1","result":{"data":{"agents":[{"pane_id":"pane:one/$x","workspace_id":"workspace:team/$alpha?slot=1","name":"worker-one","status":"working","revision":"rev-1"},{"pane_id":"pane:other","workspace_id":"workspace:other","name":"worker-other","status":"done","revision":"rev-9"}]}}}',
			}),
		);
		const client = new HerdrClient(fake.runner);

		await expect(client.listAgents(workspaceId)).resolves.toEqual([
			{
				paneId: "pane:one/$x",
				workspaceId: "workspace:team/$alpha?slot=1",
				name: "worker-one",
				status: "working",
				revision: "rev-1",
			},
		]);
		expect(fake.calls).toEqual([
			{
				command: "herdr",
				args: ["agent", "list"],
				options: {
					env: { HERDR_WORKSPACE_ID: "workspace:team/$alpha?slot=1" },
					timeoutMs: 15_000,
				},
			},
		]);
	});

	test("uses caller timeouts without exceeding the fixed Herdr bound", async () => {
		const workspaceId = "workspace-timeout";
		const fake = makeFakeRunner(
			commandResult({
				stdout: '{"id":"list-timeout","result":{"agents":[]}}',
			}),
			commandResult({ stdout: "pane output" }),
		);
		const client = new HerdrClient(fake.runner);

		await client.listAgents(workspaceId, 27);
		await client.readPane("pane-timeout", workspaceId, 4, 90_000);

		expect(fake.calls.map((call) => call.options?.timeoutMs)).toEqual([
			27, 15_000,
		]);

		const invalid = makeFakeRunner();
		const invalidClient = new HerdrClient(invalid.runner);
		const error = await catchRejection(() =>
			invalidClient.listAgents(workspaceId, 0),
		);
		expect(error).toBeInstanceOf(HerdrAdapterError);
		expect(error.message).toBe("Herdr command timeout is invalid");
		expect(invalid.calls).toEqual([]);
	});

	test("creates a non-focused tab with exact workspace-scoped argv", async () => {
		const workspaceId = "workspace:team/$alpha?slot=1";
		const fake = makeFakeRunner(
			commandResult({
				stdout:
					'{"id":"create-1","result":{"tab":{"tab_id":"tab://fleet/$opaque?one=1","workspace_id":"workspace:team/$alpha?slot=1"},"root_pane":{"pane_id":"pane://fleet/$opaque?two=2","workspace_id":"workspace:team/$alpha?slot=1"}}}',
			}),
		);
		const client = new HerdrClient(fake.runner);

		await expect(
			client.createSupervisorTab({
				workspaceId,
				cwd: "/tmp/repo with spaces;$(never-run)",
				label: "Fleet's supervisor $HOME",
				env: {
					Z_SECRET: "very secret value",
					A_MODE: "safe;literal",
				},
			}),
		).resolves.toEqual({
			tabId: "tab://fleet/$opaque?one=1",
			paneId: "pane://fleet/$opaque?two=2",
		});
		expect(fake.calls).toEqual([
			{
				command: "herdr",
				args: [
					"tab",
					"create",
					"--workspace",
					"workspace:team/$alpha?slot=1",
					"--cwd",
					"/tmp/repo with spaces;$(never-run)",
					"--label",
					"Fleet's supervisor $HOME",
					"--no-focus",
					"--env",
					"A_MODE=safe;literal",
					"--env",
					"HERDR_ENV=1",
					"--env",
					"HERDR_WORKSPACE_ID=workspace:team/$alpha?slot=1",
					"--env",
					"Z_SECRET=very secret value",
				],
				options: {
					env: { HERDR_WORKSPACE_ID: "workspace:team/$alpha?slot=1" },
					timeoutMs: 15_000,
				},
			},
		]);
	});

	test("rejects a tab created outside the requested workspace", async () => {
		const fake = makeFakeRunner(
			commandResult({
				stdout:
					'{"id":"create-2","result":{"tab":{"tab_id":"tab-one","workspace_id":"workspace-other"},"root_pane":{"pane_id":"pane-one","workspace_id":"workspace-one"}}}',
			}),
		);
		const client = new HerdrClient(fake.runner);

		const error = await catchRejection(() =>
			client.createSupervisorTab({
				workspaceId: "workspace-one",
				cwd: "/tmp/repository",
				label: "fleet-supervisor",
			}),
		);

		expect(error).toBeInstanceOf(HerdrAdapterError);
		expect(error.message).toBe(
			"Herdr created the tab in a different workspace",
		);
	});

	test("redacts environment values from structured command failures", async () => {
		const fake = makeFakeRunner(
			commandResult({
				stderr:
					'{"id":"create-3","error":{"code":"s3cr3t-value","message":"token s3cr3t-value was rejected"}}',
				exitCode: 9,
			}),
		);
		const client = new HerdrClient(fake.runner);

		const error = await catchRejection(() =>
			client.createSupervisorTab({
				workspaceId: "workspace-one",
				cwd: "/tmp/repository",
				label: "fleet-supervisor",
				env: { API_TOKEN: "s3cr3t-value" },
			}),
		);

		expect(error).toBeInstanceOf(HerdrServerError);
		expect(error.message).toBe(
			"Herdr server error (server_error): token [redacted] was rejected",
		);
		expect(error.message).not.toContain("s3cr3t-value");
	});

	test("maps nonzero and timeout runner results to output-free diagnostics", async () => {
		const secretOutput = "stderr contains token-do-not-leak and /private/repo";
		const fake = makeFakeRunner(
			commandResult({ stderr: secretOutput, exitCode: 23 }),
			new CommandRunnerError(
				"low-level timeout included token-do-not-leak",
				commandResult({
					stdout: secretOutput,
					stderr: secretOutput,
					exitCode: 137,
					timedOut: true,
				}),
			),
		);
		const client = new HerdrClient(fake.runner);

		const nonzero = await catchRejection(() =>
			client.listAgents("workspace-one"),
		);
		const timeout = await catchRejection(() =>
			client.listAgents("workspace-one"),
		);

		expect(nonzero).toBeInstanceOf(HerdrAdapterError);
		expect(nonzero.message).toBe("Herdr command failed (exit 23)");
		expect(nonzero.message).not.toContain(secretOutput);
		expect(timeout).toBeInstanceOf(HerdrAdapterError);
		expect(timeout.message).toBe("Herdr command timed out");
		expect(timeout.message).not.toContain("token-do-not-leak");
	});

	test("rejects truncated JSON and pane output at the observable boundary", async () => {
		const fake = makeFakeRunner(
			commandResult({
				stdout: '{"id":"list-2","result":{"agents":[]}}',
				stdoutTruncated: true,
			}),
			commandResult({
				stdout: "partial pane output",
				stdoutTruncated: true,
			}),
		);
		const client = new HerdrClient(fake.runner);

		const jsonError = await catchRejection(() =>
			client.listAgents("workspace-one"),
		);
		const paneError = await catchRejection(() =>
			client.readPane("pane-one", "workspace-one", 12),
		);

		expect(jsonError.message).toBe("Herdr response exceeded the output limit");
		expect(paneError.message).toBe(
			"Herdr pane output exceeded the output limit",
		);
	});

	test("uses fixed pane run, read, inspect, interrupt, and close arguments", async () => {
		const paneId = "pane://worker/$alpha?slot=9#tail";
		const tabId = "tab://fleet/$alpha?slot=9#tail";
		const workspaceId = "workspace://team/$alpha?slot=1";
		const processArgv = ["/opt/Bun Runtime/bin/bun", "run", "worker's.ts"];
		const paneCommand = buildPaneCommand(
			processArgv[0] as string,
			processArgv.slice(1),
		);
		const fake = makeFakeRunner(
			commandResult(),
			commandResult({ stdout: "line one\nline two\n" }),
			commandResult({
				stdout: JSON.stringify({
					id: "inspect-1",
					result: {
						process_info: {
							pane_id: paneId,
							foreground_processes: [
								{
									pid: 321,
									name: "bun",
									argv0: processArgv[0],
									argv: processArgv,
									cmdline: `${paneCommand} ignored-untrusted-suffix`,
								},
							],
						},
					},
				}),
			}),
			commandResult(),
			commandResult(),
		);
		const client = new HerdrClient(fake.runner);

		await client.runInPane(paneId, paneCommand, workspaceId);
		await expect(client.readPane(paneId, workspaceId, 37, 413)).resolves.toBe(
			"line one\nline two\n",
		);
		const processInfo = await client.inspectPane(paneId, workspaceId);
		expect(processInfo).toEqual({ command: paneCommand });
		expect(paneProcessOwnsCommand(processInfo, paneCommand)).toBe(true);
		expect(
			paneProcessOwnsCommand(processInfo, "'/opt/Bun Runtime/bin/bun'"),
		).toBe(false);
		expect(paneProcessOwnsCommand(processInfo, `${paneCommand} 'extra'`)).toBe(
			false,
		);
		await client.interruptPane(paneId, workspaceId);
		await client.closeTab(tabId, workspaceId);

		expect(fake.calls).toEqual([
			{
				command: "herdr",
				args: ["pane", "run", paneId, paneCommand],
				options: {
					env: { HERDR_WORKSPACE_ID: workspaceId },
					timeoutMs: 15_000,
				},
			},
			{
				command: "herdr",
				args: [
					"pane",
					"read",
					paneId,
					"--source",
					"recent-unwrapped",
					"--lines",
					"37",
					"--format",
					"text",
				],
				options: {
					env: { HERDR_WORKSPACE_ID: workspaceId },
					timeoutMs: 413,
				},
			},
			{
				command: "herdr",
				args: ["pane", "process-info", "--pane", paneId],
				options: {
					env: { HERDR_WORKSPACE_ID: workspaceId },
					timeoutMs: 15_000,
				},
			},
			{
				command: "herdr",
				args: ["pane", "send-keys", paneId, "C-c"],
				options: {
					env: { HERDR_WORKSPACE_ID: workspaceId },
					timeoutMs: 15_000,
				},
			},
			{
				command: "herdr",
				args: ["tab", "close", tabId],
				options: {
					env: { HERDR_WORKSPACE_ID: workspaceId },
					timeoutMs: 15_000,
				},
			},
		]);
	});

	test("refuses incomplete, ambiguous, and wrong-pane process ownership", async () => {
		const paneId = "pane-owned";
		const expectedCommand = buildPaneCommand("/opt/bun", [
			"sidecar.ts",
			"--run-id",
			"run-owned",
		]);
		const matchingProcess = {
			pid: 91,
			name: "bun",
			argv: ["/opt/bun", "sidecar.ts", "--run-id", "run-owned"],
		};
		const fake = makeFakeRunner(
			commandResult({
				stdout: JSON.stringify({
					id: "inspect-ambiguous",
					result: {
						process_info: {
							pane_id: paneId,
							foreground_processes: [
								matchingProcess,
								{ ...matchingProcess, pid: 92 },
							],
						},
					},
				}),
			}),
			commandResult({
				stdout: JSON.stringify({
					id: "inspect-fuzzy",
					result: {
						process_info: {
							pane_id: paneId,
							foreground_processes: [
								{
									pid: 93,
									name: "bun",
									command: `prefix ${expectedCommand} suffix`,
									cmdline: expectedCommand,
								},
							],
						},
					},
				}),
			}),
			commandResult({
				stdout: JSON.stringify({
					id: "inspect-wrong-pane",
					result: {
						process_info: {
							pane_id: "pane-other",
							foreground_processes: [matchingProcess],
						},
					},
				}),
			}),
		);
		const client = new HerdrClient(fake.runner);

		const ambiguous = await client.inspectPane(paneId, "workspace-owned");
		expect(ambiguous).toEqual({ command: undefined });
		expect(paneProcessOwnsCommand(ambiguous, expectedCommand)).toBe(false);

		const fuzzy = await client.inspectPane(paneId, "workspace-owned");
		expect(fuzzy).toEqual({ command: undefined });
		expect(paneProcessOwnsCommand(fuzzy, expectedCommand)).toBe(false);

		const error = await catchRejection(() =>
			client.inspectPane(paneId, "workspace-owned"),
		);
		expect(error).toBeInstanceOf(HerdrAdapterError);
		expect(error.message).toBe(
			"Herdr process response did not match the requested pane",
		);
	});

	test("rejects non-opaque IDs before invoking the runner", async () => {
		const fake = makeFakeRunner();
		const client = new HerdrClient(fake.runner);

		const error = await catchRejection(() =>
			client.interruptPane(" pane-one", "workspace-one"),
		);

		expect(error).toBeInstanceOf(HerdrAdapterError);
		expect(error.message).toBe("Pane ID is invalid");
		expect(fake.calls).toEqual([]);
	});
});

describe("buildPaneCommand", () => {
	test("quotes every literal word, including single quotes and empty arguments", () => {
		expect(
			buildPaneCommand("/Applications/Bun Runtime/bin/bun", [
				"run",
				"sidecar's entry.ts",
				"two words",
				"",
				"$HOME; rm -rf /",
			]),
		).toBe(
			"'/Applications/Bun Runtime/bin/bun' 'run' 'sidecar'\"'\"'s entry.ts' 'two words' '' '$HOME; rm -rf /'",
		);
	});

	test("rejects newline and NUL rather than embedding shell control data", () => {
		expect(() => buildPaneCommand("bun\nsh", [])).toThrow(
			"Shell argument contains a forbidden control character",
		);
		expect(() => buildPaneCommand("bun", ["safe\0unsafe"])).toThrow(
			"Shell argument contains a forbidden control character",
		);
	});
});
