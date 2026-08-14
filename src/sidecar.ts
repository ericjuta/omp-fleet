import { realpath } from "node:fs/promises";
import { isAbsolute, join, relative, sep } from "node:path";
import { HerdrClient } from "./herdr.ts";
import { RunStore } from "./store.ts";
import {
	runSupervisor,
	type SupervisorClock,
	type SupervisorDependencies,
	type SupervisorSleep,
} from "./supervisor.ts";
import {
	assertRunId,
	containsControlCharacter,
	type RunManifest,
} from "./types.ts";

export interface SidecarArguments {
	runId: string;
	stateRoot: string;
}

type SidecarStore = SupervisorDependencies["store"] &
	Pick<RunStore, "readManifest">;
type SidecarHerdr = SupervisorDependencies["herdr"];

type SignalName = "SIGINT" | "SIGTERM";
type SignalListener = () => void;

export interface SignalSource {
	on(signal: SignalName, listener: SignalListener): unknown;
	off(signal: SignalName, listener: SignalListener): unknown;
}

export interface SidecarDependencies {
	store?: SidecarStore;
	herdr?: SidecarHerdr;
	createStore?: (stateRoot: string) => SidecarStore;
	createHerdr?: () => SidecarHerdr;
	run?: typeof runSupervisor;
	now?: SupervisorClock;
	sleep?: SupervisorSleep;
	env?: Readonly<Record<string, string | undefined>>;
	signals?: SignalSource;
	canonicalizePath?: (path: string) => Promise<string>;
	writeError?: (message: string) => void;
}

class SidecarArgumentError extends Error {}

function parseValue(
	argv: readonly string[],
	index: number,
	flag: string,
): string {
	const value = argv[index + 1];
	if (value === undefined || value.length === 0 || value.startsWith("--")) {
		throw new SidecarArgumentError(`${flag} requires a value`);
	}
	return value;
}

/** Accepts only the two control-plane-generated fixed flags. */
export function parseSidecarArguments(
	argv: readonly string[],
): SidecarArguments {
	let runId: string | undefined;
	let stateRoot: string | undefined;

	for (let index = 0; index < argv.length; index += 2) {
		const flag = argv[index];
		if (flag === "--run-id") {
			if (runId !== undefined) {
				throw new SidecarArgumentError("--run-id may only be provided once");
			}
			runId = parseValue(argv, index, flag);
			continue;
		}
		if (flag === "--state-root") {
			if (stateRoot !== undefined) {
				throw new SidecarArgumentError(
					"--state-root may only be provided once",
				);
			}
			stateRoot = parseValue(argv, index, flag);
			continue;
		}
		throw new SidecarArgumentError("unknown sidecar argument");
	}

	if (runId === undefined || stateRoot === undefined) {
		throw new SidecarArgumentError("--run-id and --state-root are required");
	}
	try {
		assertRunId(runId);
	} catch {
		throw new SidecarArgumentError("invalid run ID");
	}
	if (!isAbsolute(stateRoot) || containsControlCharacter(stateRoot)) {
		throw new SidecarArgumentError("--state-root must be an absolute path");
	}

	return { runId, stateRoot };
}

function isWithin(parent: string, candidate: string): boolean {
	const path = relative(parent, candidate);
	return (
		path === "" ||
		(path !== ".." && !path.startsWith(`..${sep}`) && !isAbsolute(path))
	);
}

async function assertExternalState(
	arguments_: SidecarArguments,
	manifest: RunManifest,
	canonicalizePath: (path: string) => Promise<string>,
): Promise<void> {
	if (manifest.runId !== arguments_.runId || !isAbsolute(manifest.repoPath)) {
		throw new Error("persisted manifest does not match sidecar arguments");
	}

	const [repository, stateRoot, runDirectory] = await Promise.all([
		canonicalizePath(manifest.repoPath),
		canonicalizePath(arguments_.stateRoot),
		canonicalizePath(join(arguments_.stateRoot, arguments_.runId)),
	]);
	if (
		isWithin(repository, stateRoot) ||
		isWithin(stateRoot, repository) ||
		isWithin(repository, runDirectory) ||
		isWithin(runDirectory, repository)
	) {
		throw new Error(
			"state directory and monitored repository must not contain one another",
		);
	}
}

/**
 * Loads an existing run and executes it. It never accepts a repository path or
 * command from argv and returns an exit code rather than terminating the host.
 */
export async function main(
	argv: readonly string[] = Bun.argv.slice(2),
	dependencies: SidecarDependencies = {},
): Promise<number> {
	const writeError =
		dependencies.writeError ??
		((message: string): void => {
			process.stderr.write(`${message}\n`);
		});
	let arguments_: SidecarArguments;
	try {
		arguments_ = parseSidecarArguments(argv);
	} catch {
		writeError("omp-fleet sidecar: invalid arguments");
		return 2;
	}

	const environment = dependencies.env ?? process.env;
	if (environment.HERDR_ENV !== "1") {
		writeError("omp-fleet sidecar: HERDR_ENV=1 is required");
		return 1;
	}

	const abort = new AbortController();
	const stop = (): void => abort.abort();
	const signals = dependencies.signals ?? process;
	signals.on("SIGINT", stop);
	signals.on("SIGTERM", stop);

	try {
		const canonicalizePath = dependencies.canonicalizePath ?? realpath;
		await canonicalizePath(join(arguments_.stateRoot, arguments_.runId));
		const store =
			dependencies.store ??
			dependencies.createStore?.(arguments_.stateRoot) ??
			new RunStore(arguments_.stateRoot);
		const herdr =
			dependencies.herdr ?? dependencies.createHerdr?.() ?? new HerdrClient();
		const manifest = await store.readManifest(arguments_.runId);
		await assertExternalState(arguments_, manifest, canonicalizePath);

		const execute = dependencies.run ?? runSupervisor;
		const supervisorDependencies: SupervisorDependencies = {
			store,
			herdr,
			...(dependencies.now === undefined ? {} : { now: dependencies.now }),
			...(dependencies.sleep === undefined
				? {}
				: { sleep: dependencies.sleep }),
		};
		const finalManifest = await execute(
			{ manifest, signal: abort.signal },
			supervisorDependencies,
		);
		return finalManifest.lifecycle === "completed" ||
			finalManifest.lifecycle === "stopped"
			? 0
			: 1;
	} catch {
		writeError("omp-fleet sidecar: failed");
		return 1;
	} finally {
		signals.off("SIGINT", stop);
		signals.off("SIGTERM", stop);
	}
}

if (import.meta.main) {
	process.exitCode = await main();
}
