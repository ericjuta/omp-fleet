#!/usr/bin/env bun

import { homedir } from "node:os";
import { join } from "node:path";
import { runLegacyLockMigration } from "./migrate-lock-command.ts";
import { ProtocolStoreError } from "./store.ts";

const DEFAULT_STATE_ROOT = join(homedir(), ".omp", "fleet", "runs");

function parseStateRoot(argv: readonly string[]): string {
	let stateRoot = DEFAULT_STATE_ROOT;
	for (let index = 0; index < argv.length; index += 1) {
		const argument = argv[index];
		if (argument === "--state-root") {
			const value = argv[index + 1];
			if (value === undefined) {
				throw new Error("--state-root requires a path");
			}
			stateRoot = value;
			index += 1;
			continue;
		}
		throw new Error(`unknown argument: ${argument}`);
	}
	return stateRoot;
}

const stateRoot = parseStateRoot(process.argv.slice(2));
try {
	const result = await runLegacyLockMigration(stateRoot);
	if (result.archivedPath === undefined) {
		console.log(
			`legacy lock container already migrated at ${stateRoot}/.manifest-lock.sqlite`,
		);
	} else {
		console.log(`archived leftover v0.1 lock to ${result.archivedPath}`);
	}
} catch (error) {
	const message =
		error instanceof ProtocolStoreError || error instanceof Error
			? error.message
			: String(error);
	console.error(`legacy lock migration failed: ${message}`);
	process.exitCode = 1;
}
