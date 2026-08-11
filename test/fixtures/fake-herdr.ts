#!/usr/bin/env bun

import { appendFile } from "node:fs/promises";

const argv = Bun.argv.slice(2);
const workspaceId = process.env.HERDR_WORKSPACE_ID;
const logPath = process.env.FAKE_HERDR_LOG;
const paneId = process.env.FAKE_HERDR_PANE_ID ?? "pane-worker-smoke";

if (logPath !== undefined) {
	await appendFile(
		logPath,
		`${JSON.stringify({ argv, cwd: process.cwd(), workspaceId: workspaceId ?? null })}\n`,
		"utf8",
	);
}

if (argv.length === 2 && argv[0] === "agent" && argv[1] === "list") {
	if (workspaceId === undefined || workspaceId.length === 0) {
		process.stderr.write("fake-herdr: missing HERDR_WORKSPACE_ID\n");
		process.exitCode = 64;
	} else {
		process.stdout.write(
			`${JSON.stringify({
				id: "fake-agent-list",
				result: {
					agents: [
						{
							pane_id: paneId,
							workspace_id: workspaceId,
							name: "worker-smoke",
							status: "done",
							revision: "smoke-revision",
						},
					],
				},
			})}\n`,
		);
	}
} else if (
	argv.length === 9 &&
	argv[0] === "pane" &&
	argv[1] === "read" &&
	argv[2] === paneId &&
	argv[3] === "--source" &&
	argv[4] === "recent-unwrapped" &&
	argv[5] === "--lines" &&
	argv[6] === "200" &&
	argv[7] === "--format" &&
	argv[8] === "text"
) {
	process.stdout.write("FAKE_HERDR_SMOKE_OUTPUT\n");
} else {
	process.stderr.write("fake-herdr: unexpected arguments\n");
	process.exitCode = 64;
}
