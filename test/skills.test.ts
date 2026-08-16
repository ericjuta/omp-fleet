import { describe, expect, test } from "bun:test";
import { readdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseFrontmatter } from "@oh-my-pi/pi-utils";

const SKILLS_ROOT = join(
	dirname(fileURLToPath(import.meta.url)),
	"..",
	"skills",
);

describe("packaged skill frontmatter", () => {
	test("parses every skills/*/SKILL.md and pins name to the directory", async () => {
		const directories = (await readdir(SKILLS_ROOT, { withFileTypes: true }))
			.filter((entry) => entry.isDirectory() && !entry.name.startsWith("."))
			.map((entry) => entry.name)
			.sort();
		expect(directories.length).toBeGreaterThan(0);

		for (const directory of directories) {
			const skillPath = join(SKILLS_ROOT, directory, "SKILL.md");
			const content = await Bun.file(skillPath).text();
			const { frontmatter } = parseFrontmatter(content, {
				source: skillPath,
				level: "fatal",
				repair: false,
				rawKeys: true,
			});
			expect(frontmatter.name).toBe(directory);
			expect(typeof frontmatter.description).toBe("string");
			expect((frontmatter.description as string).trim().length).toBeGreaterThan(
				0,
			);
		}
	});

	test("rejects an implicit-key description that OMP cannot load", () => {
		const broken = `---
name: omp-fleet-supervision
description:
  Use OMP Fleet to start, inspect, stop, or review reports for a bounded
  read-only Herdr worker supervisor. Use when asked to keep tabs on delegated
  agents, monitor a Herdr worker cohort, supervise a swarm, collect done or
  blocked output, observe prompt-evaluation runs, or compose \`fleet <task>\` /
  \`fleet it\` / cleanup Fleet. Default is the shared parent → coordinator A →
  Fleet contract: compose and auto-handoff through Herdr tooling; Fleet only
  observes. Does not execute the named task, create workers, or monitor
  repository drift as Fleet.
---
`;
		expect(() =>
			parseFrontmatter(broken, {
				source: "broken-implicit-key",
				level: "fatal",
				repair: false,
				rawKeys: true,
			}),
		).toThrow(/Multiline implicit key/);
	});
});
