import { describe, expect, test } from "bun:test";
import {
	chmod,
	lstat,
	mkdir,
	readdir,
	readFile,
	rm,
	symlink,
	writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import {
	type LegacyManifestLockMigrationEvidence,
	migrateLegacyManifestLockFile,
	ProtocolStoreError,
} from "../src/store.ts";
import { makeTempDirectory, removeTempDirectory } from "./helpers.ts";

const MANIFEST_MUTEX_DIRECTORY = ".manifest-lock.sqlite";
const ARCHIVE_NAME = ".manifest-lock.sqlite.v0.1-file-20260814T142317Z";
const FIXED_NOW = new Date("2026-08-14T14:23:17.456Z");
const QUIESCENT_EVIDENCE = {
	noSidecarPids: true,
	noLockHolders: true,
	missingSupervisorPanes: true,
} as const satisfies LegacyManifestLockMigrationEvidence;

async function withMigrationRoot(
	action: (root: string) => Promise<void>,
): Promise<void> {
	const root = await makeTempDirectory("omp-fleet-lock-migration-");
	try {
		await action(root);
	} finally {
		await removeTempDirectory(root);
	}
}

describe("legacy manifest lock migration", () => {
	test("archives the v0.1 file and creates the private v0.2 container", async () => {
		await withMigrationRoot(async (root) => {
			const mutexPath = join(root, MANIFEST_MUTEX_DIRECTORY);
			const archivePath = join(root, ARCHIVE_NAME);
			await writeFile(mutexPath, "legacy-lock-bytes\n", { mode: 0o600 });

			expect(
				await migrateLegacyManifestLockFile(
					root,
					QUIESCENT_EVIDENCE,
					FIXED_NOW,
				),
			).toBe(archivePath);

			expect((await lstat(mutexPath)).isDirectory()).toBe(true);
			expect((await lstat(mutexPath)).mode & 0o777).toBe(0o700);
			expect((await lstat(archivePath)).isFile()).toBe(true);
			expect((await lstat(archivePath)).mode & 0o777).toBe(0o600);
			expect(await readFile(archivePath, "utf8")).toBe("legacy-lock-bytes\n");
			expect((await readdir(root)).sort()).toEqual(
				[MANIFEST_MUTEX_DIRECTORY, ARCHIVE_NAME].sort(),
			);
		});
	});

	test("is an evidence-free no-op for an existing private container", async () => {
		await withMigrationRoot(async (root) => {
			const mutexPath = join(root, MANIFEST_MUTEX_DIRECTORY);
			await mkdir(mutexPath, { mode: 0o700 });

			expect(
				await migrateLegacyManifestLockFile(
					root,
					{
						noSidecarPids: false,
						noLockHolders: false,
						missingSupervisorPanes: false,
					},
					new Date(Number.NaN),
				),
			).toBeUndefined();
			expect((await lstat(mutexPath)).mode & 0o777).toBe(0o700);
			expect(await readdir(root)).toEqual([MANIFEST_MUTEX_DIRECTORY]);
		});
	});

	test("requires every quiescence signal before moving the legacy file", async () => {
		await withMigrationRoot(async (root) => {
			const mutexPath = join(root, MANIFEST_MUTEX_DIRECTORY);
			await writeFile(mutexPath, "still-live\n", { mode: 0o600 });
			const unsafeEvidence: LegacyManifestLockMigrationEvidence[] = [
				{ ...QUIESCENT_EVIDENCE, noSidecarPids: false },
				{ ...QUIESCENT_EVIDENCE, noLockHolders: false },
				{ ...QUIESCENT_EVIDENCE, missingSupervisorPanes: false },
			];

			for (const evidence of unsafeEvidence) {
				await expect(
					migrateLegacyManifestLockFile(root, evidence, FIXED_NOW),
				).rejects.toBeInstanceOf(ProtocolStoreError);
			}

			expect(await readFile(mutexPath, "utf8")).toBe("still-live\n");
			expect(await readdir(root)).toEqual([MANIFEST_MUTEX_DIRECTORY]);
		});
	});

	test("refuses missing, symlinked, and non-private container paths", async () => {
		await withMigrationRoot(async (root) => {
			const mutexPath = join(root, MANIFEST_MUTEX_DIRECTORY);
			await expect(
				migrateLegacyManifestLockFile(root, QUIESCENT_EVIDENCE, FIXED_NOW),
			).rejects.toBeInstanceOf(ProtocolStoreError);

			const sentinelPath = join(root, "sentinel");
			await writeFile(sentinelPath, "unchanged\n", { mode: 0o600 });
			await symlink(sentinelPath, mutexPath, "file");
			await expect(
				migrateLegacyManifestLockFile(root, QUIESCENT_EVIDENCE, FIXED_NOW),
			).rejects.toBeInstanceOf(ProtocolStoreError);
			expect((await lstat(mutexPath)).isSymbolicLink()).toBe(true);
			expect(await readFile(sentinelPath, "utf8")).toBe("unchanged\n");

			await rm(mutexPath);
			await mkdir(mutexPath, { mode: 0o755 });
			await chmod(mutexPath, 0o755);
			await expect(
				migrateLegacyManifestLockFile(root, QUIESCENT_EVIDENCE, FIXED_NOW),
			).rejects.toBeInstanceOf(ProtocolStoreError);
			expect((await lstat(mutexPath)).mode & 0o777).toBe(0o755);
		});
	});

	test("never replaces an existing timestamped archive", async () => {
		await withMigrationRoot(async (root) => {
			const mutexPath = join(root, MANIFEST_MUTEX_DIRECTORY);
			const archivePath = join(root, ARCHIVE_NAME);
			await writeFile(mutexPath, "legacy-current\n", { mode: 0o600 });
			await writeFile(archivePath, "earlier-archive\n", { mode: 0o600 });

			await expect(
				migrateLegacyManifestLockFile(root, QUIESCENT_EVIDENCE, FIXED_NOW),
			).rejects.toBeInstanceOf(ProtocolStoreError);
			expect(await readFile(mutexPath, "utf8")).toBe("legacy-current\n");
			expect(await readFile(archivePath, "utf8")).toBe("earlier-archive\n");
		});
	});
});
