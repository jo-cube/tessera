import hashlib
import importlib.util
import io
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('release', REPO / 'scripts/release.py')
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)

MANIFEST = '[package]\nname = "tessera"\nversion = "0.1.0"\nrust-version = "1.89"\n\n[dependencies]\ncrc32fast = "1"\n'
LOCK = ('version = 4\n\n[[package]]\nname = "crc32fast"\nversion = "1.5.0"\n'
        'source = "registry+https://github.com/rust-lang/crates.io-index"\n\n'
        '[[package]]\nname = "tessera"\nversion = "0.1.0"\ndependencies = [\n "crc32fast",\n]\n')
BENCH_LOCK = ('version = 4\n\n[[package]]\nname = "tessera"\nversion = "0.1.0"\n\n'
              '[[package]]\nname = "tessera-bench"\nversion = "0.1.0"\n')


class Repository(unittest.TestCase):
    """A throwaway Git repository shaped like Tessera's release files."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.write('Cargo.toml', MANIFEST)
        self.write('Cargo.lock', LOCK)
        self.write('tools/bench/Cargo.lock', BENCH_LOCK)
        self.write('CHANGELOG.md', '# Changelog\n\n## Unreleased\n')
        self.git('init', '--quiet', '-b', 'main')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.invalid')
        self.git('config', 'commit.gpgsign', 'false')
        self.git('config', 'tag.gpgsign', 'false')
        self.commit('Initial commit')

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True).strip()

    def commit(self, subject):
        self.git('add', '-A')
        self.git('commit', '--quiet', '--allow-empty', '-m', subject)
        return self.git('rev-parse', '--short', 'HEAD')

    def prep(self, version, **kwargs):
        out = io.StringIO()
        section = release.release_prep(self.root, version, date='2026-11-20', out=out, **kwargs)
        return section, out.getvalue()


class Versions(Repository):
    def test_tag_must_match_the_manifest_and_both_lockfiles(self):
        self.assertEqual(release.validate_version(self.root, 'v0.1.0'), '0.1.0')
        for name, old, new in [
            ('Cargo.toml', 'version = "0.1.0"', 'version = "0.2.0"'),
            ('Cargo.lock', 'name = "tessera"\nversion = "0.1.0"', 'name = "tessera"\nversion = "0.0.9"'),
            ('tools/bench/Cargo.lock', 'name = "tessera"\nversion = "0.1.0"', 'name = "tessera"\nversion = "0.0.9"'),
        ]:
            with self.subTest(name=name):
                path = self.root / name
                original = path.read_text()
                path.write_text(original.replace(old, new, 1))
                with self.assertRaisesRegex(ValueError, 'match|version'):
                    release.validate_version(self.root, 'v0.1.0')
                path.write_text(original)
        for tag in ['v0.2.0', 'v01.0.0', 'v0.1.0-rc.1', '0.1.0', 'v0.1']:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release.validate_version(self.root, tag)

    def test_only_the_package_section_version_counts(self):
        self.write('Cargo.toml', '[package]\nname = "tessera"\nversion = "0.1.0"\n\n'
                                 '[dependencies]\nzstd = { version = "8" }\n')
        self.assertEqual(release.manifest_version(self.root), '0.1.0')


class Tags(Repository):
    def setUp(self):
        super().setUp()
        self.write('CHANGELOG.md', '# Changelog\n\n## Unreleased\n\n## 0.1.0 - 2026-11-20\n\n- engine: first\n')
        self.commit('chore: release 0.1.0')
        self.head = self.git('rev-parse', 'HEAD')

    def test_an_annotated_tag_on_main_with_notes_passes(self):
        self.git('tag', '-a', 'v0.1.0', '-m', 'tessera 0.1.0')
        self.assertEqual(release.check_tag(self.root, 'v0.1.0', self.head, 'main'), ('0.1.0', self.head))

    def test_a_lightweight_tag_on_main_with_notes_passes(self):
        self.git('tag', 'v0.1.0')
        self.assertEqual(release.check_tag(self.root, 'v0.1.0', self.head, 'main'), ('0.1.0', self.head))

    def test_a_mismatched_version_fails(self):
        self.git('tag', '-a', 'v0.2.0', '-m', 'tessera 0.2.0')
        with self.assertRaisesRegex(ValueError, 'does not match'):
            release.check_tag(self.root, 'v0.2.0', main_ref='main')

    def test_a_commit_off_main_fails(self):
        self.git('checkout', '--quiet', '-b', 'dev')
        self.commit('feat(engine): unreleased')
        self.git('tag', '-a', 'v0.1.0', '-m', 'tessera 0.1.0')
        with self.assertRaisesRegex(ValueError, 'reachable'):
            release.check_tag(self.root, 'v0.1.0', main_ref='main')

    def test_a_missing_changelog_section_fails(self):
        self.write('CHANGELOG.md', '# Changelog\n\n## Unreleased\n')
        self.commit('docs: drop notes')
        self.git('tag', '-a', 'v0.1.0', '-m', 'tessera 0.1.0')
        with self.assertRaisesRegex(ValueError, 'CHANGELOG'):
            release.check_tag(self.root, 'v0.1.0', main_ref='main')

    def test_the_checkout_and_trigger_must_be_the_tagged_commit(self):
        self.git('tag', '-a', 'v0.1.0', '-m', 'tessera 0.1.0')
        with self.assertRaisesRegex(ValueError, 'triggering'):
            release.check_tag(self.root, 'v0.1.0', 'b' * 40, 'main')
        self.commit('fix(engine): later')
        with self.assertRaisesRegex(ValueError, 'checkout'):
            release.check_tag(self.root, 'v0.1.0', main_ref='main')


class Changelog(unittest.TestCase):
    def test_commits_are_grouped_and_housekeeping_is_left_out(self):
        commits = [
            ('a1', 'feat(engine): add scans'),
            ('a2', 'fix(checkpoint): sync the directory'),
            ('a3', 'perf(read): skip a copy'),
            ('a4', 'docs(api): describe options'),
            ('a5', 'refactor(api)!: rename the workspace'),
            ('a6', 'docs(engine): reword'),
            ('a7', 'test(engine): cover scans'),
            ('a8', 'ci: cache'),
            ('a9', 'chore: tidy'),
            ('b1', 'refactor(run): split the builder'),
            ('b2', 'Initial commit'),
            ('b3', 'build(deps): bump zstd'),
        ]
        section = release.changelog_entries(commits, '0.2.0', '2026-11-20')
        self.assertEqual(section, '\n'.join([
            '## 0.2.0 - 2026-11-20', '',
            '### Features', '', '- engine: add scans (a1)', '',
            '### Fixes', '', '- checkpoint: sync the directory (a2)', '',
            '### Performance', '', '- read: skip a copy (a3)', '',
            '### API', '', '- api: describe options (a4)', '- api: rename the workspace **(breaking)** (a5)', '',
            '### Other', '', '- run: split the builder (b1)', '- Initial commit (b2)', '- deps: bump zstd (b3)', '',
        ]))

    def test_nothing_to_release_fails(self):
        with self.assertRaisesRegex(ValueError, 'no changes'):
            release.changelog_entries([('a1', 'test: more'), ('a2', 'ci: faster')], '0.2.0', 'today')

    def test_sections_are_found_by_version(self):
        text = '# Changelog\n\n## Unreleased\n\n## 0.2.0 - d\n\n- two\n\n## 0.1.0 - d\n\n- one\n'
        self.assertEqual(release.changelog_section(text, '0.2.0'), '- two')
        self.assertEqual(release.changelog_section(text, '0.1.0'), '- one')
        self.assertIsNone(release.changelog_section(text, '0.1'))
        self.assertIsNone(release.changelog_section(text, '0.3.0'))

    def test_unreleased_notes_move_into_the_new_section(self):
        text = '# Changelog\n\n## Unreleased\n\nHand-written.\n\n## 0.1.0 - d\n\n- one\n'
        section = release.changelog_entries([('a1', 'feat: x')], '0.2.0', 'd', release.unreleased_notes(text))
        self.assertEqual(release.prepend_section(text, '0.2.0', section),
                         '# Changelog\n\n## Unreleased\n\n## 0.2.0 - d\n\nHand-written.\n\n'
                         '### Features\n\n- x (a1)\n\n## 0.1.0 - d\n\n- one\n')


class Preparation(Repository):
    def test_prep_bumps_versions_prepends_the_changelog_and_commits(self):
        self.git('tag', '-a', 'v0.1.0', '-m', 'tessera 0.1.0')
        self.commit('test(engine): ignored')
        feature = self.commit('feat(engine): add a thing')
        section, out = self.prep('0.2.0')
        self.assertEqual(section, f'## 0.2.0 - 2026-11-20\n\n### Features\n\n- engine: add a thing ({feature})\n')
        self.assertEqual(release.validate_version(self.root, 'v0.2.0'), '0.2.0')
        self.assertIn('crc32fast"\nversion = "1.5.0"', (self.root / 'Cargo.lock').read_text())
        self.assertIn('tessera-bench"\nversion = "0.1.0"', (self.root / 'tools/bench/Cargo.lock').read_text())
        self.assertTrue((self.root / 'CHANGELOG.md').read_text().startswith(
            '# Changelog\n\n## Unreleased\n\n## 0.2.0 - 2026-11-20\n'))
        self.assertEqual(self.git('log', '-1', '--format=%s'), 'chore: release 0.2.0')
        self.assertEqual(self.git('status', '--porcelain'), '')
        self.assertEqual(self.git('tag', '--list', 'v0.2.0'), '')
        self.assertIn('merge it to main through the usual pull request', out)
        self.assertIn('create the v0.2.0 release in the GitHub Releases UI targeting main', out)
        self.assertNotIn('git tag', out)
        self.assertNotIn('git push', out)

    def test_prep_commits_on_a_non_main_branch(self):
        self.git('checkout', '--quiet', '-b', 'dev')
        self.commit('feat(engine): add a thing')
        _, out = self.prep('0.2.0')
        self.assertEqual(self.git('branch', '--show-current'), 'dev')
        self.assertEqual(self.git('log', '-1', '--format=%s'), 'chore: release 0.2.0')
        self.assertEqual(self.git('status', '--porcelain'), '')
        self.assertIn('usual pull request', out)
        self.assertIn('GitHub Releases UI targeting main', out)

    def test_the_first_release_keeps_the_version_and_reads_all_history(self):
        self.commit('fix(engine): one')
        section, _ = self.prep('0.1.0')
        self.assertIn('- Initial commit', section)
        self.assertIn('- engine: one', section)
        self.assertEqual(release.manifest_version(self.root), '0.1.0')

    def test_a_dry_run_changes_nothing(self):
        self.commit('feat(engine): add a thing')
        before = self.git('rev-parse', 'HEAD')
        self.write('scratch.txt', 'dirty')
        output = self.root / 'notes.md'
        self.git('checkout', '--quiet', '-b', 'dev')
        section, out = self.prep('0.2.0', dry_run=output)
        self.assertEqual(output.read_text(), section + '\n')
        self.assertIn('dry run', out)
        self.assertEqual(release.manifest_version(self.root), '0.1.0')
        self.assertEqual(self.git('rev-parse', 'HEAD'), before)

    def test_prep_refuses_mistakes(self):
        self.commit('feat(engine): add a thing')
        with self.assertRaisesRegex(ValueError, 'older'):
            self.prep('0.0.9')
        with self.assertRaisesRegex(ValueError, 'X.Y.Z'):
            self.prep('v0.2.0')
        self.write('scratch.txt', 'dirty')
        with self.assertRaisesRegex(ValueError, 'clean'):
            self.prep('0.2.0')
        (self.root / 'scratch.txt').unlink()
        self.git('tag', '-a', 'v0.2.0', '-m', 'tessera 0.2.0')
        with self.assertRaisesRegex(ValueError, 'exists'):
            self.prep('0.2.0')


class Package(unittest.TestCase):
    LISTING = ('.cargo_vcs_info.json\nARCHITECTURE.md\nCHANGELOG.md\nCargo.lock\nCargo.toml\n'
               'Cargo.toml.orig\nLICENSE\nREADME.md\nexamples/basic.rs\nsrc/lib.rs\ntests/batch.rs\n')

    def test_consumer_files_pass(self):
        self.assertEqual(len(release.check_package(self.LISTING)), 11)

    def test_tooling_or_missing_files_fail(self):
        for extra in ['tools/bench/Cargo.toml', 'scripts/release.py', '.github/workflows/ci.yml',
                      '.work/notes.md', 'rust-toolchain.toml', 'justfile']:
            with self.subTest(extra=extra), self.assertRaisesRegex(ValueError, 'does not need'):
                release.check_package(self.LISTING + extra + '\n')
        with self.assertRaisesRegex(ValueError, 'missing: CHANGELOG.md'):
            release.check_package(self.LISTING.replace('CHANGELOG.md\n', ''))


class Publication(Repository):
    REPO = 'owner/tessera'

    def setUp(self):
        super().setUp()
        self.write('CHANGELOG.md', '# Changelog\n\n## Unreleased\n\n## 0.1.0 - 2026-11-20\n\n- engine: first\n')
        self.commit('chore: release 0.1.0')
        self.git('tag', '-a', 'v0.1.0', '-m', 'tessera 0.1.0')
        self.revision = self.git('rev-parse', 'HEAD')
        self.crate = self.root / 'dist/tessera-0.1.0.crate'
        self.write('dist/tessera-0.1.0.crate', 'crate bytes')
        self.git('update-ref', 'refs/remotes/origin/main', 'HEAD')

    def remote(self, kind='tag', revision=None):
        def check(method, url, data=None, content_type=None):
            if '/git/ref/tags/' in url:
                return {'object': {'type': kind, 'sha': (revision or self.revision) if kind == 'commit' else 'c' * 40}}
            return {'object': {'type': 'commit', 'sha': revision or self.revision}}
        return check

    def test_the_remote_tag_can_be_lightweight_or_annotated_and_must_be_unmoved(self):
        for kind in ['tag', 'commit']:
            with self.subTest(kind=kind):
                with patch.object(release, 'api', side_effect=self.remote(kind=kind)) as api:
                    release.check_remote_tag(self.REPO, 'v0.1.0', self.revision)
                    self.assertEqual(api.call_count, 2 if kind == 'tag' else 1)
                with patch.object(release, 'api', side_effect=self.remote(kind=kind, revision='d' * 40)), \
                        self.assertRaisesRegex(ValueError, 'moved'):
                    release.check_remote_tag(self.REPO, 'v0.1.0', self.revision)

    def test_a_remote_tag_must_name_a_commit(self):
        with patch.object(release, 'api', side_effect=self.remote(kind='tree')), \
                self.assertRaisesRegex(ValueError, 'moved'):
            release.check_remote_tag(self.REPO, 'v0.1.0', self.revision)

    def test_a_release_with_assets_is_never_changed(self):
        for draft in [False, True]:
            existing = {'tag_name': 'v0.1.0', 'draft': draft, 'assets': [{'name': 'existing'}]}
            with self.subTest(draft=draft), patch.object(release, 'check_remote_tag'), \
                    patch.object(release, 'api', return_value=[existing]) as api:
                with self.assertRaisesRegex(ValueError, 'already has assets'):
                    release.publish(self.root, 'v0.1.0', self.revision, self.crate, self.REPO)
                self.assertEqual([call.args[0] for call in api.call_args_list], ['GET'])

    def test_multiple_releases_for_a_tag_are_refused_across_pages(self):
        first = [{'tag_name': 'v0.1.0', 'id': 7}] + [{'tag_name': 'v0.2.0'}] * 99
        second = [{'tag_name': 'v0.1.0', 'id': 8}]
        with patch.object(release, 'check_remote_tag'), \
                patch.object(release, 'api', side_effect=[first, second]) as api:
            with self.assertRaisesRegex(ValueError, 'multiple releases'):
                release.publish(self.root, 'v0.1.0', self.revision, self.crate, self.REPO)
            self.assertEqual([call.args[0] for call in api.call_args_list], ['GET', 'GET'])
            self.assertTrue(api.call_args_list[-1].args[1].endswith('page=2'))

    def publication_api(self, existing=None, failure=None):
        state = dict(existing) if existing is not None else {
            'id': 7, 'draft': True, 'tag_name': 'v0.1.0', 'assets': []}
        calls = []

        def api(method, url, data=None, content_type='application/json'):
            calls.append((method, url, data))
            if method == 'GET' and '/releases?' in url:
                return [dict(existing)] if existing is not None else []
            if method == 'POST' and url.endswith('/releases'):
                self.assertIsNone(existing)
                self.assertTrue(data['draft'])
                self.assertEqual(data['target_commitish'], self.revision)
                self.assertEqual(data['name'], 'tessera 0.1.0')
                self.assertIn('- engine: first', data['body'])
                self.assertIn(self.revision, data['body'])
                state.update(data)
            elif method == 'POST':
                self.assertEqual(content_type, 'application/octet-stream')
                name = parse_qs(urlparse(url).query)['name'][0]
                state['assets'].append({'name': name, 'state': 'uploaded', 'size': len(data),
                                        'digest': 'sha256:' + hashlib.sha256(data).hexdigest()})
            elif method == 'GET':
                if failure == 'missing':
                    state['assets'].pop()
                elif failure == 'duplicate':
                    state['assets'].append(dict(state['assets'][0]))
                elif failure == 'size':
                    state['assets'][0]['size'] += 1
                elif failure == 'digest':
                    state['assets'][0]['digest'] = 'sha256:' + '0' * 64
                elif failure == 'no-digest':
                    state['assets'][0].pop('digest')
                elif failure == 'state':
                    state['assets'][0]['state'] = 'new'
                elif failure == 'name':
                    state['assets'][0]['name'] = 'unexpected'
                elif failure == 'tag':
                    state['tag_name'] = 'v0.2.0'
                elif failure == 'draft':
                    state['draft'] = not state['draft']
            elif method == 'PATCH':
                self.assertNotIn('make_latest', data)
                state.update(data)
            return dict(state)

        return api, calls, state

    def test_an_asset_free_release_is_adopted_and_only_a_draft_is_published(self):
        for draft in [False, True]:
            with self.subTest(draft=draft):
                existing = {'id': 7, 'draft': draft, 'tag_name': 'v0.1.0', 'assets': [],
                            'name': 'Human title', 'body': 'Human notes'}
                api, calls, state = self.publication_api(existing)
                with patch.object(release, 'check_remote_tag') as remote, \
                        patch.object(release, 'api', side_effect=api):
                    url = release.publish(self.root, 'v0.1.0', self.revision, self.crate, self.REPO)
                self.assertEqual(url, 'https://github.com/owner/tessera/releases/tag/v0.1.0')
                self.assertEqual(state['name'], 'Human title')
                self.assertEqual(state['body'], 'Human notes')
                self.assertFalse(state['draft'])
                self.assertEqual(len(state['assets']), 2)
                self.assertEqual([d for m, _, d in calls if m == 'PATCH'], [{'draft': False}] if draft else [])
                self.assertFalse(any(m == 'POST' and u.endswith('/releases') for m, u, _ in calls))
                self.assertEqual(remote.call_count, 2)

    def test_an_adopted_release_with_an_empty_body_gets_changelog_notes(self):
        for draft in [False, True]:
            for body in ['', None]:
                with self.subTest(draft=draft, body=body):
                    existing = {'id': 7, 'draft': draft, 'tag_name': 'v0.1.0', 'assets': [],
                                'name': 'Human title', 'body': body}
                    api, calls, state = self.publication_api(existing)
                    with patch.object(release, 'check_remote_tag'), patch.object(release, 'api', side_effect=api):
                        release.publish(self.root, 'v0.1.0', self.revision, self.crate, self.REPO)
                    self.assertEqual(state['name'], 'Human title')
                    self.assertEqual(state['body'], '- engine: first')
                    patches = [d for m, _, d in calls if m == 'PATCH']
                    self.assertEqual(patches, [{'body': '- engine: first'}] + ([{'draft': False}] if draft else []))

    def test_the_crate_must_be_this_version(self):
        other = self.root / 'dist/tessera-0.2.0.crate'
        other.write_text('x')
        with patch.object(release, 'api') as api, self.assertRaisesRegex(ValueError, 'expected'):
            release.publish(self.root, 'v0.1.0', self.revision, other, self.REPO)
        api.assert_not_called()

    def test_publication_uploads_the_crate_and_checksums_with_the_notes(self):
        for failure in [None, 'missing', 'duplicate', 'size', 'digest', 'no-digest', 'state', 'name', 'tag', 'draft']:
            with self.subTest(failure=failure):
                api, calls, _ = self.publication_api(failure=failure)
                with patch.object(release, 'check_remote_tag'), patch.object(release, 'api', side_effect=api):
                    if failure:
                        with self.assertRaises(ValueError):
                            release.publish(self.root, 'v0.1.0', self.revision, self.crate, self.REPO)
                        self.assertNotIn('PATCH', [call[0] for call in calls])
                        continue
                    url = release.publish(self.root, 'v0.1.0', self.revision, self.crate, self.REPO)
                self.assertEqual(url, 'https://github.com/owner/tessera/releases/tag/v0.1.0')
                uploads = {parse_qs(urlparse(u).query)['name'][0]: d for m, u, d in calls if 'uploads.' in u}
                digest = hashlib.sha256(b'crate bytes').hexdigest()
                self.assertEqual(uploads, {'tessera-0.1.0.crate': b'crate bytes',
                                           'SHA256SUMS': f'{digest}  tessera-0.1.0.crate\n'.encode()})


class Pins(unittest.TestCase):
    """The toolchain and MSRV agree wherever they are written down."""

    def read(self, name):
        return (REPO / name).read_text()

    def test_toolchain_and_msrv_pins_agree(self):
        channel = re.search(r'^channel = "(.*)"$', self.read('rust-toolchain.toml'), re.M).group(1)
        msrv = re.search(r'^rust-version = "(.*)"$', self.read('Cargo.toml'), re.M).group(1)
        ci = self.read('.github/workflows/ci.yml')
        toolchains = set(re.findall(r'toolchain: (\S+)', ci))
        self.assertEqual(toolchains, {channel, msrv + '.0'})
        self.assertIn(f'FROM rust:{channel}-', self.read('scripts/validate.Dockerfile'))
        self.assertIn(f'tessera-validate:{channel}', self.read('scripts/validate.sh'))

    def test_every_action_is_pinned_to_a_commit(self):
        for name in ['ci.yml', 'release.yml']:
            for use in re.findall(r'uses: (\S+)(.*)', self.read('.github/workflows/' + name)):
                with self.subTest(workflow=name, action=use[0]):
                    if use[0].startswith('./'):
                        continue
                    self.assertRegex(use[0], r'@[0-9a-f]{40}$')
                    self.assertRegex(use[1], r'^ # \S')


if __name__ == '__main__':
    unittest.main()
