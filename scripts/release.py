#!/usr/bin/env python3
"""Prepare, check and publish Tessera releases. Standard library only (3.9+).

  release-prep X.Y.Z [--dry-run FILE]   bump versions, prepend the changelog, commit
  check-tag --tag vX.Y.Z                validate a release tag against the tree
  notes --version X.Y.Z                 print a version's changelog section
  check-package                         check the .crate holds only consumer files
  publish --tag --revision --crate      attach GitHub Release assets (CI only)
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
NAME = 'tessera'
LOCKFILES = ('Cargo.lock', 'tools/bench/Cargo.lock')
SEMVER = r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'
GROUPS = ('Features', 'Fixes', 'Performance', 'API', 'Other')
# Commit types left out of the changelog unless scoped `api`.
SKIPPED = {'test', 'ci', 'chore', 'docs'}
SUBJECT = re.compile(r'(?P<type>[a-z]+)(?:\((?P<scope>[a-z0-9-]+)\))?(?P<breaking>!)?: (?P<summary>.+)')
# What a consumer builds and reads; Cargo adds the first four.
PACKAGE_FILES = {'Cargo.toml', 'Cargo.toml.orig', 'Cargo.lock', '.cargo_vcs_info.json',
                 'README.md', 'LICENSE', 'CHANGELOG.md', 'ARCHITECTURE.md'}
PACKAGE_DIRS = ('src/', 'tests/', 'examples/')
PACKAGE_REQUIRED = PACKAGE_FILES - {'.cargo_vcs_info.json'} | {'src/lib.rs'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def git(root, *args):
    return subprocess.check_output(['git', *args], cwd=root, text=True).strip()


def parse_version(version):
    require(re.fullmatch(SEMVER, version), f'version must be X.Y.Z without leading zeroes: {version}')
    return tuple(int(part) for part in version.split('.'))


def manifest_version(root):
    text = (root / 'Cargo.toml').read_text()
    match = re.search(r'^\[package\]\n(?:(?!\[).*\n)*?version = "([^"]*)"$', text, re.M)
    require(match, 'Cargo.toml has no [package] version')
    return match.group(1)


def lock_versions(path, name=NAME):
    """Versions of the local (sourceless) `name` entries in a lockfile."""
    versions = []
    for entry in path.read_text().split('[[package]]')[1:]:
        fields = dict(re.findall(r'^(\w+) = "([^"]*)"$', entry, re.M))
        if fields.get('name') == name and 'source' not in fields:
            versions.append(fields.get('version'))
    return versions


def validate_version(root, tag):
    require(re.fullmatch('v' + SEMVER, tag), f'release tag must be vX.Y.Z without leading zeroes: {tag}')
    version = tag[1:]
    require(manifest_version(root) == version,
            f'tag {tag} does not match Cargo.toml version {manifest_version(root)}')
    for lockfile in LOCKFILES:
        require(lock_versions(root / lockfile) == [version], f'{lockfile} is not at version {version}')
    return version


def changelog_section(text, version):
    """The body of `## version`, or None when there is no such section."""
    match = re.search(rf'^## {re.escape(version)}(?: [^\n]*)?\n(.*?)(?=^## |\Z)', text, re.M | re.S)
    return match.group(1).strip() if match else None


def check_tag(root, tag, revision=None, main_ref='origin/main'):
    version = validate_version(root, tag)
    ref = f'refs/tags/{tag}'
    commit = git(root, 'rev-parse', '--verify', ref + '^{commit}')
    require(git(root, 'rev-parse', 'HEAD') == commit, f'checkout is not the commit {tag} names')
    if revision:
        require(revision == commit, f'{tag} names {commit}, not the triggering {revision}')
    reachable = subprocess.run(['git', 'merge-base', '--is-ancestor', commit, main_ref],
                               cwd=root).returncode == 0
    require(reachable, f'{tag} is not reachable from {main_ref}')
    require(changelog_section((root / 'CHANGELOG.md').read_text(), version),
            f'CHANGELOG.md has no section for {version}')
    return version, commit


def check_package(listing):
    files = [line for line in listing.splitlines() if line]
    extra = [path for path in files if path not in PACKAGE_FILES and not path.startswith(PACKAGE_DIRS)]
    require(not extra, 'package holds files a consumer does not need: ' + ', '.join(extra))
    missing = sorted(PACKAGE_REQUIRED - set(files))
    require(not missing, 'package is missing: ' + ', '.join(missing))
    return files


def commits_since_tag(root):
    """(hash, subject) pairs since the previous vX.Y.Z tag, oldest first."""
    described = subprocess.run(['git', 'describe', '--tags', '--abbrev=0', '--match', 'v[0-9]*', 'HEAD'],
                               cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    span = [f'{described.stdout.strip()}..HEAD'] if described.returncode == 0 else ['HEAD']
    log = git(root, 'log', '--reverse', '--no-merges', '--format=%h%x1f%s', *span)
    return [tuple(line.split('\x1f', 1)) for line in log.splitlines() if line]


def group_of(subject):
    match = SUBJECT.fullmatch(subject)
    if not match:
        return 'Other'
    kind, scope = match['type'], match['scope']
    if scope == 'api':
        return 'API'
    if kind in SKIPPED:
        return None
    return {'feat': 'Features', 'fix': 'Fixes', 'perf': 'Performance'}.get(kind, 'Other')


def entry(commit, subject):
    match = SUBJECT.fullmatch(subject)
    if not match:
        return f'- {subject} ({commit})'
    scope = f"{match['scope']}: " if match['scope'] else ''
    breaking = ' **(breaking)**' if match['breaking'] else ''
    return f"- {scope}{match['summary']}{breaking} ({commit})"


def changelog_entries(commits, version, date, unreleased=''):
    grouped = {group: [] for group in GROUPS}
    for commit, subject in commits:
        group = group_of(subject)
        if group:
            grouped[group].append(entry(commit, subject))
    lines = [f'## {version} - {date}', '']
    if unreleased:
        lines += [unreleased, '']
    for group in GROUPS:
        if grouped[group]:
            lines += [f'### {group}', '', *grouped[group], '']
    require(len(lines) > 2, f'no changes for {version}: nothing since the previous tag')
    return '\n'.join(lines)


def prepend_section(text, version, section):
    """Moves Unreleased notes into the new section, which goes below it."""
    match = re.search(r'^## Unreleased\n(.*?)(?=^## |\Z)', text, re.M | re.S)
    require(match, 'CHANGELOG.md has no "## Unreleased" heading')
    rest = text[match.end():]
    return (text[:match.start()] + '## Unreleased\n\n' + section.rstrip() + '\n\n' + rest).rstrip() + '\n'


def unreleased_notes(text):
    match = re.search(r'^## Unreleased\n(.*?)(?=^## |\Z)', text, re.M | re.S)
    return match.group(1).strip() if match else ''


def bump(root, old, new):
    path = root / 'Cargo.toml'
    text = path.read_text()
    path.write_text(re.sub(r'^(\[package\]\n(?:(?!\[).*\n)*?version = )"' + re.escape(old) + '"$',
                           lambda m: f'{m.group(1)}"{new}"', text, count=1, flags=re.M))
    for lockfile in LOCKFILES:
        path = root / lockfile
        path.write_text(re.sub(rf'(\[\[package\]\]\nname = "{NAME}"\nversion = )"{re.escape(old)}"',
                               lambda m: f'{m.group(1)}"{new}"', path.read_text()))


def release_prep(root, version, dry_run=None, date=None, out=sys.stdout):
    date = date or datetime.date.today().isoformat()
    current = manifest_version(root)
    require(parse_version(version) >= parse_version(current),
            f'{version} is older than the current version {current}')
    tag = f'v{version}'
    require(not git(root, 'tag', '--list', tag), f'{tag} already exists')
    changelog = (root / 'CHANGELOG.md').read_text()
    require(changelog_section(changelog, version) is None, f'CHANGELOG.md already has {version}')
    section = changelog_entries(commits_since_tag(root), version, date, unreleased_notes(changelog))
    if dry_run:
        Path(dry_run).write_text(section + '\n')
        change = f'keep {current}' if version == current else f'set {current} -> {version} in Cargo.toml, ' + ', '.join(LOCKFILES)
        print(f'dry run: would {change}, add this section to CHANGELOG.md and commit; '
              f'section written to {dry_run}', file=out)
        return section
    require(not git(root, 'status', '--porcelain'), 'the working tree must be clean')
    if version != current:
        bump(root, current, version)
    (root / 'CHANGELOG.md').write_text(prepend_section(changelog, version, section))
    validate_version(root, tag)
    git(root, 'add', 'Cargo.toml', *LOCKFILES, 'CHANGELOG.md')
    git(root, 'commit', '--quiet', '-m', f'chore: release {version}')
    print(f'Committed "chore: release {version}". Review it and merge it to main through the usual pull request.\n'
          f'Then create the {tag} release in the GitHub Releases UI targeting main.', file=out)
    return section


def api(method, url, data=None, content_type='application/json'):
    token = os.environ.get('GH_TOKEN')
    require(token, 'GH_TOKEN is required')
    if isinstance(data, dict):
        data = json.dumps(data).encode()
    request = Request(url, data=data, method=method, headers={
        'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json',
        'Content-Type': content_type, 'X-GitHub-Api-Version': '2022-11-28'})
    with urlopen(request, timeout=60) as response:
        return json.load(response)


def check_remote_tag(repository, tag, revision):
    """The lightweight or annotated tag on GitHub still names `revision`."""
    base = f'https://api.github.com/repos/{repository}/git'
    ref = api('GET', f'{base}/ref/tags/{quote(tag)}')['object']
    while ref['type'] == 'tag':
        ref = api('GET', f"{base}/tags/{ref['sha']}")['object']
    require(ref['type'] == 'commit' and ref['sha'] == revision, f'remote {tag} moved from {revision}')


def publish(root, tag, revision, crate, repository):
    version, commit = check_tag(root, tag, revision)
    crate = Path(crate)
    require(crate.name == f'{NAME}-{version}.crate' and crate.is_file(),
            f'expected {NAME}-{version}.crate, not {crate}')
    data = crate.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    sums = f'{digest}  {crate.name}\n'.encode()
    assets = {crate.name: data, 'SHA256SUMS': sums}
    notes = changelog_section((root / 'CHANGELOG.md').read_text(), version)
    check_remote_tag(repository, tag, commit)
    base = f'https://api.github.com/repos/{repository}/releases'
    # Listing with the publisher's token includes drafts.
    existing = []
    page = 1
    while True:
        releases = api('GET', f'{base}?per_page=100&page={page}')
        existing.extend(release for release in releases if release['tag_name'] == tag)
        if len(releases) < 100:
            break
        page += 1
    require(len(existing) <= 1, f'multiple releases exist for {tag}; inspect them before retrying')
    if existing:
        release = existing[0]
        require(not release.get('assets'), f'release {tag} already has assets; inspect it before retrying')
        was_draft = bool(release['draft'])
        if not release.get('body'):
            api('PATCH', f"{base}/{release['id']}", {'body': notes})
    else:
        release = api('POST', base, {'tag_name': tag, 'target_commitish': commit, 'name': f'{NAME} {version}',
                                    'body': f'{notes}\n\nSource revision: `{commit}`.', 'draft': True})
        require(release['draft'] and release['tag_name'] == tag, 'unexpected draft release identity')
        was_draft = True
    release_url = f"{base}/{release['id']}"
    uploads = f"https://uploads.github.com/repos/{repository}/releases/{release['id']}/assets?"
    for name, body in assets.items():
        api('POST', uploads + urlencode({'name': name}), body, 'application/octet-stream')
    release = api('GET', release_url)
    require(release['tag_name'] == tag and bool(release['draft']) == was_draft, 'release identity changed')
    uploaded = {asset['name']: asset for asset in release['assets']}
    require(len(release['assets']) == len(assets) and set(uploaded) == set(assets),
            'uploaded release assets are incomplete or unexpected')
    for name, body in assets.items():
        asset = uploaded[name]
        require(asset['state'] == 'uploaded' and asset['size'] == len(body)
                and asset.get('digest') == 'sha256:' + hashlib.sha256(body).hexdigest(),
                f'uploaded asset verification failed: {name}')
    check_remote_tag(repository, tag, commit)
    if was_draft:
        result = api('PATCH', release_url, {'draft': False})
        require(not result['draft'] and result['tag_name'] == tag, 'release publication was not confirmed')
    return f'https://github.com/{repository}/releases/tag/{tag}'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    prep = commands.add_parser('release-prep')
    prep.add_argument('version')
    prep.add_argument('--dry-run', metavar='FILE', help='write the changelog section here; change nothing')
    tag = commands.add_parser('check-tag')
    tag.add_argument('--tag', required=True)
    tag.add_argument('--revision')
    tag.add_argument('--main-ref', default='origin/main')
    notes = commands.add_parser('notes')
    notes.add_argument('--version', required=True)
    commands.add_parser('check-package')
    release = commands.add_parser('publish')
    release.add_argument('--tag', required=True)
    release.add_argument('--revision', required=True)
    release.add_argument('--crate', required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'release-prep':
            release_prep(ROOT, args.version, args.dry_run)
        elif args.command == 'check-tag':
            version, commit = check_tag(ROOT, args.tag, args.revision, args.main_ref)
            print(f'{args.tag}: version {version}, commit {commit}')
        elif args.command == 'notes':
            section = changelog_section((ROOT / 'CHANGELOG.md').read_text(), args.version)
            require(section, f'CHANGELOG.md has no section for {args.version}')
            print(section)
        elif args.command == 'check-package':
            listing = subprocess.check_output(['cargo', 'package', '--list', '--locked'],
                                              cwd=ROOT, text=True)
            files = check_package(listing)
            print(f'package: {len(files)} files, all for consumers')
        else:
            repository = os.environ.get('GITHUB_REPOSITORY')
            require(repository, 'GITHUB_REPOSITORY is required')
            print(publish(ROOT, args.tag, args.revision, args.crate, repository))
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f'release: {error}\n')


if __name__ == '__main__':
    main()
