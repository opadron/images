import argparse
import copy
import datetime
import os
import os.path
import subprocess
import sys
import time
import yaml

parser = argparse.ArgumentParser()
parser.add_argument('--repo', help='repo to monitor', required=True)
parser.add_argument('--pre-branch', help='pre branch', required=True)
parser.add_argument('--post-branch', help='post branch', required=True)
parser.add_argument('--interval', help='polling interval', type=int, default=300)
parser.add_argument('--deploy-key', help='path to deployment key', required=True)
parser.add_argument('--user-email', help='email to commit as', required=True)
parser.add_argument('--user-name', help='user name to commit as', required=True)
parser.add_argument('--source-dir',
                    help='directory from which to source manifests',
                    required=True)
parser.add_argument('--destination-dir',
                    help='directory to which to push generated manifests',
                    required=True)
parser.add_argument('--storage-dir',
                    help='directory to use for persistent storage',
                    required=True)
args = parser.parse_args()


entries_map = {}
special_map = {}
class ParsedEntry:
    def __init__(self, obj, update=True):
        global entries_map
        global special_map

        api_version = obj.get('apiVersion', None)
        if not api_version:
            raise ValueError('field missing: "apiVersion"')

        kind = obj.get('kind', None)
        if not kind:
            raise ValueError('field missing: "kind"')

        metadata = obj.get('metadata', None)
        if not metadata:
            raise ValueError('field missing: "metadata"')

        name = metadata.get('name', None)
        if not name:
            raise ValueError('field missing: "metadata.name"')

        namespace = metadata.get('namespace', 'default')

        annotations = metadata.get('annotations', None)
        ignored = (annotations and annotations.get('fluxcd.io/ignore', False))

        tokens = api_version.split('/')
        api_group, api_version = (
                ('builtin', tokens[0]) if len(tokens) == 1 else
                tokens)

        self.api_group = api_group
        self.api_version = api_version
        self.kind = kind
        self.name = name
        self.namespace = namespace
        self.obj = obj
        self.ignored = ignored

        key = (api_group, api_version, kind, namespace, name)
        if update:
            entries_map[key] = self

        is_special = (kind == 'ConfigMap' and annotations)
        if is_special:
            dr = annotations.get('cd.spack.io/derive-resource', False).lower()
            is_special = (dr not in ('false', '0', 'off', 'no', 'disabled'))

        if is_special and update:
            special_map[key] = self


def log(*args):
    sys.stdout.write('\n')
    sys.stdout.write('\x1b[1;33m[')
    sys.stdout.write(str(datetime.datetime.now()))
    sys.stdout.write('] \x1b[1;34m')
    sys.stdout.write(*args)
    sys.stdout.write('\x1b[0m\n')


def apply_patch(obj, patch):
    for p in patch:
        op = p.get('op', None)
        path = p.get('path', None)
        val = p.get('value', None)

        if not op: continue
        if not path: continue

        ptr = obj
        key = None
        if path:
            if path == '/':
                key = ''
            else:
                tokens = path.split('/')[1:]
                tokens, key = tokens[:-1], tokens[-1]
                for t in tokens:
                    if isinstance(ptr, list):
                        t = int(t)
                    ptr = ptr[t]

        if op == 'add':
            if isinstance(ptr, list):
                if key == '-':
                    ptr.append(val)
                else:
                    key = int(key)
                    ptr.insert(key, val)
            else:
                ptr[key].update(val)
        elif op == 'remove':
            if isinstance(ptr, list):
                if key == '-':
                    ptr.pop()
                else:
                    key = int(key)
                    ptr.remove(key)
            else:
                ptr.pop(key)
        elif op == 'replace':
            if isinstance(ptr, list):
                if key == '-':
                    ptr[-1] = val
                else:
                    key = int(key)
                    ptr[key] = val
            else:
                ptr[key] = val
        else:
            continue  # TODO(opadron): finish this if we ever start caring
                      #                about copy, move, or test


def read_scalar_from_path(f):
    result = None
    if os.path.exists(f):
        with open(f) as fid:
            result = fid.read().strip()
    return result


def write_scalar_to_path(f, val):
    if not isinstance(val, bytes):
        val = bytes(str(val), 'UTF-8')

    with open(f, 'w') as fid:
        fid.buffer.write(val)


def iter_manifests(path):
    for prefix, _, files in os.walk(path):
        for filename in files:
            is_manifest = (filename.endswith('.yaml') or
                           filename.endswith('.yml') or
                           filename.endswith('.json'))

            if not is_manifest:
                continue

            with open(os.path.join(prefix, filename)) as f:
                for obj in yaml.full_load_all(f):
                    yield ParsedEntry(obj)


def process_patch(patch, env):
    if isinstance(patch, list):
        return [process_patch(p, env) for p in patch]

    if isinstance(patch, dict):
        return {process_patch(k, env): process_patch(v, env)
                for k, v in patch.items()}

    if isinstance(patch, str):
        return patch.format(ENV=env)

    return patch


key_file = os.path.abspath(args.deploy_key)
git_env = {'GIT_SSH_COMMAND': ('ssh '
                               '-o StrictHostKeyChecking=no '
                               '-o UserKnownHostsFile=/dev/null '
                               f'-i {key_file}')}
def git(*args, **kwargs):
    capture = kwargs.pop('capture', False)
    new_env = {}
    new_env.update(os.environ)
    new_env.update(git_env)
    new_env.update(kwargs.pop('env', {}))
    kwargs['env'] = new_env

    args = ['git'] + list(args)
    if capture:
        return subprocess.check_output(args, **kwargs).decode('UTF-8')

    subprocess.check_call(args, **kwargs)


repo_dir = os.path.join(args.storage_dir, 'repo')
last_pre_file = os.path.join(args.storage_dir, 'last-pre')
last_post_file = os.path.join(args.storage_dir, 'last-post')

git('config', '--global', 'user.name', args.user_name)
git('config', '--global', 'user.email', args.user_email)

last_pre_hash = read_scalar_from_path(last_pre_file)
last_post_hash = read_scalar_from_path(last_post_file)

waited_last_iter = False

while True:
    start_time = time.time()

    # if repo exists, fetch
    if os.path.exists(repo_dir):
        git('-C', repo_dir,
            'fetch', 'origin',
            args.pre_branch, args.post_branch,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)

    # if the repo is not yet cloned, clone it
    else:
        log('Cloning Upstream')
        os.makedirs(repo_dir)
        git('clone', args.repo, repo_dir)

    current_pre_hash = git('-C', repo_dir, 'rev-list', '-n', '1',
                             f'origin/{args.pre_branch}', capture=True).strip()
    current_post_hash = git('-C', repo_dir, 'rev-list', '-n', '1',
                            f'origin/{args.post_branch}', capture=True).strip()

    pre_up_to_date = (last_pre_hash is not None and
                      last_pre_hash == current_pre_hash)

    post_up_to_date = (last_post_hash is not None and
                      last_post_hash == current_post_hash)

    update_pre = False
    update_post = False

    if pre_up_to_date and post_up_to_date and not waited_last_iter:
        log('waiting for updates')
        waited_last_iter = True

    if not post_up_to_date:
        waited_last_iter = False
        log('syncing downstream branch')
        git('-C', repo_dir, 'checkout', args.post_branch)
        git('-C', repo_dir, 'reset', '--hard', f'origin/{args.post_branch}')
        update_post = True

    if not pre_up_to_date:
        waited_last_iter = False
        log('syncing upstream branch')
        git('-C', repo_dir, 'checkout', args.pre_branch)
        git('-C', repo_dir, 'reset', '--hard', f'origin/{args.pre_branch}')

        entries_map = {}
        special_map = {}

        log('processing base manifests')
        for entry in iter_manifests(os.path.join(repo_dir, args.source_dir)):
            pass
        print('(done)')

        log('checking out downstream branch')
        git('-C', repo_dir, 'checkout', args.post_branch)
        git('-C', repo_dir, 'reset', '--hard', f'origin/{args.post_branch}')

        log('generating manifests')

        # initial house keeping
        local_prod_slug = os.path.join(args.destination_dir, 'production')
        prod_dir = os.path.join(repo_dir, local_prod_slug)

        local_stage_slug = os.path.join(args.destination_dir, 'staging')
        stage_dir = os.path.join(repo_dir, local_stage_slug)

        if os.path.isdir(prod_dir):
            try:
                git('-C', repo_dir, 'rm', '-rf', local_prod_slug)
            except subprocess.CalledProcessError:
                pass
        os.makedirs(prod_dir)

        if os.path.isdir(stage_dir):
            try:
                git('-C', repo_dir, 'rm', '-rf', local_stage_slug)
            except subprocess.CalledProcessError:
                pass
        os.makedirs(stage_dir)

        # main production section
        for entry in entries_map.values():
            if entry.ignored:
                continue

            filename = '.'.join((
                            '-'.join((entry.api_group,
                                      entry.api_version,
                                      entry.kind,
                                      entry.namespace,
                                      entry.name)),
                            'yaml'))

            local_filepath = os.path.join(local_prod_slug, filename)
            filepath = os.path.join(prod_dir, filename)

            local_display_path = os.path.relpath(local_filepath,
                                                 args.destination_dir)
            print(f'  + {local_display_path}')
            with open(filepath, 'w') as f:
                f.write('---\n')
                yaml.dump(entry.obj, f)

            git('-C', repo_dir, 'add', local_filepath)

        # staging section
        for entry in special_map.values():
            if entry.ignored:
                continue

            tokens = entry.obj['data']['apiVersion'].split('/')
            api_group, api_version = (
                    ('builtin', tokens[0]) if len(tokens) == 1 else tokens)
            kind = entry.obj['data']['kind']
            name = entry.obj['data']['name']
            namespace = entry.namespace

            ref = entries_map[(api_group, api_version, kind, namespace, name)]
            patch = process_patch(yaml.full_load(entry.obj['data']['patch']),
                                  env='staging')

            new_obj = copy.deepcopy(ref.obj)
            apply_patch(new_obj, patch)
            new_obj = ParsedEntry(new_obj, update=False)

            if new_obj.ignored:
                continue

            filename = '.'.join((
                            '-'.join((new_obj.api_group,
                                      new_obj.api_version,
                                      new_obj.kind,
                                      new_obj.namespace,
                                      new_obj.name)),
                            'yaml'))

            local_filepath = os.path.join(local_stage_slug, filename)
            filepath = os.path.join(stage_dir, filename)

            local_display_path = os.path.relpath(local_filepath,
                                                 args.destination_dir)
            print(f'  + {local_display_path}')
            with open(filepath, 'w') as f:
                f.write('---\n')
                yaml.dump(new_obj.obj, f)

            git('-C', repo_dir, 'add', local_filepath)

        log('committing & pushing')
        commit_msg = f'update from ref: {current_pre_hash}'
        git('-C', repo_dir, 'commit', '--allow-empty', '-m', commit_msg)
        git('-C', repo_dir, 'push', 'origin',
                ':'.join((args.post_branch, args.post_branch)))

        current_post_hash = git('-C', repo_dir, 'rev-list', '-n', '1',
                                f'origin/{args.post_branch}',
                                capture=True).strip()

        update_pre = True
        update_post = True

    if update_pre:
        write_scalar_to_path(last_pre_file, current_pre_hash)
        last_pre_hash = current_pre_hash

    if update_post:
        write_scalar_to_path(last_post_file, current_post_hash)
        last_post_hash = current_post_hash

    elapsed_time = time.time() - start_time
    sleep_time = args.interval - elapsed_time
    if sleep_time > 0:
        time.sleep(sleep_time)
