import argparse
import copy
import datetime
import os
import os.path
import subprocess
import shutil
import sys
import time
import yaml

parser = argparse.ArgumentParser()
parser.add_argument('--repo', help='repo to monitor', required=True)

parser.add_argument('--staging-branch',
                    help='staging branch',
                    required=True)
parser.add_argument('--staging-dir',
                    help=('directory from which to source '
                          'manifests on the staging branch'),
                    required=True)

parser.add_argument('--production-branch',
                    help='production branch',
                    required=True)
parser.add_argument('--production-dir',
                    help=('directory from which to source '
                          'manifests on the production branch'),
                    required=True)

parser.add_argument('--target-branch', help='target branch', required=True)
parser.add_argument('--target-dir',
                    help=('directory in which to store generated '
                          'manifests on the target branch')
                    , required=True)

parser.add_argument('--interval',
                    help='polling interval',
                    type=int, default=300)
parser.add_argument('--deploy-key',
                    help='path to deployment key',
                    required=True)
parser.add_argument('--user-email', help='email to commit as', required=True)
parser.add_argument('--user-name', help='user name to commit as', required=True)
parser.add_argument('--storage-dir',
                    help='directory to use for persistent storage',
                    required=True)
args = parser.parse_args()


entries_map = {}
special_map = {}
class ParsedEntry:
    def __init__(self, obj, env, update=True):
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
            entries_map[env][key] = self

        is_special = (kind == 'ConfigMap' and annotations)
        if is_special:
            dr = annotations.get('cd.spack.io/derive-resource', 'false').lower()
            is_special = (dr not in ('false', '0', 'off', 'no', 'disabled'))

        if is_special and update:
            special_map[env][key] = self


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


def iter_manifests(path, env):
    for prefix, _, files in os.walk(path):
        for filename in files:
            is_manifest = (filename.endswith('.yaml') or
                           filename.endswith('.yml') or
                           filename.endswith('.json'))

            if not is_manifest:
                continue

            with open(os.path.join(prefix, filename)) as f:
                for obj in yaml.full_load_all(f):
                    yield ParsedEntry(obj, env)


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
def gitC(*args, **kwargs):
    return git('-C', repo_dir, *args, **kwargs)


def rev_list(branch):
    return gitC('rev-list', '-n', '1', f'origin/{branch}', capture=True).strip()


def hard_sync(branch):
    gitC('checkout', branch)
    gitC('reset', '--hard', f'origin/{branch}')


def clear_git_dir(rel_path):
    path = os.path.join(repo_dir, rel_path)
    if os.path.isdir(path):
        try:
            gitC('rm', '-rf', rel_path)
        except subprocess.CalledProcessError:
            shutil.rmtree(path)
    os.makedirs(path)

last_staging_file = os.path.join(args.storage_dir, 'last-staging')
last_production_file = os.path.join(args.storage_dir, 'last-production')
last_target_file = os.path.join(args.storage_dir, 'last-target')

git('config', '--global', 'user.name', args.user_name)
git('config', '--global', 'user.email', args.user_email)

last_staging_hash = read_scalar_from_path(last_staging_file)
last_production_hash = read_scalar_from_path(last_production_file)
last_target_hash = read_scalar_from_path(last_target_file)

waited_last_iter = False

while True:
    start_time = time.time()

    # if repo exists, fetch
    if os.path.exists(repo_dir):
        gitC('fetch', 'origin',
             args.staging_branch,
             args.production_branch,
             args.target_branch,
             stdout=subprocess.DEVNULL,
             stderr=subprocess.DEVNULL)

    # if the repo is not yet cloned, clone it
    else:
        log('Cloning Upstream')
        os.makedirs(repo_dir)
        git('clone', args.repo, repo_dir)

    current_staging_hash = rev_list(args.staging_branch)
    current_production_hash = rev_list(args.production_branch)
    current_target_hash = rev_list(args.target_branch)

    staging_up_to_date = (last_staging_hash is not None and
                          last_staging_hash == current_staging_hash)
    production_up_to_date = (last_production_hash is not None and
                             last_production_hash == current_production_hash)
    target_up_to_date = (last_target_hash is not None and
                         last_target_hash == current_target_hash)

    update_staging = False
    update_production = False
    update_target = False

    all_up_to_date = (staging_up_to_date and
                      production_up_to_date and
                      target_up_to_date)

    if all_up_to_date and not waited_last_iter:
        log('waiting for updates')
        waited_last_iter = True

    if not target_up_to_date:
        waited_last_iter = False
        log('syncing target branch')
        hard_sync(args.target_branch)
        update_target = True

    if not production_up_to_date:
        waited_last_iter = False
        log('syncing production branch')
        hard_sync(args.production_branch)
        entries_map['production'] = {}
        special_map['production'] = {}
        log('processing production manifests')
        manifests = iter_manifests(os.path.join(repo_dir, args.production_dir),
                                   'production')
        for entry in manifests:
            pass
        print('(done)')

    if not staging_up_to_date:
        waited_last_iter = False
        log('syncing staging branch')
        hard_sync(args.staging_branch)
        entries_map['staging'] = {}
        special_map['staging'] = {}

        log('processing staging manifests')
        manifests = iter_manifests(os.path.join(repo_dir, args.staging_dir),
                                   'staging')
        for entry in manifests:
            pass
        print('(done)')

    if not (production_up_to_date and staging_up_to_date):
        log('checking out target branch')
        hard_sync(args.target_branch)
        log('generating manifests')

        # initial house keeping
        local_prod_dir = os.path.join(args.target_dir, 'production')
        local_stage_dir = os.path.join(args.target_dir, 'staging')

        clear_git_dir(local_prod_dir)
        clear_git_dir(local_stage_dir)

        # main production section
        for entry in entries_map['production'].values():
            if entry.ignored:
                continue

            filename = '.'.join((
                            '-'.join((entry.api_group,
                                      entry.api_version,
                                      entry.kind,
                                      entry.namespace,
                                      entry.name)),
                            'yaml'))

            local_filepath = os.path.join(local_prod_dir, filename)
            filepath = os.path.join(repo_dir, local_filepath)
            local_display_path = os.path.relpath(local_filepath,
                                                 args.target_dir)
            print(f'  + {local_display_path}')
            with open(filepath, 'w') as f:
                f.write('---\n')
                yaml.dump(entry.obj, f)
            gitC('add', local_filepath)

        log('committing production update')
        commit_msg = f'update from production: {current_production_hash}'
        gitC('commit', '--allow-empty', '-m', commit_msg)

        # staging section
        for entry in special_map['staging'].values():
            if entry.ignored:
                continue

            tokens = entry.obj['data']['apiVersion'].split('/')
            api_group, api_version = (
                    ('builtin', tokens[0]) if len(tokens) == 1 else tokens)
            kind = entry.obj['data']['kind']
            name = entry.obj['data']['name']
            namespace = entry.namespace

            ref = entries_map['staging'][
                    (api_group, api_version, kind, namespace, name)]
            patch = process_patch(yaml.full_load(entry.obj['data']['patch']),
                                  env='staging')

            new_obj = copy.deepcopy(ref.obj)
            apply_patch(new_obj, patch)
            new_obj = ParsedEntry(new_obj, 'staging', update=False)

            if new_obj.ignored:
                continue

            filename = '.'.join((
                            '-'.join((new_obj.api_group,
                                      new_obj.api_version,
                                      new_obj.kind,
                                      new_obj.namespace,
                                      new_obj.name)),
                            'yaml'))

            local_filepath = os.path.join(local_stage_dir, filename)
            filepath = os.path.join(repo_dir, local_filepath)
            local_display_path = os.path.relpath(local_filepath,
                                                 args.target_dir)
            print(f'  + {local_display_path}')
            with open(filepath, 'w') as f:
                f.write('---\n')
                yaml.dump(new_obj.obj, f)

            gitC('add', local_filepath)

        log('committing staging update')
        commit_msg = f'update from staging: {current_staging_hash}'
        gitC('commit', '--allow-empty', '-m', commit_msg)

        log('pushing updates')
        update_spec = ':'.join((args.target_branch, args.target_branch))
        gitC('push', 'origin', update_spec)

        current_target_hash = rev_list(args.target_branch)
        update_staging = True
        update_production = True
        update_target = True

    if update_staging:
        write_scalar_to_path(last_staging_file, current_staging_hash)
        last_staging_hash = current_staging_hash

    if update_production:
        write_scalar_to_path(last_production_file, current_production_hash)
        last_production_hash = current_production_hash

    if update_target:
        write_scalar_to_path(last_target_file, current_target_hash)
        last_target_hash = current_target_hash

    elapsed_time = time.time() - start_time
    sleep_time = args.interval - elapsed_time
    if sleep_time > 0:
        time.sleep(sleep_time)
