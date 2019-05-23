import argparse
import os

from dbt.config import RuntimeConfig, PROFILES_DIR
from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger, initialize_logger
from dbt.clients import system


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--profile',
        default=None,
        help='The profile override, if set.'
    )
    parser.add_argument(
        '--target',
        default=None,
        help='The target to use within the given profile'
    )
    parser.add_argument(
        '--profiles-dir',
        default=PROFILES_DIR,
        help='The directory to look for profiles.yml in'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='debug logging'
    )
    parser.add_argument(
        '--no-files',
        action='store_false',
        dest='write_files',
        help='If set, do not write .sql files to disk'
    )
    parser.add_argument(
        '--no-database-operations',
        action='store_false',
        dest='migrate_database',
        help='If set, do not migrate the database'
    )
    return parser.parse_args()


RENAME_COLUMNS = (
    ('valid_from', 'dbt_valid_from'),
    ('valid_to', 'dbt_valid_to'),
    ('scd_id', 'dbt_scd_id'),
)


def backup_relation(adapter, archive_target):
    new_name = archive_target.identifier + '_dbt_archive_migration_backup'
    backup_relation = archive_target.incorporate(
        path={'identifier': new_name}, table_name=new_name
    )
    # if your backup exists, throw an error!
    adapter.execute(
        'create table {} as (select * from {})'
        .format(backup_relation, archive_target)
    )
    logger.info('backed up {} to {}'.format(archive_target, backup_relation))
    return backup_relation


def migrate_archive_alter_table(adapter, archive_target):
    """Migrate the archive using "alter table" commands to rename columns.
    """
    renames = RENAME_COLUMNS
    if adapter.type() == 'snowflake':
        renames += (('dbt_updated_at', 'dbt_updated_at'),)
    for old, new in renames:
        old = adapter.quote(old)
        sql = 'alter table {!s} rename column {} to {}'.format(
            # should this be new -> adapter.quote_as_configured(new)?
            archive_target, old, new
        )
        adapter.execute(sql)
        logger.info('renamed {} -> {}'.format(old, new))


def migrate_archive_bigquery(adapter, archive_target):
    """Migrate the archive by select * EXCEPT(...) into itself
    """
    except_str = ', '.join(adapter.quote(o) for o, _ in RENAME_COLUMNS)
    rename_str = ', '.join(
        '{} as {}'.format(adapter.quote(o), n)
        for o, n in RENAME_COLUMNS
    )
    sql = 'select * EXCEPT({}), {} from {!s}'.format(
        except_str, rename_str, archive_target
    )
    adapter.connections.create_table(
        database=archive_target.database,
        schema=archive_target.schema,
        table_name=archive_target.identifier,
        sql=sql
    )


def migrate_archive_table(adapter, archive_target):
    """Migrate the given archive."""
    print('starting migration of {}'.format(archive_target))
    if adapter.type() == 'bigquery':
        result = migrate_archive_bigquery(adapter, archive_target)
    else:
        result = migrate_archive_alter_table(adapter, archive_target)
    print('finished migration of {}'.format(archive_target))
    return result


def get_archive_configs(cfg):
    default_database = cfg.credentials.database
    for archive in cfg.archive:
        target_database = archive.get('target_database', default_database)
        target_schema = archive['target_schema']
        source_database = archive.get('source_database', default_database)
        source_schema = archive['source_schema']
        for table in archive['tables']:
            table_copy = table.copy()
            table_copy['target_database'] = target_database
            table_copy['target_schema'] = target_schema
            table_copy['source_database'] = source_database
            table_copy['source_schema'] = source_schema

            yield table_copy


template = '''
{{% archive {name} %}}
    {{{{
        config({kwargs})
    }}}}
    select * from {source_relation}
{{% endarchive %}}
'''


def build_archive_data(adapter, archive):
    source_relation = [
        adapter.quote_as_configured(archive['source_database'], 'database'),
        adapter.quote_as_configured(archive['source_schema'], 'schema'),
        adapter.quote_as_configured(archive['source_table'], 'identifier'),
    ]
    kwargs = {
        'target_database': archive['target_database'],
        'target_schema': archive['target_schema'],
        'updated_at': archive['updated_at'],
        'strategy': 'timestamp',
        'unique_key': archive['unique_key'],
    }

    return template.format(
        source_relation='.'.join(source_relation),
        kwargs=repr(kwargs),
        name=archive['target_table']
    )


def collect_migration_targets(adapter, cfg):
    for archive in get_archive_configs(cfg):
        relation = adapter.Relation.create(
            database=archive['target_database'],
            schema=archive['target_schema'],
            identifier=archive['target_table'],
            quote_policy=cfg.quoting
        )
        file_contents = build_archive_data(adapter, archive)
        yield relation, file_contents


def write_migration_file(target, archive_path, contents):
    path = os.path.join(archive_path, target.identifier + '.sql')
    wrote = system.make_file(path=path, contents=contents)
    if wrote:
        msg = 'Migrated archive {!s} -> file at {}'
    else:
        msg = 'Could not write archive {!s} to file {}'
    logger.info(msg.format(target, path))
    return target


def perform_table_migration(adapter, target):
    if adapter.type() != 'bigquery':
        adapter.connections.add_begin_query()
    backup = backup_relation(adapter, target)
    migrate_archive_table(adapter, target)
    if adapter.type() != 'bigquery':
        adapter.connections.add_commit_query()
    return backup


def main():
    parsed = parse_args()
    cfg = RuntimeConfig.from_args(parsed)
    log_path = getattr(cfg, 'log_path', None)
    initialize_logger(parsed.debug, log_path)

    adapter = get_adapter(cfg)
    archive_path = os.path.normpath(cfg.archive_paths[0])
    system.make_directory(archive_path)

    backups_made = []
    archives_written = []

    with adapter.connection_named('migration'):
        for target, contents in collect_migration_targets(adapter, cfg):
            if parsed.migrate_database:
                logger.info('migrating database for {}'.format(target))
                backups_made.append(perform_table_migration(adapter, target))
            else:
                logger.info('not migrating database for {}'.format(target))

            if parsed.write_files:
                logger.info('writing new sql file for {}'.format(target))
                logger.info('Do not forget to remove or comment out the '
                            'existing entry!')
                path = write_migration_file(target, archive_path, contents)
                archives_written.append(path)
            else:
                logger.info('not writing new sql file for {}'.format(target))

    logger.info('Migration complete!')
    if backups_made:
        msg = (
            'Backup archives you may wish to drop:\n\t{}'
            .format('\n\t'.join(map(str, backups_made)))
        )
        logger.info(msg)

    if archives_written:
        msg = (
            'Archive files written for the following tables, you should '
            'remove them from dbt_project.yml:\n\t{}'
            .format('\n\t'.join(map(str, archives_written)))
        )
        logger.info(msg)


if __name__ == '__main__':
    main()
