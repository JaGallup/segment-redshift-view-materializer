from sqlalchemy import Table
from sqlalchemy.sql import select, func, literal, over, union_all, Select, or_, and_, join
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import MetaData


class CreateTableAs(Select):
    """Create a CREATE TABLE ...  AS SELECT ... statement."""

    def __init__(self, columns, new_table_name, dist_key, sort_key, is_temporary=False,
                 on_commit_delete_rows=False, on_commit_drop=False, *arg, **kwargs):
        super(CreateTableAs, self).__init__(columns, *arg, **kwargs)
        self.new_table_name = new_table_name
        self.dist_key = dist_key
        self.sort_key = sort_key
        self.is_temporary = is_temporary
        self.on_commit_delete_rows = on_commit_delete_rows
        self.on_commit_drop = on_commit_drop


@compiles(CreateTableAs)
def s_create_table_as(element, compiler, **kwargs):
    """Compile the statement."""
    text = compiler.visit_select(element)
    spec = ['CREATE', 'TABLE', element.new_table_name, 'DISTKEY', element.dist_key, 'SORTKEY', element.sort_key,
            'AS SELECT']

    if element.is_temporary:
        spec.insert(1, 'TEMPORARY')

    on_commit = None

    if element.on_commit_delete_rows:
        on_commit = 'ON COMMIT DELETE ROWS'
    elif element.on_commit_drop:
        on_commit = 'ON COMMIT DROP'

    if on_commit:
        spec.insert(len(spec) - 1, on_commit)

    text = text.replace('SELECT', ' '.join(spec), 1)
    return text


def materialize(connection, engine, schema_name, schema_users, schema_properties):
    trans = connection.begin()
    try:
        create_event_union(connection, engine, schema_name, schema_users, schema_properties)
        create_sessions(connection, engine, schema_name, schema_users, schema_properties)
        create_event_facts(connection, engine, schema_name, schema_users, schema_properties)
        trans.commit()
    except:
        trans.rollback()
        raise
    finally:
        trans.close()


def create_event_union(connection, engine, schema_name, schema_users, schema_properties):
    meta = MetaData(engine)
    pages = Table('pages', meta, autoload=True,
                  autoload_with=connection,
                  schema=schema_name)
    tracks = Table('tracks', meta, autoload=True,
                   autoload_with=connection,
                   schema=schema_name)
    s1_columns = [
        pages.c.id,
        pages.c.anonymous_id,
        pages.c.received_at,
        pages.c.path.label('event'),
        literal('pages').label('event_source')
    ]
    s2_columns = [
        tracks.c.id,
        tracks.c.anonymous_id,
        tracks.c.received_at,
        tracks.c.event.label('event'),
        literal('tracks').label('event_source')
    ]
    for c in pages.c:
        for prop in schema_properties:
            if prop in c.name or 'context_' in c.name:
                s1_columns.insert(len(s1_columns) - 1, c)
    for c in tracks.c:
        for prop in schema_properties:
            if prop in c.name or 'context_' in c.name:
                s2_columns.insert(len(s2_columns) - 1, c)
    s1 = select(s1_columns)
    s2 = select(s2_columns)
    union = union_all(s1, s2)
    events = [
        union,
        func.datediff('seconds',
                      over(func.lag(union.c.received_at),
                           partition_by=union.c.anonymous_id,
                           order_by=union.c.received_at),
                      union.c.received_at).label('idle_time_seconds'),
        func.datediff('minutes',
                      over(func.lag(union.c.received_at),
                           partition_by=union.c.anonymous_id,
                           order_by=union.c.received_at),
                      union.c.received_at).label('idle_time_minutes')
    ]
    trans = connection.begin()
    try:
        Table('event_union', meta, schema=schema_name).drop(engine, checkfirst=True)
        create_table = CreateTableAs(events, '{0}.event_union'.format(schema_name), '(1)', '(3)')
        connection.execute(create_table)
        grant_select = 'GRANT SELECT ON ' + schema_name + '.event_union TO ' + schema_users + ';'
        connection.execute(grant_select)
        trans.commit()
    except:
        trans.rollback()
        raise


def create_sessions(connection, engine, schema_name, schema_users, schema_properties):
    meta = MetaData(engine)
    event_union = Table('event_union', meta, autoload=True,
                        autoload_with=connection,
                        schema=schema_name)
    columns = [
        (
            func.row_number().over(
                partition_by=event_union.c.anonymous_id,
                order_by=event_union.c.received_at
            ) + ' - ' + event_union.c.anonymous_id
        ).label('session_id'),
        event_union.c.anonymous_id,
        event_union.c.received_at.label('session_start_at'),
        func.row_number().over(
            partition_by=event_union.c.anonymous_id,
            order_by=event_union.c.received_at
        ).label('session_sequence_number'),
        func.lead(event_union.c.received_at).over(
            partition_by=event_union.c.anonymous_id,
            order_by=event_union.c.received_at
        ).label('next_session_start_at'),
    ]
    for c in event_union.c:
        for prop in schema_properties:
            if prop in c.name or 'context_' in c.name:
                columns.insert(len(columns), c)
    trans = connection.begin()
    try:
        Table('sessions', meta, schema=schema_name).drop(engine, checkfirst=True)
        create_table = CreateTableAs(columns, '{0}.sessions'.format(schema_name), '(1)', '(3)').where(
            or_(
                event_union.c.idle_time_minutes > 30,
                event_union.c.idle_time_minutes == None
            )
        )
        connection.execute(create_table)
        grant_select = 'GRANT SELECT ON ' + schema_name + '.sessions TO ' + schema_users + ';'
        connection.execute(grant_select)
        trans.commit()
    except:
        trans.rollback()
        raise


def create_event_facts(connection, engine, schema_name, schema_users, schema_properties):
    meta = MetaData(engine)
    event_union = Table('event_union', meta, autoload=True,
                        autoload_with=connection,
                        schema=schema_name)
    sessions = Table('sessions', meta, autoload=True,
                     autoload_with=connection,
                     schema=schema_name)
    columns = [
        event_union.c.id,
        event_union.c.anonymous_id,
        event_union.c.received_at,
        sessions.c.session_id,
        sessions.c.session_sequence_number,
        sessions.c.session_start_at,
        event_union.c.event,
        event_union.c.event_source,
        sessions.c.context_page_referrer.label('session_referrer'),
        func.row_number().over(
            partition_by=sessions.c.session_id,
            order_by=event_union.c.received_at
        ).label('track_sequence_number'),
        func.row_number().over(
            partition_by=[
                sessions.c.session_id,
                event_union.c.event_source
            ],
            order_by=event_union.c.received_at
        ).label('source_sequence_number'),
        event_union.c.idle_time_seconds
    ]
    for c in event_union.c:
        for prop in schema_properties:
            if prop in c.name or ('context_' in c.name and 'campaign' not in c.name):
                columns.insert(len(columns), c)
    for c in sessions.c:
        if 'context_campaign' in c.name:
            columns.insert(len(columns), c)
    j = join(event_union, sessions,
             and_(
                 event_union.c.anonymous_id == sessions.c.anonymous_id,
                 event_union.c.received_at >= sessions.c.session_start_at,
                 or_(
                     event_union.c.received_at < sessions.c.next_session_start_at,
                     sessions.c.next_session_start_at == None
                 )
             )
             )
    trans = connection.begin()
    try:
        Table('event_facts', meta, schema=schema_name).drop(engine, checkfirst=True)
        create_table = CreateTableAs(columns, '{0}.event_facts'.format(schema_name), '(1)', '(3)').select_from(j)
        connection.execute(create_table)
        grant_select = 'GRANT SELECT ON ' + schema_name + '.event_facts TO ' + schema_users + ';'
        connection.execute(grant_select)
        trans.commit()
    except:
        trans.rollback()
        raise
