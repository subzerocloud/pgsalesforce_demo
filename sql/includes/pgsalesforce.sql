-- Create a server for Salesforce using the settings from the GUCs
create or replace function pgsalesforce.create_server(
    server_name text,
    fdw_name text
)
returns void as $$
declare
    instance text := current_setting('pgsalesforce.instance');
    client_id text := current_setting('pgsalesforce.client_id');
    client_secret text := current_setting('pgsalesforce.client_secret');
    api_version text := current_setting('pgsalesforce.api_version');
begin
    -- check gucs are set
    if instance is null then
        raise exception 'pg_salesforce.instance is not set';
    end if;
    if client_id is null then
        raise exception 'pg_salesforce.client_id is not set';
    end if;
    if client_secret is null then
        raise exception 'pg_salesforce.client_secret is not set';
    end if;
    if api_version is null then
        raise exception 'pg_salesforce.api_version is not set';
    end if;
    
    -- create a foreign server for Salesforce
    execute format(
        '
        create server %I
        foreign data wrapper %I
        options (
            sf_instance %L,
            sf_client_id %L,
            sf_client_secret %L,
            sf_api_version %L
        )
        ',
        server_name,
        fdw_name,
        instance,
        client_id,
        client_secret,
        api_version
    );
end;
$$ language plpgsql;

-- Create a local and foreign table for Salesforce sobject
create or replace function pgsalesforce.create_table_pairs(
    server_name text,
    target_schema_name text,
    sobjects text[]
)
returns void as $$
declare
    sobject_name text;
    sobject_schema jsonb;
    ddl text;
    foreign_keys_ddls text[];
    r record;
begin
    execute format('create schema if not exists %I', target_schema_name);

    foreach sobject_name in array sobjects loop
        raise notice '%: fetching schema', sobject_name;
        sobject_schema := pgsalesforce.describe_sobject(sobject_name);
        
        raise notice '%: creatig foreign table', sobject_name;
        ddl := pgsalesforce.get_create_foreign_table_ddl(target_schema_name, sobject_name || '_fdw', server_name, sobject_schema, null);
        execute ddl;
        
        raise notice '%: creating local table', sobject_name;
        ddl := pgsalesforce.get_create_local_table_ddl(target_schema_name, sobject_name, sobject_schema, null);
        execute ddl;

        -- collect foreign key ddls in an array
        foreign_keys_ddls := array_cat(
            foreign_keys_ddls,
            pgsalesforce.get_create_foreign_key_ddl(target_schema_name, sobject_name, sobject_schema, sobjects, null)
        );
    end loop;

    -- raise notice 'Creating foreign keys';
    foreach ddl in array foreign_keys_ddls loop
        -- raise notice 'Executing: %', ddl;
        execute ddl;
    end loop;

    -- disable all fk triggers
    -- we want to maintain the foreign key definitions but disable the triggers
    -- that enforce them so that we can sync data from Salesforce
    for r in
        select
            tg.tgname as trigger_name,
            tbl.relname as table_name,
            nsp.nspname as schema_name,
            con.conname as constraint_name,
            con.contype as constraint_type
        from
            pg_trigger tg
        join
            pg_constraint con on tg.tgconstraint = con.oid
        join
            pg_class tbl on tg.tgrelid = tbl.oid
        join
            pg_namespace nsp on tbl.relnamespace = nsp.oid
        where
            con.contype = 'f' -- foreign key constraint
            and nsp.nspname = target_schema_name -- filter by schema name
            and tbl.relname = any(sobjects) -- filter by table list
    loop
        -- raise notice 'disabling trigger %', r.trigger_name;
        execute format('alter table %I.%I disable trigger %I', r.schema_name, r.table_name, r.trigger_name);
    end loop;
    -- raise notice 'Done';
end;
$$ language plpgsql;


-- This function runs an upsert query to sync data from a Salesforce to a PostgreSQL
-- It assumes scehma_name.sobject and schema_name.sobject_fdw tables exist and have the same columns
create or replace function pgsalesforce.salesforce_to_pg_sync(schema_name text, sobject text, conditions text[] default null)
returns void AS $$
declare
    source_table text;
    target_table text;
    column_names text;
    id_column text := 'Id';  -- Assuming 'Id' is the primary key in the target table
    update_clause text;
    query text;
    where_clause text;
    triggers_to_disable text[];
    trigger_name text;
    affected_rows_count integer;
begin
    -- Define source and target tables
    source_table = format('%I.%I', schema_name, sobject || '_fdw');
    target_table = format('%I.%I', schema_name, sobject);

    -- Generate the list of columns for the target table
    select string_agg(format('%I', column_name), ', ')
    into column_names
    from information_schema.columns
    where table_schema = schema_name and table_name = sobject;

    -- Generate the update clause (column = excluded.column)
    select string_agg(format('%I = excluded.%I', column_name, column_name), ', ')
    into update_clause
    from information_schema.columns
    where table_schema = schema_name and table_name = sobject;

    -- set where clause
    if conditions is not null then
        where_clause := ' where ' || array_to_string(conditions, ' and ');
    else
        where_clause := '';
    end if;

    -- Construct the upsert query dynamically
    query = format('
        with upsert as (
            insert into %s (%s)
            select %s from %s %s
            on conflict (%I)
            do update set %s
            returning 1
        )
        select count(*) from upsert
    ', target_table, column_names, column_names, source_table, where_clause, id_column, update_clause);

    -- disable the triggers that push data to Salesforce
    triggers_to_disable := array[
        format('%s_before_update_trigger', sobject),
        format('%s_before_insert_trigger', sobject),
        format('%s_before_delete_trigger', sobject)
    ];
    foreach trigger_name in array triggers_to_disable
    loop
        execute format('alter table %I.%I disable trigger %I', schema_name, sobject, trigger_name);
    end loop;

    -- Execute the upsert query
    execute query into affected_rows_count;

    -- enable back the triggers that push data to Salesforce
    foreach trigger_name in array triggers_to_disable
    loop
        execute format('alter table %I.%I enable trigger %I', schema_name, sobject, trigger_name);
    end loop;

    raise notice 'Synced %.% with % rows', schema_name, sobject, affected_rows_count;
end $$ language plpgsql;


-- Get the column names of a table by introspecting the information_schema.columns view
-- This function is used by trigger functions that sync data between a local and a foreign table
create or replace function pgsalesforce.get_table_column_names(s text, t text)
returns text[] AS $$
declare
    column_names text[];
begin
    select array_agg(c.column_name)
    into column_names
    from information_schema.columns c
    where c.table_schema = s
    and c.table_name = t;

    return column_names;
end;
$$ language plpgsql STABLE;

-- Trigger functions to execute then insert on the foreign table when a row is inserted in the local table
create or replace function pgsalesforce.before_insert_sync_to_fdw()
returns trigger as $$
declare
    all_column_names text[];
    column_names text[];
    placeholders text[];
    values_array text[];
    col text;
    col_val text;
    i int := 1;
    local_id text;
    remote_id text;
    r record;
begin
    -- Get the column names
    all_column_names := pgsalesforce.get_table_column_names(TG_TABLE_SCHEMA, TG_TABLE_NAME);

    -- Iterate through each column in the NEW record
    foreach col in array all_column_names
    loop
        -- get the column value from the NEW record
        execute format('select ($1).%I::text', col) into col_val using NEW;
        
        -- Only process columns that are not null
        if col_val is not null then
            -- Append the column name to the list
            column_names := column_names || quote_ident(col);

            -- Append each column's value to the array
            values_array := values_array || col_val;

            -- Append a placeholder for the prepared statement
            placeholders := placeholders || format('$1[%s]', i);
            i := i + 1;
        end if;
    end loop;

    -- Construct and execute the prepared statement to insert into the FDW table
    execute format(
        'insert into %I.%I (%s) values (%s)', 
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME || '_fdw', 
        array_to_string(column_names, ', '),
        array_to_string(placeholders, ', ')
    )
    USING values_array;

    -- underlying fdw lib dependency does not support returning clause
    -- so we need to fetch the remote id after insert using the a custom function
    -- provided by pgsalesforce extension
    -- get inserted ids from get_inserted_row_ids, loop and find the row
    for r in select * from pgsalesforce.get_inserted_row_ids()
    loop
        if NEW."Id" = r.local_id then
            -- Change the local Id to the remote Id
            NEW."Id" := r.remote_id;
            exit;
        end if;
    end loop;
    -- Continue with the insert into the local table
    return NEW;
end;
$$ language plpgsql;

-- Trigger functions to execute then update on the foreign table when a row is updated in the local table
create or replace function pgsalesforce.before_update_sync_to_fdw()
returns trigger AS $$
declare
    all_column_names text[];
    set_columns text[];
    values_array text[];
    col text;
    col_val text;
    col_val_old text;
    i int := 1;
begin
    -- Get the column names
    all_column_names := pgsalesforce.get_table_column_names(TG_TABLE_SCHEMA, TG_TABLE_NAME);

    foreach col in array all_column_names
    loop
        execute format('select ($1).%I::text', col) into col_val using NEW;
        execute format('select ($1).%I::text', col) into col_val_old using OLD;
        -- Only process columns that changed
        if col_val <> col_val_old then

            -- Append the column name to the list
            set_columns := set_columns || format('%I = $2[%s]', col, i);

            -- Append each column's value to the array
            values_array := values_array || col_val;

            i := i + 1;
        end if;
    end loop;

    -- Construct and execute the prepared statement to update the FDW table
    execute format(
        'update %I.%I set %s where "Id" = $1', 
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME || '_fdw', 
        array_to_string(set_columns, ', ')
    )
    using NEW."Id", values_array;

    return NEW;
end;
$$ language plpgsql;

-- Trigger functions to execute then delete on the foreign table when a row is deleted in the local table
create or replace function pgsalesforce.before_delete_sync_to_fdw()
returns trigger AS $$
begin
    -- Delete the corresponding record from the FDW table
    execute format(
        'delete from %I.%I where "Id" = $1', 
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME || '_fdw'
    )
    using OLD."Id";
    return OLD;
end;
$$ language plpgsql;

-- Attach triggers to sync data from a PostgreSQL table to a Salesforce object
create or replace function pgsalesforce.attach_pg_to_sf_sync_triggers(
    target_schema_name text,
    sobject text
)
returns void as $$
begin
    execute
        format('
            create trigger %I before insert on %I.%I
            for each row execute function pgsalesforce.before_insert_sync_to_fdw()',
            sobject || '_before_insert_trigger',
            target_schema_name,
            sobject
        );

    execute
        format('
            create trigger %I before update on %I.%I
            for each row execute function pgsalesforce.before_update_sync_to_fdw()',
            sobject || '_before_update_trigger',
            target_schema_name,
            sobject
        );

    execute
        format('
            create trigger %I before delete on %I.%I
            for each row execute function pgsalesforce.before_delete_sync_to_fdw()',
            sobject || '_before_delete_trigger',
            target_schema_name,
            sobject
        );
end;
$$ language plpgsql;


-- create a table to store the last sync time for each sobject
create table if not exists pgsalesforce.sync_times (
    sobject_name text primary key,
    last_sync_time timestamp
);

-- split a string into an array and trim the elements
create or replace function pgsalesforce.split_string_to_array(
    p_string text,
    p_delimiter text
)
returns text[] as $$
begin
    return array(
        select trim(both p_delimiter from elem)
        from unnest(string_to_array(p_string, p_delimiter)) as elem
    );
end;
$$ language plpgsql;

create or replace procedure pgsalesforce.init_tables(server_name text, target_schema_name text, s_objects text[])
as $$
declare
    available_sobjects text[];
    unavailable_sobjects text[];
    o text;
    now_time timestamp;
begin
    select array_agg(sobject->>'name') as sobjects
    into available_sobjects
    from (
        select jsonb_array_elements((pgsalesforce.get_available_sobjects())->'sobjects') as sobject
    ) as sobject_names;

    select array(select unnest(s_objects) except select unnest(available_sobjects))
    into unavailable_sobjects;

    -- Check all s_objects are contained in the available s_objects
    if unavailable_sobjects is not null and array_length(unavailable_sobjects, 1) > 0
    then
        raise exception 'These salesforce sobjects are not available: %', unavailable_sobjects;
    end if;

    -- Create local and foreign tables for each s_object
    -- for each s_object "object" (local) and "object_fdw" (foreign) tables are created
    -- the foreign key constraints are also created but their triggers are disabled (to make sync easier)
    perform pgsalesforce.create_table_pairs(
        server_name, -- server_name
        target_schema_name, -- target_schema_name (scheam were to create the local and foreign tables)
        s_objects -- array of s_objects
    );

    -- Add the triggers that sync local changes to Salesforce
    foreach o in array s_objects loop
        perform pgsalesforce.attach_pg_to_sf_sync_triggers(target_schema_name, o);
    end loop;

    -- Sync the data from Salesforce to PostgreSQL
    -- This is mostly a dinamically created UPSERT query
    -- that updates the local table with the data from the foreign table
    foreach o in array s_objects loop
        -- save the current time
        now_time := now();
        perform pgsalesforce.salesforce_to_pg_sync(target_schema_name,o);
        -- save the sync time in the table
        execute '
            insert into pgsalesforce.sync_times(sobject_name, last_sync_time)
            values ($1, $2)
            on conflict (sobject_name)
            do update set last_sync_time = excluded.last_sync_time'
        using o, now_time;
    end loop;
end $$
language plpgsql;

-- a procedure to sync data from Salesforce to PostgreSQL that can be called by a cron job
create or replace procedure sync_data(target_schema_name text, objects_to_sync text[])
as $$
declare
    o text;
    now_time timestamp;
    last_sync_time timestamp;
begin
    raise notice 'Syncing data...';

    foreach o in array objects_to_sync loop
        -- save the current time
        now_time := now();
        
        -- get the last sync time
        select st.last_sync_time
        into last_sync_time
        from pgsalesforce.sync_times as st
        where sobject_name = o;

        perform pgsalesforce.salesforce_to_pg_sync(
            target_schema_name, -- target_schema_name
            o, -- sObject
            array[
                format('"%s" > ''%s''', 'LastModifiedDate', last_sync_time)
            ] -- optional WHERE clause
        );
        -- save the sync time in the table
        execute '
            insert into pgsalesforce.sync_times(sobject_name, last_sync_time)
            values ($1, $2)
            on conflict (sobject_name)
            do update set last_sync_time = excluded.last_sync_time'
        using o, now_time;

    end loop;
    raise notice 'Data synced';
end $$
language plpgsql;
