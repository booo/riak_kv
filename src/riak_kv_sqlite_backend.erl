-module(riak_kv_sqlite_backend).
-behavior(riak_kv_backend).

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
% -define(CAPABILITIES, [async_fold, indexes]).
-define(CAPABILITIES, [indexes]).

-record(state, {ref :: reference()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the sqlite backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %fprof:trace(start),
    DataDir = filename:join(app_helper:get_prop_or_env(data_root, Config, sqlite3), integer_to_list(Partition)),
    filelib:ensure_dir(DataDir),
    %io:fwrite(DataDir),
    %io:fwrite("\n"),
    %{ok, Pid} = sqlite3:open(file, [{file, DataDir}]),
    %io:fwrite("Trying to open sqlite3 database...\n"),
    Db = list_to_atom(integer_to_list(Partition)),
    %TODO maybe improve this
    % seems like we "restart" database while/after handoff
    % maybe this was caused by not implementing the stop function?
    Pid = case sqlite3:open(Db, [{file, DataDir}]) of
        {error,{already_started,Pid2}} ->
            Pid2;
        {ok, Pid2} ->
            Pid2
    end,
    %io:fwrite("database opened.\n"),
    ok = sqlite3:sql_exec(Db,
        "CREATE TABLE IF NOT EXISTS store (
            bucket BLOB,
            key BLOB,
            value BLOB,
            CONSTRAINT store_primary_key PRIMARY KEY (bucket, key)
        );"
    ),
    % optimization? optimization!!!
    ok = sqlite3:sql_exec(Db, "PRAGMA synchronous=OFF;"),
    ok = sqlite3:sql_exec(Db, "PRAGMA count_changes=FALSE;"),
    ok = sqlite3:sql_exec(Db, "PRAGMA auto_vacuum=FULL;"),
    [{columns, _},{rows,[{null}]}] =
        sqlite3:sql_exec(Db, "SELECT load_extension('libspatialite.so.3')"),
    % this pattern matching is useless
    [{columns, _},{rows,[{_Value}]}] =
        sqlite3:sql_exec(Db, "SELECT InitSpatialMetaData()"),
    [{columns,["locking_mode"]},{rows,[{<<"exclusive">>}]}] = sqlite3:sql_exec(Db, "PRAGMA locking_mode=EXCLUSIVE;"),
    %% TODO prepare some statements
    %sqlite3:create_table(Db, store, [{bucket, blob}, {key, blob}, {value, blob}]),
    {ok, #state{ref=Pid}}.
    %% io:write(Partition),
    %% io:fwrite("\n"),
    %% io:write(Config),
    %% io:fwrite("\n"),
    %% DataDir = filename:join(app_helper:get_prop_or_env(data_root, Config, sqlite3), integer_to_list(Partition)),
    %% io:fwrite(DataDir),
    %% io:fwrite("\n"),
    %{ok, Pid} = sqlite3:open(DataDir, {file, DataDir}),
    %{ok, #state{ref=Pid}},
    % create table if not exists

%% @doc Stop the sqlite backend
stop(#state{ref=Ref}) ->
    %fprof:trace(stop),
    %TODO maybe do some error handling?
    ok = sqlite3:close(Ref).

%%%% @doc Retrieve an object from the sqlite backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{ref=Ref}=State) ->
    case sqlite3:sql_exec(Ref, "SELECT value FROM store WHERE bucket = ? AND key = ?",
        [{1, Bucket}, {2, Key}]) of
        [{columns,_},{rows,[{Value}]}] ->
            {ok, Value, State};
        _Result ->
            %io:fwrite("Result: ~80p~n", [Result]),
            {error, not_found, State}
    end.

get_field_type(Field) ->
    case lists:suffix("_int", Field) of
        true -> "INTEGER";
        false -> "BLOB"
    end.

%TODO add type definition
add_index(Ref, Bucket, Key, Field, FieldValue) ->
    FieldString = binary:bin_to_list(Field),
    Query = case lists:suffix("_spatial", FieldString) of
        true -> "UPDATE store SET " ++ FieldString ++ " = ST_GeomFromText(?, 4326)
                    WHERE bucket = ? AND key = ?;";
        false -> "UPDATE store SET " ++ FieldString ++ " = ?
                    WHERE bucket = ? AND key = ?;"
        end,
    case sqlite3:sql_exec(Ref, Query,
        [{1,mochiweb_util:unquote(FieldValue)}, {2, Bucket}, {3, Key}]) of
        ok -> ok;
        % if column does not exist
        % create one and an index
        % TODO maybe use a transaction?
        {error, _ErrorCode, _ErrorMsg} ->
            Condition = {lists:suffix("_int", FieldString),
                lists:suffix("_bin", FieldString),
                lists:suffix("_spatial", FieldString)},

            % TODO I'm so ugly please beautify me
            case Condition of
                {true, false, false} ->
                    ok = sqlite3:sql_exec(Ref,
                        "ALTER TABLE store ADD COLUMN " ++ FieldString
                            ++ " " ++ get_field_type(FieldString) ++ ";"),
                    ok = sqlite3:sql_exec(Ref,
                        "CREATE INDEX store_secondary_index_" ++ FieldString ++
                        " ON store (" ++ FieldString ++ ");" ),
                    sqlite3:sql_exec(Ref,
                        "UPDATE store SET " ++ FieldString ++ " = ?
                            WHERE bucket = ? AND key = ?;",
                        [{1, FieldValue}, {2, Bucket}, {3, Key}]);
                %% TODO this is somehow duplicated
                {false, true, false} ->
                    ok = sqlite3:sql_exec(Ref,
                        "ALTER TABLE store ADD COLUMN " ++ FieldString
                            ++ " " ++ get_field_type(FieldString) ++ ";"),
                    ok = sqlite3:sql_exec(Ref,
                        "CREATE INDEX store_secondary_index_" ++ FieldString ++
                        " ON store (" ++ FieldString ++ ");" ),
                    sqlite3:sql_exec(Ref,
                        "UPDATE store SET " ++ FieldString ++ " = ?
                            WHERE bucket = ? AND key = ?;",
                        [{1, FieldValue}, {2, Bucket}, {3, Key}]);
                {false, false, true} ->
                    [{columns,_},{rows,[{1}]}] = sqlite3:sql_exec(Ref,
                        "SELECT AddGeometryColumn('store', ?, 4326, 'GEOMETRY', 'XY');",
                        [{1, Field}]
                    ),
                    [{columns,_},{rows,[{1}]}] = sqlite3:sql_exec(Ref,
                        "SELECT CreateSpatialIndex('store' , ?);",
                        [{1, Field}]
                    ),
                    sqlite3:sql_exec(Ref,
                        "UPDATE store SET " ++ FieldString ++ " = ST_GeomFromText(?, 4326)
                            WHERE bucket = ? AND key = ?;",
                        [{1, mochiweb_util:unquote(FieldValue)}, {2, Bucket}, {3, Key}])
            end
    end.

remove_index(Ref, Bucket, Key, Field) ->
    sqlite3:sql_exec(Ref,
        %TODO maybe do some error handling?
        "UPDATE store SET " ++ binary:bin_to_list(Field) ++ " = NULL
            WHERE bucket = ? AND key = ?;",
        [{1, Bucket}, {2, Key}]).


%% @doc Insert an object into the sqlite backend.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, Key, IndexSpecs, Val, #state{ref=Ref}=State) ->
    %io:fwrite("Bucket: ~80p~n", [Bucket]),
    %io:fwrite("Key: ~80p~n", [Key]),
    %io:fwrite("IndexSpecs: ~80p~n", [IndexSpecs]),
    % IndexSpecs: [{add,<<"field1_bin">>,<<"val1">>},{add,<<"field2_int">>,1001}]
    %io:fwrite("Val: ~80p~n", [Val]),
    %io:fwrite("Ref: ~80p~n", [Ref]),
    SpecsHandler = fun(Spec) ->
            case Spec of
                {add, Field, FieldValue} ->
                    ok = add_index(Ref, Bucket, Key, Field, FieldValue);
                {remove, Field, _Value} ->
                    ok = remove_index(Ref, Bucket, Key, Field)
            end
    end,
    case sqlite3:sql_exec(Ref,
                    "INSERT INTO store (bucket, key, value) VALUES (?, ?, ?);",
                    [{1, Bucket}, {2, Key}, {3, Val}]) of
                        {rowid, _} ->
                            % update secondary indexes
                            lists:foreach(SpecsHandler, IndexSpecs),
                            {ok, State};
                        {error, 19, _Error} ->
                            %io:format("updating..."),
                            ok = sqlite3:sql_exec(Ref, "UPDATE store SET value = ?
                                    WHERE bucket = ? AND key = ?;",
                                [{1, Val}, {2, Bucket}, {3, Key}]
                            ),
                            lists:foreach(SpecsHandler, IndexSpecs),
                            {ok, State};
                        {error, Code, Error} ->
                            io:format("~p: ~p~n",[Code, Error]),
                            {error, Error, State}
    end.

%%%% @doc Delete an object from the sqlite backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, _IndexSpecs, #state{ref=Ref}=State) ->
    % we do not need to remove secondary indexes in another query
    % thanks to sql DELETE FROM
    case sqlite3:sql_exec(Ref,
            "DELETE FROM store WHERE bucket = ? AND key = ?;",
            [{1, Bucket}, {2, Key}]) of
                ok ->
                    {ok, State};
                [_, _, Error] ->
                    {error, Error, State}
    end.

%% @doc Delete all objects from this sqlite backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{ref=Ref}=State) ->
    case sqlite3:sql_exec(Ref, "DELETE FROM store;") of
            ok ->
                {ok, State};
            _ ->
                {error, "An error.", State}
    end.

%% @doc Returns true if this sqlite backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(#state{ref=Ref}) ->
    % http://www.mail-archive.com/sqlite-users@sqlite.org/msg19151.html
    case sqlite3:sql_exec(Ref, "SELECT (SELECT bucket FROM store LIMIT 1) IS NOT NULL;") of
        [{columns,_},{rows,[{Value}]}] ->
            Value,
            if Value =:= 0 ->
                false;
            true ->
                true
            end;
        _ ->
            {error, "Error in is_empty()"}
    end.

%% @doc Get the status information for this eleveldb backend
-spec status(state()) -> [{atom(), term()}].
status(_State) ->
    [{stats, ok}].

%%%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.


%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, _Opts, #state{ref=Ref}) ->
    FoldFun = fun ({Bucket}, Acc2) ->
            FoldBucketsFun(Bucket, Acc2)
    end,
    %TODO implement async fold
    [{columns, ["bucket"]}, {rows, Rows}] = sqlite3:sql_exec(Ref,
        "SELECT DISTINCT bucket FROM store;"
    ),
    Result = lists:foldl(FoldFun, Acc, Rows),
    {ok, Result}.

fold_keys_query({index, FilterBucket, {eq, <<"$bucket">>, _}}) ->
    {"SELECT bucket, key FROM store WHERE bucket = ?;", [{1, FilterBucket}]};

fold_keys_query({index, FilterBucket, {eq, FilterField, FilterTerm}}) ->
    FilterFieldString = binary:bin_to_list(FilterField),
    case lists:suffix("_spatial", FilterFieldString) of

        true -> fold_keys_query({index, FilterBucket, {spatial, FilterField, FilterTerm}});

        false -> {"SELECT bucket, key FROM store WHERE bucket = ? AND "
            ++ FilterFieldString ++ " = ?;",
            [{1, FilterBucket}, {2, FilterTerm}]
        }
    end;

fold_keys_query({index, FilterBucket, {spatial, FilterField, FilterTerm}}) ->
    FilterFieldString = binary:bin_to_list(FilterField),
    {"SELECT bucket, key FROM store
        WHERE bucket = ?
        AND
        ROWID IN (
            SELECT pkid FROM idx_store_" ++ FilterFieldString
            ++ " WHERE pkid MATCH RTreeIntersects(
            MbrMinX(ST_GeomFromText(?, 4326)),
            MbrMinY(ST_GeomFromText(?, 4326)),
            MbrMaxX(ST_GeomFromText(?, 4326)),
            MbrMaxY(ST_GeomFromText(?, 4326))
        ))
        AND
        ST_intersects(" ++ FilterFieldString ++
                ", ST_GeomFromText(?, 4326))"
      ,
      [{1, FilterBucket}, {2, FilterTerm}, {3, FilterTerm}, {4, FilterTerm},
       {5, FilterTerm}, {6, FilterTerm}]
    };

fold_keys_query({index, FilterBucket, {range, <<"$key">>, StartKey, EndKey}}) ->
    {"SELECT bucket, key FROM store WHERE bucket = ? AND
        key >= ? AND key <= ?;",
        [{1, FilterBucket}, {2, StartKey}, {3, EndKey}]
    };

fold_keys_query({index, FilterBucket, {range, FilterField, StartTerm, EndTerm}}) ->
    {"SELECT bucket, key FROM store WHERE bucket = ? AND "
        ++ binary:bin_to_list(FilterField) ++ " >= ? AND "
        ++ binary:bin_to_list(FilterField) ++ " <= ?;",
        [{1, FilterBucket}, {2, StartTerm}, {3, EndTerm}]
    }.
%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{ref=Ref}) ->

    %% Figure out how we should limit the fold: by bucket, by
    %% secondary index, or neither (fold across everything.)
    Bucket = lists:keyfind(bucket, 1, Opts),
    Index = lists:keyfind(index, 1, Opts),

    %% Multiple limiters may exist. Take the most specific limiter.
    {Query, QueryValues} = if
        Index /= false ->
            fold_keys_query(Index);
        Bucket /= false ->
            {bucket, FilterBucket} = Bucket,
            {"SELECT bucket, key FROM store WHERE bucket = ?", {1, FilterBucket}};
        true ->
            {"SELECT bucket, key FROM store;", []}
    end,
    %io:format("Executing: ~p~n",[Query]),
    %io:format("Query values: ~p~n", [QueryValues]),
    Rows = case sqlite3:sql_exec(Ref, Query, QueryValues) of
        [{columns, ["bucket", "key"]}, {rows, Rows2}] -> Rows2;
        {error, Code, Error} ->
            %TODO add some error handling, e.g. for table does not exists
            %indexes don't exist on some tables
            io:format("~p: ~p~n",[Code, Error]),
            []
    end,
    Folder = fun ({Bucket2, Key2}, Acc2) ->
            FoldKeysFun(Bucket2, Key2, Acc2)
    end,
    %TODO implement async fold
    Result = lists:foldl(Folder, Acc, Rows),
    {ok, Result}.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{ref=Ref}) ->
    %TODO implement async fold
    %TODO implement secondary index
    Bucket = lists:keyfind(bucket, 1, Opts),
    BucketsKeysValues = if
        Bucket /= false ->
            {bucket, FilterBucket} = Bucket,
            [{columns, ["bucket", "key", "value"]}, {rows, Rows}] = sqlite3:sql_exec(
                Ref,
                "SELECT bucket, key, value FROM store WHERE bucket = ?;",
                [{1, FilterBucket}]
            ),
            Rows;
        true ->
            [{columns, ["bucket", "key", "value"]}, {rows, Rows}] = sqlite3:sql_exec(
                Ref,
                "SELECT bucket, key, value FROM store;"
            ),
            Rows
    end,
    FoldFun = fun ({Bucket2, Key2, Value2}, Acc2) ->
            FoldObjectsFun(Bucket2, Key2, Value2, Acc2)
    end,
    Result = lists:foldl(FoldFun, Acc, BucketsKeysValues),
    {ok, Result}.
%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test_() ->
    ?assertCmd("rm -rf test/sqlite-backend"),
    application:set_env(sqlite, data_root, "test/sqlite-backend"),
    riak_kv_backend:standard_test(?MODULE, []).

-endif.

-ifdef(EQC).

eqc_test_() ->
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [
          {timeout, 60000,
           [?_assertEqual(true,
                          backend_eqc:test(?MODULE, false,
                                           [{data_root,
                                             "test/sqlite-backend"},
                                         {async_folds, false}]))]},
          {timeout, 60000,
            [?_assertEqual(true,
                          backend_eqc:test(?MODULE, false,
                                           [{data_root,
                                             "test/sqlite-backend"}]))]}
         ]}]}]}.

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "riak_kv_sqlite_backend_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_sqlite_backend_eqc.log"}),

    ok.

cleanup(_) ->
    ?_assertCmd("rm -rf test/sqlite-backend").

-endif. % EQC
