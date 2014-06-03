%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2014, ACK CYFRONET AGH
%%% @doc
%%%
%%% @end
%%% Created : 16. Mar 2014 8:09 PM
%%%-------------------------------------------------------------------
-module(rrderlang).
-author("Krzysztof Trzepla").

-behaviour(gen_server).
-include("common.hrl").

%% API
-export([start_link/0]).
-export([create/4,
  fetch/3,
  fetch/4,
  fetch/5,
  update/3,
  update/4,
  graph/2]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {port}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Create Round Robin Database.
%%
%% @end
%%--------------------------------------------------------------------
-spec(create(Filename :: binary(), Options :: binary(), DSs :: [binary()], RRAs :: [binary()]) ->
  {ok, Result :: binary()} |
  {error, Error :: term()}).
create(Filename, Options, DSs, RRAs) ->
  try
    gen_server:call(?MODULE, {create, Filename, Options, DSs, RRAs}, ?TIMEOUT)
  catch
    Error:Reason ->
      {Error, Reason}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Update Round Robin Database using current timestamp.
%%
%% @end
%%--------------------------------------------------------------------
-spec(update(Filename :: binary(), Options :: binary(), Values :: [float() | integer()]) ->
  {ok, Result :: binary()} |
  {error, Error :: term()}).
update(Filename, Options, Values) ->
  update(Filename, Options, Values, <<"N">>).

%%--------------------------------------------------------------------
%% @doc
%% Update Round Robin Database using given timestamp.
%%
%% @end
%%--------------------------------------------------------------------
-spec(update(Filename :: binary(), Options :: binary(), Values :: [float() | integer()], Timestamp :: integer() | binary()) ->
  {ok, Result :: binary()} |
  {error, Error :: term()}).
update(Filename, Options, Values, Timestamp) when is_integer(Timestamp) ->
  update(Filename, Options, Values, integer_to_binary(Timestamp));
update(Filename, Options, Values, Timestamp) ->
  try
    gen_server:call(?MODULE, {update, Filename, Options, Values, Timestamp}, ?TIMEOUT)
  catch
    Error:Reason ->
      {Error, Reason}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Fetch all columns from Round Robin Database and return them in default format.
%%
%% @end
%%--------------------------------------------------------------------
-spec(fetch(Filename :: binary(), Options :: binary(), CF :: binary()) ->
  {ok, Result :: {Timestamp :: integer(), [[float()]]}} |
  {error, Error :: term()}).
fetch(Filename, Options, CF) ->
  fetch(Filename, Options, CF, all, default).

%%--------------------------------------------------------------------
%% @doc
%% Fetch all columns Round Robin Database and return them in specified format.
%%
%% @end
%%--------------------------------------------------------------------
-spec(fetch(Filename :: binary(), Options :: binary(), CF :: binary(), Formatter :: default | json) ->
  {ok, Result :: term()} |
  {error, Error :: term()}).
fetch(Filename, Options, CF, Formatter) ->
  fetch(Filename, Options, CF, all, Formatter).

%%--------------------------------------------------------------------
%% @doc
%% Fetch specified columns Round Robin Database and return them in specified format.
%%
%% @end
%%--------------------------------------------------------------------
-spec(fetch(Filename :: binary(), Options :: binary(), CF :: binary(), Columns :: all | [binary() | {starts_with, binary()} | integer()], Formatter :: default | json) ->
  {ok, Result :: term()} |
  {error, Error :: term()}).
fetch(Filename, Options, CF, Columns, Formatter) ->
  try
    gen_server:call(?MODULE, {fetch, Filename, Options, CF, Columns, Formatter}, ?TIMEOUT)
  catch
    Error:Reason ->
      {Error, Reason}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Create graphical representation of data from Round Robin Database.
%%
%% @end
%%--------------------------------------------------------------------
-spec(graph(Filename :: binary(), Options :: binary()) ->
  {ok, Result :: binary()} |
  {error, Error :: term()}).
graph(Filename, Options) ->
  try
    gen_server:call(?MODULE, {graph, Filename, Options}, ?TIMEOUT)
  catch
    Error:Reason ->
      {Error, Reason}
  end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  process_flag(trap_exit, true),
  case os:find_executable("rrdtool") of
    false ->
      {stop, no_rrdtool};
    RRDtool ->
      Port = open_port({spawn_executable, RRDtool}, [{line, 1024}, {args, ["-"]}, binary]),
      {ok, #state{port = Port}}
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call({create, Filename, Options, DSs, RRAs}, _From, State) ->
  Command = format_command([<<"create">>, Filename, Options, format_command(DSs), format_command(RRAs), <<"\n">>]),
  execute_command(Command, State);
handle_call({update, Filename, Options, Values, Timestamp}, _From, State) ->
  BinaryValues = lists:map(fun
    (Elem) when is_float(Elem) -> float_to_binary(Elem);
    (Elem) when is_integer(Elem) -> integer_to_binary(Elem);
    (_) -> <<"U">> end, Values),
  Command = format_command([<<"update">>, Filename, Options, format_command([Timestamp | BinaryValues], <<":">>), <<"\n">>]),
  execute_command(Command, State);
handle_call({fetch, Filename, Options, CF, Columns, Formatter}, _From, State) ->
  Command = format_command([<<"fetch">>, Filename, CF, Options, <<"\n">>]),
  execute_command(Command, Columns, Formatter, State);
handle_call({graph, Filename, Options}, _From, State) ->
  Command = format_command([<<"graph">>, Filename, Options]),
  execute_command(Command, State);
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, State) ->
  port_command(State#state.port, [<<"quit\n">>]),
  port_close(State#state.port),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

format_command(Command) ->
  format_command(Command, ?SEPARATOR).

format_command([], _) ->
  <<>>;
format_command([Command | Commands], Separator) ->
  lists:foldl(fun(Elem, Acc) -> <<Acc/binary, Separator/binary, Elem/binary>> end, Command, Commands).

execute_command(Command, State) ->
  port_command(State#state.port, Command),
  case receive_answer(State#state.port, []) of
    {ok, Answer} -> {reply, {ok, Answer}, State};
    {error, Error} -> {reply, {error, Error}, State}
  end.


execute_command(Command, Columns, default, State) ->
  execute_command(Command, Columns, fun formatters:default_formatter/1, State);
execute_command(Command, Columns, json, State) ->
  execute_command(Command, Columns, fun formatters:json_formatter/1, State);
execute_command(Command, Columns, Formatter, State) ->
  port_command(State#state.port, Command),
  case receive_header(State#state.port, [], Columns) of
    {ok, {BinaryHeader, NewColumns}} ->
      case receive_values(State#state.port, [], NewColumns) of
        {ok, BinaryValues} ->
          {reply, Formatter(BinaryValues ++ BinaryHeader), State};
        {error, Error} ->
          {reply, {error, Error}, State}
      end;
    {error, Error} ->
      {reply, {error, Error}, State}
  end.

receive_answer(Port, Acc) ->
  receive
    {Port, {data, {eol, <<"OK", _/binary>>}}} ->
      {ok, Acc};
    {Port, {data, {eol, <<"ERROR: ", Error/binary>>}}} ->
      {error, Error};
    {Port, {data, {eol, Data}}} ->
      receive_answer(Port, [Data | Acc]);
    Other ->
      {error, Other}
  end.

receive_header(Port, Acc, all) ->
  receive
    {Port, {data, {eol, <<>>}}} ->
      {ok, {[<<>> | Acc], all}};
    {Port, {data, {eol, <<"ERROR: ", Error/binary>>}}} ->
      {error, Error};
    {Port, {data, {eol, Data}}} ->
      receive_header(Port, [Data | Acc], all);
    Other ->
      {error, Other}
  end;
receive_header(Port, Acc, Columns) ->
  receive
    {Port, {data, {eol, <<>>}}} ->
      {ok, {[<<>> | Acc], Columns}};
    {Port, {data, {eol, <<"ERROR: ", Error/binary>>}}} ->
      {error, Error};
    {Port, {data, {eol, Data}}} ->
      case select_header(Data, Columns) of
        {ok, {NewData, NewColumns}} ->
          receive_header(Port, [NewData | Acc], NewColumns);
        {error, Error} -> {error, Error}
      end;
    Other ->
      {error, Other}
  end.

receive_values(Port, Acc, all) ->
  receive_answer(Port, Acc);
receive_values(Port, Acc, Columns) ->
  receive
    {Port, {data, {eol, <<"OK", _/binary>>}}} ->
      {ok, Acc};
    {Port, {data, {eol, <<"ERROR: ", Error/binary>>}}} ->
      {error, Error};
    {Port, {data, {eol, Data}}} ->
      case select_values(Data, Columns) of
        {ok, {NewData, NewColumns}} ->
          receive_values(Port, [NewData | Acc], NewColumns);
        {error, Error} -> {error, Error}
      end;
    Other ->
      {error, Other}
  end.

select_header(Data, Columns) ->
  Values = lists:filter(fun
    (Value) -> Value =/= <<>>
  end, binary:split(Data, ?SEPARATOR, [global])),
  select_data(Values, Columns, [], 1, [0]).

select_values(Data, Columns) ->
  Values = lists:filter(fun
    (Value) -> Value =/= <<>>
  end, binary:split(Data, ?SEPARATOR, [global])),
  select_data(Values, Columns, [], 0, []).

select_data(_, [], Acc, _, NewColumns) ->
  Data = lists:foldl(fun
    (Binary, <<>>) -> Binary;
    (Binary, BinaryAcc) -> <<Binary/binary, " ", BinaryAcc/binary>>
  end, <<>>, Acc),
  Columns = lists:reverse(NewColumns),
  {ok, {Data, Columns}};
select_data([], _, _, _, _) ->
  {error, <<"Selection error: no more data.">>};
select_data([Value | Values], [{starts_with, Column} | Columns], Acc, N, NewColumns) when is_binary(Column) ->
  Length = size(Column),
  case binary:longest_common_prefix([Value, Column]) of
    Length -> select_data(Values, Columns, [Value | Acc], N + 1, [N | NewColumns]);
    _ -> select_data(Values, [{starts_with, Column} | Columns], Acc, N + 1, NewColumns)
  end;
select_data([_ | Values], [{starts_with, Column} | Columns], Acc, N, NewColumns) when is_binary(Column) ->
  select_data(Values, [{starts_with, Column} | Columns], Acc, N + 1, NewColumns);
select_data([Column | Values], [Column | Columns], Acc, N, NewColumns) when is_binary(Column) ->
  select_data(Values, Columns, [Column | Acc], N + 1, [N | NewColumns]);
select_data([_ | Values], [Column | Columns], Acc, N, NewColumns) when is_binary(Column) ->
  select_data(Values, [Column | Columns], Acc, N + 1, NewColumns);
select_data([Value | Values], [N | Columns], Acc, N, NewColumns) ->
  select_data(Values, Columns, [Value | Acc], N + 1, [N | NewColumns]);
select_data([_ | Values], Columns, Acc, N, NewColumns) ->
  select_data(Values, Columns, Acc, N + 1, NewColumns).



