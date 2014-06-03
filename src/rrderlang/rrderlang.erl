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

%% TEST
-ifdef(TEST).
-export([select_header/2, select_row/2]).
-endif.

%% API
-export([start_link/0]).
-export([create/4,
  fetch/3,
  fetch/4,
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
%% Fetch all columns from Round Robin Database.
%%
%% @end
%%--------------------------------------------------------------------
-spec(fetch(Filename :: binary(), Options :: binary(), CF :: binary()) ->
  {ok, Result :: {Timestamp :: integer(), [[float()]]}} |
  {error, Error :: term()}).
fetch(Filename, Options, CF) ->
  fetch(Filename, Options, CF, all).

%%--------------------------------------------------------------------
%% @doc
%% Fetch specified columns from Round Robin Database.
%%
%% @end
%%--------------------------------------------------------------------
-spec(fetch(Filename :: binary(), Options :: binary(), CF :: binary(), Columns :: all | {name, [binary()]} | {starts_with, [binary()]} | {index, [integer()]}) ->
  {ok, {Header, Body}} |
  {error, Error :: term()} when
  Header :: [ColumnNames :: binary()],
  Body :: [Row],
  Row :: [{Timestamp, Values}],
  Timestamp :: binary(),
  Values :: binary()).
fetch(Filename, Options, CF, Columns) ->
  try
    gen_server:call(?MODULE, {fetch, Filename, Options, CF, Columns}, ?TIMEOUT)
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
handle_call({create, Filename, Options, DSs, RRAs}, _From, #state{port = Port} = State) ->
  Command = format_command([<<"create">>, Filename, Options, format_command(DSs), format_command(RRAs), <<"\n">>]),
  Result = execute_command(Port, Command),
  {reply, Result, State};
handle_call({update, Filename, Options, Values, Timestamp}, _From, #state{port = Port} = State) ->
  BinaryValues = lists:map(fun
    (Elem) when is_float(Elem) -> float_to_binary(Elem);
    (Elem) when is_integer(Elem) -> integer_to_binary(Elem);
    (_) -> <<"U">> end, Values),
  Command = format_command([<<"update">>, Filename, Options, format_command([Timestamp | BinaryValues], <<":">>), <<"\n">>]),
  Result = execute_command(Port, Command),
  {reply, Result, State};
handle_call({fetch, Filename, Options, CF, Columns}, _From, #state{port = Port} = State) ->
  Command = format_command([<<"fetch">>, Filename, CF, Options, <<"\n">>]),
  Result = execute_command(Port, Command, Columns),
  {reply, Result, State};
handle_call({graph, Filename, Options}, _From, #state{port = Port} = State) ->
  Command = format_command([<<"graph">>, Filename, Options]),
  Result = execute_command(Port, Command),
  {reply, Result, State};
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


execute_command(Port, Command) ->
  port_command(Port, Command),
  receive_answer(Port).


execute_command(Port, Command, Columns) ->
  port_command(Port, Command),
  case receive_header(Port, Columns) of
    {ok, {Header, NewColumns}} ->
      case receive_body(Port, NewColumns) of
        {ok, Body} ->
          {ok, {Header, Body}};
        {error, Error} ->
          {error, Error}
      end;
    {error, Error} ->
      {error, Error}
  end.

receive_answer(Port) ->
  receive_answer(Port, []).

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

receive_header(Port, Columns) ->
  receive_header(Port, Columns, []).

receive_header(Port, Columns, BinaryHeader) ->
  receive
    {Port, {data, {eol, <<>>}}} ->
      Header = split(BinaryHeader),
      select_header(Header, Columns);
    {Port, {data, {eol, <<"ERROR: ", Error/binary>>}}} ->
      {error, Error};
    {Port, {data, {eol, Data}}} ->
      receive_header(Port, Columns, Data);
    Other ->
      {error, Other}
  end.

select_header(Header, Columns) ->
  try
    select_header(Header, Columns, 1, [], [])
  catch
    _:_ -> {error, <<"Header selection error.">>}
  end.

select_header(Header, all, N, NewHeader, NewColumns) ->
  select_header(Header, {index, lists:seq(1, length(Header))}, N, NewHeader, NewColumns);
select_header([], _, _, NewHeader, NewColumns) ->
  {ok, {lists:reverse(NewHeader), lists:reverse(NewColumns)}};
select_header([Value | Values], {index, Columns}, N, NewHeader, NewColumns) ->
  case exists(N, {index, Columns}) of
    true -> select_header(Values, {index, Columns}, N + 1, [Value | NewHeader], [N | NewColumns]);
    _ -> select_header(Values, {index, Columns}, N + 1, NewHeader, NewColumns)
  end;
select_header([Value | Values], Columns, N, NewHeader, NewColumns) ->
  case exists(Value, Columns) of
    true -> select_header(Values, Columns, N + 1, [Value | NewHeader], [N | NewColumns]);
    _ -> select_header(Values, Columns, N + 1, NewHeader, NewColumns)
  end.

exists(_, {_, []}) ->
  false;
exists(Value, {index, Columns}) when is_integer(Value) ->
  lists:member(Value, Columns);
exists(Value, {name, [Value | _]}) ->
  true;
exists(Value, {name, [_ | Columns]}) ->
  exists(Value, {name, Columns});
exists(Value, {starts_with, [Column | Columns]}) ->
  Size = size(Column),
  case binary:longest_common_prefix([Value, Column]) of
    Size -> true;
    _ -> exists(Value, {starts_with, Columns})
  end;
exists(_, _) ->
  false.

receive_body(Port, Columns) ->
  receive_body(Port, Columns, []).

receive_body(Port, Columns, Body) ->
  receive
    {Port, {data, {eol, <<"OK", _/binary>>}}} ->
      {ok, lists:reverse(Body)};
    {Port, {data, {eol, <<"ERROR: ", Error/binary>>}}} ->
      {error, Error};
    {Port, {data, {eol, Data}}} ->
      case select_row(Data, Columns) of
        {ok, Row} ->
          receive_body(Port, Columns, [Row | Body]);
        {error, Error} ->
          {error, Error}
      end;
    Other ->
      {error, Other}
  end.


select_row(Data, Columns) ->
  try
    [TimeStamp, Values | _] = binary:split(Data, <<":">>, [global]),
    case select_row(split(Values), Columns, 1, []) of
      {ok, Row} -> {ok, {TimeStamp, lists:reverse(Row)}};
      _ -> {error, <<"Body selection error.">>}
    end
  catch
    _:_ -> {error, <<"Body selection error.">>}
  end.

select_row(_, [], _, Acc) ->
  {ok, Acc};
select_row([], _, _, _) ->
  {error, <<"Body selection error: no more data.">>};
select_row([Value | Values], [N | Columns], N, Acc) ->
  select_row(Values, Columns, N + 1, [to_number(Value) | Acc]);
select_row([_ | Values], Columns, N, Acc) ->
  select_row(Values, Columns, N + 1, Acc).

split(Data) ->
  lists:filter(fun
    (Value) -> Value =/= <<>>
  end, binary:split(Data, ?SEPARATOR, [global])).

to_number(Binary) ->
  try
    binary_to_float(Binary)
  catch
    _:_ -> nan
  end.


