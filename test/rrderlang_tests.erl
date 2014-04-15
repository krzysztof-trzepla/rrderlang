%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2014, ACK CYFRONET AGH
%%% @doc This module tests the functionality of rrderlang module.
%%% It contains unit tests that base on eunit.
%%%
%%% @end
%%% Created : 25. Mar 2014 20:07
%%%-------------------------------------------------------------------
-module(rrderlang_tests).
-author("Krzysztof Trzepla").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("common.hrl").
-define(RRD_NAME, "test_database.rrd").
-endif.

-ifdef(TEST).

%% ===================================================================
%% Tests description
%% ===================================================================

rrderlang_test_() ->
  {foreach,
    fun setup/0,
    fun teardown/1,
    [
      {"should start rrderlang application", fun should_start_rrderlang_application/0},
      {"should create rrd", fun should_create_rrd/0},
      {"should not create rrd when_missing parameters", fun should_not_create_rrd_when_missing_parameters/0},
      {"should not overwrite rrd", fun should_not_overwrite_rrd/0},
      {"should update rrd", fun should_update_rrd/0},
      {"should not update rrd when not created", fun should_not_update_rrd_when_not_created/0},
      {"should not update rrd when missing parameters", fun should_not_update_rrd_when_missing_parameters/0},
      {"should fetch data", {timeout, 15, fun should_fetch_data/0}},
      {"should not fetch data", fun should_not_fetch_data/0},
      {"should fetch selected data", {timeout, 15, fun should_fetch_selected_data/0}},
      {"should stop rrderlang application", fun should_stop_rrderlang_application/0}
    ]
  }.

%% ===================================================================
%% Setup/teardown functions
%% ===================================================================

setup() ->
  ok.

teardown(_) ->
  ?assertCmd("rm -f " ++ ?RRD_NAME).

%% ===================================================================
%% Tests functions
%% ===================================================================

should_start_rrderlang_application() ->
  ?assertEqual(ok, application:start(rrderlang)).

should_create_rrd() ->
  Filename = list_to_binary(?RRD_NAME),
  Options = <<"--step 1">>,
  DSs = [
    <<"DS:first:GAUGE:20:-100:100">>,
    <<"DS:second:COUNTER:20:-100:100">>,
    <<"DS:third:DERIVE:20:-100:100">>,
    <<"DS:fourth:ABSOLUTE:20:-100:100">>
  ],
  RRAs = [
    <<"RRA:AVERAGE:0.5:1:100">>,
    <<"RRA:MIN:0.5:1:100">>,
    <<"RRA:MAX:0.5:1:100">>,
    <<"RRA:LAST:0.5:1:100">>
  ],
  {CreateAnswer, _} = rrderlang:create(Filename, Options, DSs, RRAs),
  ?assertEqual(ok, CreateAnswer).

should_not_create_rrd_when_missing_parameters() ->
  Filename = list_to_binary(?RRD_NAME),
  Options = <<>>,
  DSs = [],
  RRAs = [],
  {CreateAnswer, _} = rrderlang:create(Filename, Options, DSs, RRAs),
  ?assertEqual(error, CreateAnswer).

should_not_overwrite_rrd() ->
  should_create_rrd(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<"--no-overwrite">>,
  DSs = [
    <<"DS:first:GAUGE:20:-100:100">>
  ],
  RRAs = [
    <<"RRA:AVERAGE:0.5:1:100">>
  ],
  {CreateAnswer, _} = rrderlang:create(Filename, Options, DSs, RRAs),
  ?assertEqual(error, CreateAnswer).

should_update_rrd() ->
  should_create_rrd(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<>>,
  {FirstUpdateAnswer, _} = rrderlang:update(Filename, Options, [1.0, 2, 3, 4]),
  ?assertEqual(ok, FirstUpdateAnswer),
  {SecondUpdateAnswer, _} = rrderlang:update(Filename, Options, [1, 2, 3, 4]),
  ?assertEqual(ok, SecondUpdateAnswer),
  {ThirdUpdateAnswer, _} = rrderlang:update(Filename, Options, [1.0, 2, 3, 4]),
  ?assertEqual(ok, ThirdUpdateAnswer).

should_not_update_rrd_when_not_created() ->
  Filename = list_to_binary(?RRD_NAME),
  Options = <<>>,
  {UpdateAnswer, _} = rrderlang:update(Filename, Options, [1.0, 2.0, 3.0, 4.0]),
  ?assertEqual(error, UpdateAnswer).

should_not_update_rrd_when_missing_parameters() ->
  should_create_rrd(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<>>,
  {UpdateAnswer, _} = rrderlang:update(Filename, Options, []),
  ?assertEqual(error, UpdateAnswer).

should_fetch_data() ->
  should_create_rrd(),

  Data = update_rrd_ntimes(10, 1),
  [{StartTime, _} | _] = Data,
  BinaryStartTime = integer_to_binary(StartTime - 1),
  {EndTime, _} = lists:last(Data),
  BinaryEndTime = integer_to_binary(EndTime - 1),

  Filename = list_to_binary(?RRD_NAME),
  Options = <<"--start ", BinaryStartTime/binary, " --end ", BinaryEndTime/binary>>,
  CF = <<"AVERAGE">>,
  {FetchAnswer, {FetchHeader, FetchData}} = rrderlang:fetch(Filename, Options, CF),

  ?assertEqual(ok, FetchAnswer),
  ?assertEqual([<<"first">>, <<"second">>, <<"third">>, <<"fourth">>], FetchHeader),
  lists:zipwith(fun
    ({_, [Value | _]}, {_, [FetchValue | _]}) -> ?assertEqual(Value, round(FetchValue))
  end, Data, FetchData).

should_not_fetch_data() ->
  should_create_rrd(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<"--start -5 --end -10">>,
  CF = <<"AVERAGE">>,
  {FetchAnswer, _} = rrderlang:fetch(Filename, Options, CF),
  ?assertEqual(error, FetchAnswer).

should_fetch_selected_data() ->
  should_create_rrd(),

  Data = update_rrd_ntimes(10, 1),
  [{StartTime, _} | _] = Data,
  BinaryStartTime = integer_to_binary(StartTime - 1),
  {EndTime, _} = lists:last(Data),
  BinaryEndTime = integer_to_binary(EndTime - 1),

  Filename = list_to_binary(?RRD_NAME),
  Options = <<"--start ", BinaryStartTime/binary, " --end ", BinaryEndTime/binary>>,
  CF = <<"AVERAGE">>,
  {FetchAnswer, {FetchHeader, FetchData}} = rrderlang:fetch(Filename, Options, CF, [<<"first">>], default),

  ?assertEqual(ok, FetchAnswer),
  ?assertEqual([<<"first">>], FetchHeader),
  lists:zipwith(fun
    ({_, [Value | _]}, {_, [FetchValue | Rest]}) ->
      ?assertEqual(Value, round(FetchValue)),
      ?assertEqual([], Rest)
  end, Data, FetchData).

should_stop_rrderlang_application() ->
  ?assertEqual(ok, application:stop(rrderlang)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

update_rrd_ntimes(N, Step) ->
  update_rrd_ntimes(N, Step * 1000, []).

update_rrd_ntimes(0, _, Acc) ->
  lists:reverse(Acc);
update_rrd_ntimes(N, Step, Acc) ->
  {MegaSecs, Secs, MicroSecs} = erlang:now(),
  random:seed(MegaSecs, Secs, MicroSecs),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<>>,
  Values = lists:map(fun(_) -> random:uniform(100) end, lists:duplicate(4, 0)),
  Timestamp = 1000000 * MegaSecs + Secs,
  {UpdateAnswer, _} = rrderlang:update(Filename, Options, Values, Timestamp),
  ?assertEqual(ok, UpdateAnswer),
  timer:sleep(Step),
  update_rrd_ntimes(N - 1, Step, [{Timestamp, Values} | Acc]).

-endif.
