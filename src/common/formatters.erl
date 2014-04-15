%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2014, ACK CYFRONET AGH
%%% @doc
%%%
%%% @end
%%% Created : 25. Mar 2014 17:22
%%%-------------------------------------------------------------------
-module(formatters).
-author("Krzysztof Trzepla").

-include("common.hrl").

%% API
-export([default_formatter/1, json_formatter/1]).

default_formatter(BinaryData) ->
  default_formatter(BinaryData, []).

default_formatter([<<>>, BinaryHeader], Data) ->
  Header = lists:filter(fun
    (Column) -> Column =/= <<>>
  end, binary:split(BinaryHeader, ?SEPARATOR, [global])),
  {ok, {Header, Data}};
default_formatter([<<BinaryTimestamp:10/binary, ": ", BinaryValues/binary>> | BinaryData], Data) ->
  Timestamp = binary_to_integer(BinaryTimestamp),
  Values = lists:map(fun
    (BinaryValue) -> try binary_to_float(BinaryValue)
                     catch _:_ -> nan end
  end, binary:split(BinaryValues, ?SEPARATOR, [global])),
  default_formatter(BinaryData, [{Timestamp, Values} | Data]);
default_formatter(_, _) ->
  {error, format_error}.

json_formatter(BinaryData) ->
  json_formatter(BinaryData, []).

json_formatter([<<>>, BinaryHeader], Data) ->
  FormatHeader = fun
    (Column) when is_binary(Column) -> "{\"label\": \"" ++ binary_to_list(Column) ++ "\", \"type\": \"number\"}";
    (Column) -> "{\"label\": \"" ++ Column ++ "\", \"type\": \"date\"}"
  end,
  Header = lists:map(fun
    (Column) -> ", " ++ FormatHeader(Column)
  end, lists:filter(fun
    (Column) -> Column =/= <<>>
  end, binary:split(BinaryHeader, ?SEPARATOR, [global]))),
  Columns = FormatHeader("Timestamp") ++ string:join(Header, ""),
  Rows = string:join(Data, ", "),
  {ok, "{\"cols\": [" ++ Columns ++ "], \"rows\": [" ++ Rows ++ "]}"};
json_formatter([<<BinaryTimestamp:10/binary, ": ", BinaryValues/binary>> | BinaryData], Data) ->
  FormatValue = fun
    (Value) when is_binary(Value) -> "{\"v\": " ++ binary_to_list(Value) ++ "}";
    (Value) -> "{\"v\": " ++ Value ++ "}"
  end,
  Values = lists:map(fun
    (BinaryValue) -> try _Result = binary_to_float(BinaryValue), ", " ++ FormatValue(BinaryValue)
                     catch _:_ -> ", " ++ FormatValue("\"null\"") end
  end, binary:split(BinaryValues, ?SEPARATOR, [global])),
  Row = "{\"c\": [" ++ FormatValue("\"Date(" ++
    integer_to_list(binary_to_integer(BinaryTimestamp) * 1000) ++ ")\"") ++
    string:join(Values, "") ++ "]}",
  json_formatter(BinaryData, [Row | Data]);
json_formatter(_, _) ->
  {error, format_error}.