-module(session_influxdb).

-export([start_link/1]).
-export([init/1, handle_info/2, handle_call/3, terminate/2]).



start_link(URL) ->
  gen_server:start_link({local,flu_session_db_writer}, ?MODULE, [URL], []).


-record(influx, {
  url
}).

init([<<"influx://", URL0/binary>>]) ->
  [URL1, Database] = binary:split(URL0, <<"/">>),
  URL = <<"http://", URL1/binary, "/db/", Database/binary, "/series">>,
  {ok, #influx{url = URL}}.



handle_call(check, _From, #influx{url = _URL} = DB) ->
  % lager:info("check: ~s", [URL]),
  {reply, ok, DB}.

handle_info({write, Session}, #influx{url = URL} = DB) ->
  [Sessions] = [unpack(Session)|accumulate_sessions(1000)],
  JSON = jsx:encode([#{name => <<"sessions">>, columns => [K || {K,_} <- Sessions], points => [ [V || {_,V} <- Sessions]]}]),
  case lhttpc:request(URL, post, [{<<"Content-Type">>,<<"application/json">>}], JSON, 10000) of
    {ok, {{200,_}, _, _}} ->
      lager:debug("flushed ~p sessions to influxdb", [length(Sessions)]);
    {ok, {{Code,_}, _, Msg}} ->
      lager:info("Failed to flush ~p sessions to influxdb: ~p ~p", [length(Sessions), Code, Msg]);
    {error, Error} ->
      lager:info("Failed to flush ~p sessions to influxdb: ~p", [length(Sessions), Error])
  end,
  {noreply, DB};


handle_info(Msg, #influx{} = DB) ->
  lager:info("msg: ~p", [Msg]),
  {noreply, DB}.


terminate(_,_) -> ok.


accumulate_sessions(0) -> [];
accumulate_sessions(N) ->
  receive
    {write, Session} -> [unpack(Session)|accumulate_sessions(N-1)]
  after
    0 -> []
  end.

unpack(Session) ->
  Whitelist = [country,bytes_sent,host,type,ip,deleted_at,user_agent],
  Name = proplists:get_value(name, Session),
  case re:run(Name, "^[0-9]{1,}/[0-9]{1,}/.*") of
    {match,_} ->
      [ClientID, StreamID, _] = binary:split(Name,<<"/">>,[global]),
      if ClientID == <<"0">> andalso StreamID == <<"0">> -> [];
         true ->
          [{time,proplists:get_value(created_at, Session)},{host,flu:hostname()},{client_id,ClientID},{stream_id,StreamID}] ++ [KV || {K,_} = KV <- Session, lists:member(K,Whitelist)]
      end;
    nomatch ->
      []
  end.




