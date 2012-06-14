-module(mod_chat_log_mongodb).

-behaviour(gen_mod).
-behaviour(gen_server).

-export([start/2,
    stop/1,
    log_user_send/3
]).

-export([init/1, start_link/2, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-include_lib("exmpp/include/exmpp.hrl").
-include_lib("exmpp/include/exmpp_jid.hrl").

-include("ejabberd.hrl").

-define(PROCNAME, ?MODULE).
-define(INTERVAL, 1000). % flush to mongo every 2 seconds

-record(state, {
	host,
	db,
	collection,
	conn
}).

%% gen_mod callbacks

start(Host, Opts) ->
    ?INFO_MSG("~p starting...", [?MODULE]),
    HostB = list_to_binary(Host),
	ejabberd_hooks:add(user_send_packet, HostB, ?MODULE, log_user_send, 55),
	Proc = gen_mod:get_module_proc(Host, ?PROCNAME),

	ChildSpec =
	    {Proc,
	        {?MODULE, start_link, [Host, Opts]},
	        temporary,
	        1000,
	        worker,
	        [?MODULE]},
	supervisor:start_child(ejabberd_sup, ChildSpec).

stop(Host) ->
    HostB = list_to_binary(Host),
    ejabberd_hooks:delete(user_send_packet, HostB, ?MODULE, log_user_send, 50),
	Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
	gen_server:call(Proc, stop),
	supervisor:delete_child(ejabberd_sup, Proc),
    ok.


%% gen_server callbacks

start_link(Host, Opts) ->
	Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
	?INFO_MSG("*** ********** ~p", [Proc]),
	gen_server:start_link({local, Proc}, ?MODULE, [Host, Opts], []).

init([_Host, Opts]) ->
    ?INFO_MSG("*** INIT", []),
    ?MODULE = ets:new(?MODULE, [ordered_set, public, named_table]),
	%timer:send_interval(?INTERVAL, flush),
	
	Host = gen_mod:get_opt(hosts, Opts, ["localhost:27017"]),
	DB = gen_mod:get_opt(db, Opts, "xmpp"),
	Collection = gen_mod:get_opt(collection, Opts, "message"),
	mongodb:replicaSets(xmpp_mongo, Host),
	mongodb:connect(xmpp_mongo),
	
	Conn = mongoapi:new(xmpp_mongo, list_to_binary(DB)),
	Conn:set_encode_style(mochijson),
	
	{ok, #state{
		host = Host,
		db = DB,
		collection = Collection,
		conn = Conn
	}}.

terminate(_Reason, _State) ->
	mongodb:deleteConnection(xmpp_mongo),
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

handle_call(Request, _From, State) ->
	?INFO_MSG("Unexpected call: ~p", [Request]),
	{reply, ok, State}.

handle_cast({save, Rec}, S=#state{conn=Conn, collection=Coll}) ->
    %?INFO_MSG("Save Message: ~p", [Rec]),
    Conn:save(Coll, Rec),
    {noreply, S}.

handle_info(flush, S=#state{conn=Conn, collection=Coll}) ->
    %?INFO_MSG("flushing chat log to mongo...", []),

    %% separate out the ets timestamps and records
    {Keys, Vals} = lists:foldl(fun({Key, Val}, {Keys, Vals}) -> {[Key|Keys], 
        [Val|Vals]} end, {[], []}, flush()),
    
    case {Keys, Vals} of
        {[], []} ->
            ok;
        {Times, Recs} ->
            %% insert the records into mongo
            ok = Conn:batchInsert(Coll, Recs),
            
            %% remove the ets entries we just saved to mongo
            lists:foldl(fun(Key, _) -> ets:delete(?MODULE, Key) end, [], Times),
            ok
    end,
    
    %?INFO_MSG("flushed ~p messages", [length(Keys)]),
    {noreply, S};
handle_info(Info, State) ->
    %?INFO_MSG("Unexpected info: ", [Info]),
    {noreply, State}.

%% ejabberd hook callback

log_user_send(From, To, Packet) ->
    log_packet(From, To, Packet).

%% private

log_packet(From, To, Packet=#xmlel{name='message', attrs=Attrs}) ->
    Type = exmpp_xml:get_attribute_from_list(Attrs, <<"type">>, <<>>),
	case Type of
		"error" -> %% we don't log errors
			?DEBUG("dropping error: ~s", [xmpp_xml:document_to_list(Packet)]),
			ok;
		_ ->
			save_packet(From, To, Packet, Type)
	end;    
log_packet(_From, _To, _Packet) ->
    ok.

save_packet(From, To, Packet, Type) ->
	Body = exmpp_xml:get_cdata(exmpp_xml:get_element(Packet, "body")),
	MsgUUID = exmpp_xml:get_attribute_as_binary(Packet, <<"msguuid">>, ""),
	case Body of
		<<"">> -> %% don't log empty messages
			?DEBUG("not logging empty message from ~p",[From]),
			ok;
		_ ->
			
			FromJid = exmpp_jid:prep_node_as_list(From),
			FromHost = exmpp_jid:prep_domain_as_list(From),
			FromResource = exmpp_jid:prep_resource_as_list(From),
			ToJid = exmpp_jid:prep_node_as_list(To),
			ToHost = exmpp_jid:prep_domain_as_list(To),
			ToResource = exmpp_jid:prep_resource_as_list(To),			
			Timestamp = unix_timestamp(),
			MicroTime = now_us(erlang:now()),

          %   Rec = {MicroTime, [
	        	% {<<"_from_user">>, prepare(FromJid)},
          %       {<<"from_host">>, prepare(FromHost)},
          %       {<<"from_resource">>, prepare(FromResource)},
          %       {<<"_to_group">>, prepare(ToJid)},
          %       {<<"to_host">>, prepare(ToHost)},
          %       {<<"to_resource">>, prepare(ToResource)},
          %       {<<"content">>, Body},
          %       {<<"timestamp">>, Timestamp},
          %       {<<"timestamp_micro">>, MicroTime},
          %       {<<"msg_type">>, Type},
          %       {<<"msg_uuid">>, MsgUUID}
          %   ]},
          %   ets:insert(?MODULE, Rec)
          	Rec = [
	        	{<<"_from_user">>, prepare(FromJid)},
                {<<"from_host">>, prepare(FromHost)},
                {<<"from_resource">>, prepare(FromResource)},
                {<<"_to_group">>, prepare(ToJid)},
                {<<"to_host">>, prepare(ToHost)},
                {<<"to_resource">>, prepare(ToResource)},
                {<<"content">>, Body},
                {<<"timestamp">>, Timestamp},
                {<<"timestamp_micro">>, MicroTime},
                {<<"msg_type">>, Type},
                {<<"msg_uuid">>, MsgUUID}
            ],
            %?INFO_MSG("ready to send... ~p", [Rec]),
            Proc = gen_mod:get_module_proc(FromHost, ?PROCNAME),
            %?INFO_MSG("ready to send...proc ~p", [Proc]),
            gen_server:cast(Proc, {save, Rec})
            %?INFO_MSG("ready to sended ~p", [Proc])
            % ets:insert(?MODULE, Rec)
	end.

flush() ->
    flush(now_us(erlang:now()), [], ets:next(?MODULE, 0)).

flush(_, Acc, '$end_of_table') ->
    Acc;
flush(MaxKey, Acc, CurrKey) when CurrKey < MaxKey ->
    [Res] = ets:lookup(?MODULE, CurrKey),
    flush(MaxKey, [Res|Acc], ets:next(?MODULE, CurrKey));
flush(MaxKey, Acc, CurrKey) when CurrKey >= MaxKey ->
    Acc.

unix_timestamp() ->
    unix_timestamp(calendar:universal_time()).

unix_timestamp(DT) ->
    Epoch = {{1970,1,1},{0,0,0}},
    calendar:datetime_to_gregorian_seconds(DT) -
        calendar:datetime_to_gregorian_seconds(Epoch).
		
now_us({MegaSecs,Secs,MicroSecs}) ->
	(MegaSecs*1000000 + Secs)*1000000 + MicroSecs. 

prepare(Val) ->
    case Val of
        undefined -> 
            <<"">>;
	    Val when is_list(Val) ->
		    list_to_binary(Val);
        _ -> 
            Val
    end.
