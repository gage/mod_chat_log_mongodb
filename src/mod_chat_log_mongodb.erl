-module(mod_chat_log_mongodb).

-behaviour(gen_mod).
-behaviour(gen_server).

-export([start/2,
     start_link/2,
	 stop/1,
	 log_user_receive/4,
	 unix_timestamp/0,
	 unix_timestamp/1,
	 now_us/1,
         check_if_undefined/1
	 ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
 		terminate/2, code_change/3]).

-include_lib("exmpp/include/exmpp.hrl").
-include_lib("exmpp/include/exmpp_jid.hrl").

-include("ejabberd.hrl").


-define(PROCNAME, ?MODULE).

-record(state, {
	host,
	db,
	collection,
	conn
}).


start_link(Host, Opts) ->
	Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
	gen_server:start_link({local, Proc}, ?MODULE, [Host, Opts], []).

start(Host, Opts) ->
	HostB = list_to_binary(Host),
	?INFO_MSG("Starting ~p on ~p", [?MODULE, HostB]),
	ejabberd_hooks:add(user_receive_packet, HostB, ?MODULE, log_user_receive, 55),
	Proc = gen_mod:get_module_proc(Host, ?PROCNAME),

	ChildSpec =
			{Proc,               
				{?MODULE, start_link, [Host, Opts]},
				permanent,
				50,
				worker,
				[?MODULE]},
	supervisor:start_child(ejabberd_sup, ChildSpec).

init([_Host, Opts]) ->
	?INFO_MSG("~p init", [?MODULE]),

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
	?INFO_MSG("Terminate called", []),
	mongodb:deleteConnection(xmpp_mongo),
	mongodb:stop(),
	ok.

stop(Host) ->
    HostB = list_to_binary(Host),
    ejabberd_hooks:delete(user_receive_packet, HostB, ?MODULE, log_user_receive, 50),
	Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
	gen_server:call(Proc, stop),
	supervisor:delete_child(ejabberd_sup, Proc),
    ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

handle_call(stop, _From, State) ->
	{stop, normal, State}.

handle_cast({save,  FromJid, FromHost, FromResource, ToJid, ToHost, ToResource, Body, Type}, State) ->
	#state{collection=Collection,conn=Conn} = State,
    FromJid2 = check_if_undefined(FromJid),
    FromHost2 = check_if_undefined(FromHost),
    FromResource2 = check_if_undefined(FromResource),
    ToJid2 = check_if_undefined(ToJid),
    ToHost2 = check_if_undefined(ToHost),
    ToResource2 = check_if_undefined(ToResource),
          
	Conn:save(Collection, [
			{<<"from">>, FromJid2},
			{<<"from_host">>, FromHost2},
			{<<"from_resource">>, FromResource2},
			{<<"to">>, ToJid2},
			{<<"to_host">>, ToHost2},
			{<<"to_resource">>, ToResource2},
			{<<"content">>, Body},
			{<<"timestamp">>, unix_timestamp()},
			{<<"timestamp_micro">>, now_us(erlang:now())},
			{<<"type">>, Type}
		]),
	{noreply, State}.

handle_info({'DOWN', _MonitorRef, process, _Pid, _Info}, State) ->
	{stop, connection_dropped, State};
handle_info(Info, State) ->
	?INFO_MSG("Got Info:~p, State:~p", [Info, State]),
	{noreply, State}.

log_user_receive(Jid, From, To, Packet) ->
    log_packet(Jid, From, To, Packet).
	
log_packet(#jid{domain = Server}, From, To,
		    Packet=#xmlel{name = 'message', attrs = Attrs, children = Els}) ->
    Type = exmpp_xml:get_attribute_from_list(Attrs, <<"type">>, <<>>),
	case Type of
		"error" -> %% we don't log errors
			?DEBUG("dropping error: ~s", [xmpp_xml:document_to_list(Packet)]),
			ok;
		_ ->
			save_packet(From, To, Packet, Type)
	end;
    
log_packet(_JID, _From, _To, _Packet) ->
    ok.


save_packet(From, To, Packet, Type) ->
	Body = exmpp_xml:get_cdata(exmpp_xml:get_element(Packet, "body")),
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
			Proc = gen_mod:get_module_proc(ToHost, ?PROCNAME),
			gen_server:cast(Proc, {save, FromJid, FromHost, FromResource, ToJid, ToHost, ToResource, Body, Type})
	end.
	

unix_timestamp() ->
    unix_timestamp(calendar:universal_time()).

unix_timestamp(DT) ->
    %LocalEpoch = calendar:universal_time_to_local_time({{1970,1,1},{0,0,0}}),
    Epoch = {{1970,1,1},{0,0,0}},
    calendar:datetime_to_gregorian_seconds(DT) -
        calendar:datetime_to_gregorian_seconds(Epoch).
		
now_us({MegaSecs,Secs,MicroSecs}) ->
	(MegaSecs*1000000 + Secs)*1000000 + MicroSecs. 

check_if_undefined(Val) ->
    case Val of
    undefined -> <<"">>;
	Val when is_list(Val) ->
		list_to_binary(Val);
    _ -> 
        Val
    end.
