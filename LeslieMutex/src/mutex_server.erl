-module(mutex_server).

-behaviour(gen_server).

-export([start_link/1, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([lock/1, unlock/1]).

-define(SERVER, ?MODULE).
-record(state,
		{timestamp, 
		 step,
		 pending_client_requests, 
		 request_queue, 
		 peers_response,
		 owned_lock_client
		}).

stop() ->
    gen_server:call(?SERVER, stop).

start_link(Step) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Step], []).

lock(ClientPid) ->
    gen_server:call(?SERVER, {start_client_mutex_lock, ClientPid}).

unlock({Timestamp, ClientPid}) ->
	gen_server:call(?SERVER, {start_client_mutex_unlock, {Timestamp, ClientPid}}).

%% 
init([Step]) ->
    {ok, #state{timestamp=0,
			   step=Step,
			   pending_client_requests=[],
			   request_queue=request_queue:new(),
			   peers_response=#{},
			   owned_lock_client=none
			   }}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call({start_client_mutex_lock, ClientPid}, _From, State) ->
	#state{timestamp = Timestamp, 
	 step = Step,
     pending_client_requests = PendingRequests, 
     request_queue = RequestQueue,
	 peers_response = PeersResponse} = State,

	NextTimestamp = next_timestamp(Timestamp, Step),
    NewRequestQueue = request_queue:put({NextTimestamp, node()}, RequestQueue),
	NewPendingRequests = PendingRequests ++ [{NextTimestamp, ClientPid}],
	lists:foreach(
		fun(Node) ->
			gen_server:cast({?SERVER, Node},
							{mutex_lock_request, {NextTimestamp, node()}})
		end,
		nodes()),
	NewState0 = State#state{request_queue=NewRequestQueue, timestamp=NextTimestamp},
	case PeersResponse of
		#{} ->
			NewPeersResponse = maps:from_list(
								 [{Node, 0} || Node <- nodes()]);
		_ ->
			NewPeersResponse = PeersResponse
	end,	
    {reply, ok, NewState0#state{peers_response=NewPeersResponse, pending_client_requests=NewPendingRequests}};

handle_call({start_client_mutex_unlock, {Timestamp, ClientPid}}, _From, State) ->
	#state{owned_lock_client=OwnedClient, request_queue=RequestQueue} = State,
	
	{NewRequestQueue, NewOwnedClient} = 
		case match_owned_lock_client(OwnedClient, {Timestamp, ClientPid}) of
			true -> 
				%% send async mutex_unlock_request to peers
				lists:foreach(
			  		fun(Node) ->
						gen_server:cast({?SERVER, Node},
									  	{mutex_unlock_request, {Timestamp, node()}})
			  		end,
			  		nodes()),
				%% remove from current request queue
				RQ = request_queue:remove({Timestamp, node()}, RequestQueue),
				%% notify mutex client
				ClientPid ! {ok, unlocked},
				{RQ, none};
			false -> 
				ClientPid ! {error, mutex_not_owned},
				{RequestQueue, OwnedClient}
		end,
	{reply, ok, State#state{request_queue=NewRequestQueue, owned_lock_client=NewOwnedClient}}.

match_owned_lock_client(A, A) -> true;
match_owned_lock_client(_, _) -> false.

handle_cast({mutex_lock_request, {Timestamp, Peer}}, State) ->
	#state{request_queue = RequestQueue, step = Step, timestamp = MyTimestamp} = State,
	NewRequestQueue = request_queue:put({Timestamp, Peer}, RequestQueue),

	ResponseTimestamp = round_up_first_timestamp(max(MyTimestamp, Timestamp), Step),
	gen_server:cast({?SERVER, Peer}, {mutex_lock_response, {ResponseTimestamp, node()}}),
	NewState = State#state{timestamp = ResponseTimestamp, request_queue = NewRequestQueue},
	{noreply, NewState};

handle_cast({mutex_lock_response, {PeerTimestamp, PeerRef}}, State) ->
	#state{request_queue=RequestQueue, 
		   pending_client_requests = PendingClientRequests, 
		   peers_response = PeersResponse} = State,

	NewPeersResponse = case maps:find(PeerRef, PeersResponse)  of
		{ok, Value} ->
			if 
				PeerTimestamp > Value ->
					maps:put(PeerRef, PeerTimestamp, PeersResponse);
			  	true ->
					PeersResponse
			end;
		error ->
			io:format("Error: invalid peer (~p)~n", [PeerRef]),
			PeersResponse
	end,
	{NewPendingClientRequests, OwnedClient} = check_grant_lock(PendingClientRequests, NewPeersResponse, RequestQueue),
	{noreply, State#state{pending_client_requests=NewPendingClientRequests,
						  peers_response = NewPeersResponse,
						  owned_lock_client=OwnedClient}};

handle_cast({mutex_unlock_request, {Timestamp, ServerRef}}, State) ->
	#state{pending_client_requests=PendingClientRequests,
		   request_queue=RequestQueue,
		   peers_response=PeersResponse} = State,
	NewRequestQueue = request_queue:remove({Timestamp, ServerRef}, RequestQueue),
	{NewPendingClientRequests, OwnedClient} = 
		check_grant_lock(PendingClientRequests, PeersResponse, NewRequestQueue),
	{noreply, State#state{pending_client_requests=NewPendingClientRequests, 
						  request_queue=NewRequestQueue,
						  owned_lock_client=OwnedClient}}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% internal functions

check_grant_lock(PendingClientRequests, PeersResponse, RequestQueue) ->
	case handle_mutex_client(PendingClientRequests, PeersResponse, RequestQueue) of
		{ok, {Timestamp, ClientPid}} ->
			[_|T] = PendingClientRequests,
			{T, {Timestamp, ClientPid}};
		_ ->
			{PendingClientRequests, none}
	end.

handle_mutex_client([], _, _) ->
	false;
handle_mutex_client([{Timestamp, ClientPid} | _], PeersResponse, RequestQueue) ->
	case local_check_locked_condition(Timestamp, PeersResponse) andalso 
		 first_of_request_queue(RequestQueue)
	of
		true ->
			ClientPid ! {ok, {locked, Timestamp}},
			{ok, {Timestamp, ClientPid}};
		false ->
			false
	end.

local_check_locked_condition(Timestamp, PeersResponse) ->
	lists:all(fun(PeerTimestamp) -> PeerTimestamp > Timestamp end, maps:values(PeersResponse)).
		
first_of_request_queue(RequestQueue) ->
	case request_queue:is_first(node(), RequestQueue) of
		{ok, _} -> true;
		_ -> false
	end.

%% return round up 
round_up_first_timestamp(Timestamp, Step) ->
	floor(Timestamp / Step) * Step + Step.

next_timestamp(CurTime, Step) ->
    CurTime + Step.
