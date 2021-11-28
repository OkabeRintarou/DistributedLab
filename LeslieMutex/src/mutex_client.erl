-module(mutex_client).

-export([lock/0, unlock/1]).

%% mutex_server:start_link();
%% {ok, Lock} = mutex_clict:lock();
%% do something when owning the distributed lock
%% mutex_clict:unlock(Lock).

-record(leslie_mutex, {timestamp, pid}).

lock() ->
	Pid = self(),
	mutex_server:lock(Pid),
	receive
		{ok, {locked, Timestamp}} ->
			{ok, #leslie_mutex{timestamp=Timestamp, pid=self()}};
		Other ->
			Other
	end.

unlock(#leslie_mutex{timestamp=Timestamp, pid=Pid}) ->
	mutex_server:unlock({Timestamp, Pid}),
	receive
		Msg -> Msg
	end.
