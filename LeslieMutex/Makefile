all:RUN

RUN:src/*.erl
	erlc -o ebin src/*.erl

.PHONY:clean
clean:
	@rm ebin/*.beam
