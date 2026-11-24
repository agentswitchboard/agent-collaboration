# Switchboard Central link tests

Module tests drive the link policy with explicit elapsed time and independent expected state.

The integration target uses real loopback WebSockets and an injected clock. The
C5 cases assert the advertisement order across reconnect, both typed refusals
and ordinary traffic on the original socket, and the existing phase and decode
fault behavior.
