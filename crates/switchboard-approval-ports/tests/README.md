# Approval ports tests

The integration target reads the public enums exhaustively and checks the exact
stop-cause mapping. Compile-fail doctests exercise construction from an external
consumer. Both targets exercise declarations without a service, clock or network.
