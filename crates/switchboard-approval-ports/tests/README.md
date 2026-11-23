# Approval ports tests

The integration target reads the public enums exhaustively and checks the exact
stop-cause mapping. A small consumer exercises owner binding registration,
retirement and the unregistered-binding fault through the ports. Compile-fail
doctests exercise construction from an external consumer. These checks use no
service, clock or network.
