#!/usr/bin/env python
"""Postgres Connection."""

from twisted.internet import defer

from txpostgres import txpostgres


class PatchedConnection(txpostgres.Connection):
    """Postgres Connection for Async."""

    def connect(self, *args, **kwargs):
        """
        Connect to the database.

        The reason this class exists is because the psycopg2cffi expects the
        keyword argument 'async_' whereas txpostgres passes in 'async'.

        Any arguments will be passed to :attr:`connectionFactory`. Use them to
        pass database names, usernames, passwords, etc.

        :return: A :d:`Deferred` that will fire when the connection is open.

        :raise: :exc:`~txpostgres.txpostgres.AlreadyConnected` when the
            connection has already been opened.
        """
        if self.detector:
            self.detector.setReconnectable(self, *args, **kwargs)

        if self._connection and not self._connection.closed:
            return defer.fail(txpostgres.AlreadyConnected())

        kwargs['async_'] = True
        try:
            self._connection = self.connectionFactory(*args, **kwargs)
        except Exception:
            return defer.fail()

        def startReadingAndPassthrough(ret):
            """Add socket to reactor."""
            self.reactor.addReader(self)
            return ret

        # The connection is always a reader in the reactor, to receive NOTIFY
        # events immediately when they're available.
        d = self.poll()
        return d.addCallback(startReadingAndPassthrough)

