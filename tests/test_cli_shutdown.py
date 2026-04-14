import signal
import unittest
from unittest.mock import Mock, patch

from cli import _install_signal_shutdown, _remove_signal_shutdown


class _FakeLoop:
    def __init__(self):
        self.handlers = {}
        self.removed = []

    def add_signal_handler(self, sig, callback, *args):
        self.handlers[sig] = (callback, args)

    def remove_signal_handler(self, sig):
        self.removed.append(sig)


class CliShutdownTests(unittest.TestCase):
    def test_install_signal_shutdown_cancels_main_task_and_sets_stop_reason(self):
        loop = _FakeLoop()
        main_task = Mock()
        stop_context = {"reason": "completed"}

        with patch("cli.click.echo") as echo:
            registered = _install_signal_shutdown(loop, main_task, stop_context)
            callback, args = loop.handlers[signal.SIGTERM]
            callback(*args)

        self.assertEqual(registered, [signal.SIGTERM])
        self.assertEqual(stop_context["reason"], "signal_sigterm")
        main_task.cancel.assert_called_once_with()
        echo.assert_called_once_with("Received SIGTERM; shutting down...")

    def test_remove_signal_shutdown_unregisters_registered_signals(self):
        loop = _FakeLoop()

        _remove_signal_shutdown(loop, [signal.SIGTERM])

        self.assertEqual(loop.removed, [signal.SIGTERM])


if __name__ == "__main__":
    unittest.main()
