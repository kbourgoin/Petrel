import logging
import time

from petrel import ipc

log = logging.getLogger(__name__)

class Component(object):
    """Base class for Spout and Bolt implementations"""
    # TODO: Figure this bit out
    def report_exception(self, base_message, exception):
        ipc.send_error('does this work?')
        return
        parameters = (
            base_message,
            os.environ.get('SCRIPT', sys.argv[0]),
            socket.gethostname(),
            'pid', os.getpid(),
            'port', self.worker_port,
            'taskindex', self.task_index,
            type(exception).__name__,
            #str(exception),
        )
        #message = '%s: %s (pid %d) on %s failed with %s: %s' % parameters
        message = '__'.join(str(p).replace('.', '_') for p in parameters)
        sendFailureMsgToParent(message)

        # Sleep for a few seconds to try and ensure Storm reads this message
        # before we terminate. If it does, then our message above will appear in
        # the Storm UI.
        time.sleep(5)

class Spout(Component):
    """Base Spout class

    `ack`, `fail`, and `initialize` can be overridden to provide that
    functionality for the spout.

    `next_tuple` *must* be overridden or the spout won't be able to emit!

    """
    def initialize(self, conf, context):
        """Override to perform spout initialization

        For an example of the input format, see the `multilang handshake`_

        .. _multilang handshake: https://github.com/nathanmarz/storm/wiki/Multilang-protocol#initial-handshake

        :param conf: Configuration info from Storm
        :param context: Topology context from Storm

        """
        pass

    def ack(self, id):
        """Called when a tuple has finished processing"""
        pass

    def fail(self, id):
        """Called when a Tuple has failed"""
        pass

    def next_tuple(self):
        """Called when the next Tuple is requested. Implement this.

        If a Tuple is not ready, because of i/o wait or whatever,
        call `self.wait()` to wait without blocking other tasks
        the spout needs to perform, like ack'ing Tuples.

        """
        raise NotImplementedError('A spout needs to emit Tuples')

    def run(self):
        # Init IPC to get conf info and then init bolt
        self.conf, self.context = ipc.initialize()
        self.initialize(self.conf, self.context)

        # Synchronous loop to process incoming messages.
        # Unlike Bolts, this doesn't need to be threaded or evented because
        # spouts get messages in-order
        # See: https://github.com/nathanmarz/storm/wiki/Multilang-protocol#spouts
        try:
            while True:
                msg = ipc.read()
                command = msg["command"]
                log.info('command: %s', command)
                if command == "next":
                    self.next_tuple()
                elif command == "ack":
                    self.ack(msg["id"])
                elif command == "fail":
                    self.fail(msg["id"])
                ipc.send_sync()
        except Exception, e:
            self.report_exception('E_SPOUTFAILED', e)
            log.exception('Caught exception in Spout.run: %s', str(e))

    def wait(self, secs=0.1):
        """Sleep for `secs` before telling Storm there are no Tuples ready"""
        # TODO: gevent compat
        time.sleep(secs)
        ipc.sync()

    def emit(self,
             values,
             stream=None,
             id=None,
             direct_task=None,
             need_task_ids=False):
        """Emit a tuple"""
        msg = {"command": "emit",
               "tuple": values,
               "need_task_ids": need_task_ids,
               }
        if id is not None:
            msg["id"] = id
        if stream is not None:
            msg["stream"] = stream
        if direct_task is not None:
            msg["task"] = direct_task
        ipc.write(msg)
        if need_task_ids:
            return ipc.read()
