import logging

from traceback import format_exc
from abc import abstractmethod

from .definitions import Job, Handler, SessionFactory

logger = logging.getLogger(__name__)


class BaseHandler(object):
    """
    Base Class for all Handlers
    """

    def __init__(self, task_id: int, handler_name: str = None, mutable: bool = True) -> None:
        """
        Construct a new instance
        of Base Handler (cannot actually
        be instantiated because abstract
        methods need to be implemented)

        :param task_id: (int) the job id of the pending
            task to handle
        :param handler_name: (str) the name of the handler (how it will be referenced in DB)
        :param mutable: (bool) whether or not this handler can be toggled in its availability through
            access handlers

        """

        self.task_id: int = task_id

        self.task: Job or None = None  # assigned later

        self.handler_name: str = handler_name

        self.mutable: bool = mutable
        self.Session = SessionFactory()

    def is_handler_enabled(self) -> bool:
        """
        Check whether the associated handler
        is enabled in the database

        :return: (boolean) whether or not the handler is enabled
        """
        if not self.mutable:  # non-mutable handlers are always enabled
            return True

        handler: Handler = self.Session.query(Handler).filter_by(name=self.handler_name).first()

        if handler:
            return handler.enabled

        raise Exception(f"Handler {self.handler_name} cannot be found in the database")

    @staticmethod
    def _format_html_exception(traceback: str) -> str:
        """
        Turn a Python string into HTML so that it can be rendered in the deployment GUI

        :param string: string to format for the GUI
        :returns: string in HTML pre-formatted code-block format

        """
        # what we need to change in order to get formatted HTML <code>
        replacements: dict = {
            '\n': '<br>',
            "'": ""
        }
        for subject, target in replacements.items():
            traceback: str = traceback.replace(subject, target)

        return f'<br><code>{traceback}</code>'

    def update_task_in_db(self, status: str = None, info: str = None) -> None:
        """
        Update the current task in the Database
        under a local context

        :param status: (str) the new status to update to
        :param info: (str) the new info to update to

        One or both of the arguments (status/info) can be
        specified, and this function will update
        the appropriate attributes of the task and commit it
        """
        self.task.update(status=status, info=info)
        self.Session.add(self.task)
        self.Session.commit()

    def succeed_task_in_db(self, success_message: str) -> None:
        """
        Succeed the current task in the Database under
        a local session context

        :param success_message: (str) the message to update
            the info string to

        """
        self.task.succeed(success_message)
        self.Session.add(self.task)
        self.Session.commit()

    def fail_task_in_db(self, failure_message: str) -> None:
        """
        Fail the current task in the database under
        a local session context

        :param failure_message: (str) the message to updatr
            the info string to

        """
        self.task.fail(failure_message)

        self.Session.add(self.task)
        self.Session.commit()

    @abstractmethod
    def _handle(self) -> None:
        """
        Subclass-specific job handler
        functionality
        """
        pass

    # noinspection PyBroadException
    def handle(self):
        """
        Handle the given task and update
        statuses/detailed info on error/success
        """
        try:
            logger.info("Checking Handler Availability")
            if self.is_handler_enabled():
                logger.info("Handler is available")
                self.task: Job = self.Session.query(Job).filter(Job.id == self.task_id).first()
                self.task.parse_payload()
                logger.info("Retrieved task: " + str(self.task.__dict__))

                self.update_task_in_db(status='RUNNING', info='A Service Worker has found your Job')
                self._handle()
                self.succeed_task_in_db(f"Success! Target '{self.handler_name} completed successfully.")
            else:
                self.fail_task_in_db(f"Error: Target '{self.handler_name}' is disabled")

            self.Session.commit()

        except Exception:
            logger.exception(f"Encountered an unexpected error while processing Task #{self.task_id}")
            self.Session.rollback()
            self.fail_task_in_db(f"Error: <br>{self._format_html_exception(format_exc())}")

        finally:
            self.Session.close()  # close the thread local session in all cases, exception or no exception
