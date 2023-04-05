from IPython import display


class StopExecution(Exception):
    """
    This object is used to stop a notebook from running.
    Example: raise StopExecution("Portfolio missing")
    """

    def __init__(self, message):
        self.message = message

    def _render_traceback_(self):
        display(self.message)
