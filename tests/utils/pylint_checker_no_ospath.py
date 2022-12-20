# pylint: disable=ospath-check
"""
Custom Pylint checker to detect the use of Python built-in os.path.
Pathlib is preferred over os.path and this custom checker is intended
to help avoid the use of os.path.

Extended from example:
https://github.com/PyCQA/pylint/blob/main/examples/custom_raw.py
"""
from typing import TYPE_CHECKING

from astroid import nodes
from pylint.checkers import BaseRawFileChecker

if TYPE_CHECKING:
    from pylint.lint import PyLinter


class OSPathUseChecker(BaseRawFileChecker):
    """
    Check for the use of os.path and recommend pathlib instead
    """

    name = "custom_os_path_check"
    msgs = {
        "W0001": (
            "use pathlib instead of os.path for filesystem operations",
            "ospath-check",
            (
                "Used when os.path has been detected where pathlib"
                "may otherwise be used."
            ),
        )
    }
    options = ()

    def process_module(self, node: nodes.Module) -> None:
        """Process a module.

        the module's content is accessible via node.stream() function
        """
        with node.stream() as stream:
            for (lineno, line) in enumerate(stream):
                # check for os.path used within a line
                if "os.path" in str(line):
                    self.add_message("ospath-check", line=lineno)


def register(linter: "PyLinter") -> None:
    """This required method auto registers the checker during initialization.

    :param linter: The linter to register the checker to.
    """
    linter.register_checker(OSPathUseChecker(linter))
