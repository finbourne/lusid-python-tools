import argparse
import sys
import lusidtools.lpt as lpt
import os
import ast
import importlib


def find_tools():
    """
    Searches for available tools in the LPT folder with their TOOLNAMEs and TOOLTIPs.

    Finds tools that contain a main() function and descriptive data. TOOLNAME is the abbreviated name for a tool, and
    TOOLTIP contains a brief description of the functionality of the tool.

    Returns
    -------
    LPT_tools       generator
                    generator object containing LPT TOOLNAMEs and TOOLTIPs
    """
    root = lpt.__path__[0]

    files = [filename for filename in os.listdir(root) if filename.endswith(".py")]
    for filename in files:
        with open(os.path.join(root, filename), "rt") as file:
            toolmodule = filename[:-3]

            # parse the Abstract Syntax tree for each command
            tree = ast.parse(file.read(), filename=filename)

            # extract TOOLNAME and TOOLTIP from assignment nodes
            toolname = [
                item.value.s
                for item in tree.body
                if isinstance(item, ast.Assign)
                and "TOOLNAME" == item.targets[0].id
                and isinstance(item.value, ast.Str)
            ]

            tooltip = [
                item.value.s
                for item in tree.body
                if isinstance(item, ast.Assign)
                and "TOOLTIP" == item.targets[0].id
                and isinstance(item.value, ast.Str)
            ]

            # Handle cases where TOOLTIP/TOOLNAME is assigned multiple or no times
            if len(toolname) < 1:
                toolname = filename[:-3]
            else:
                toolname = toolname[0]

            if len(tooltip) < 1:
                tooltip = toolname
            else:
                tooltip = " / ".join(tooltip)

            if (
                len(
                    [
                        item.name
                        for item in tree.body
                        if isinstance(item, ast.FunctionDef) and "main" == item.name
                    ]
                )
                == 1
            ):
                yield toolname, (toolmodule, tooltip)


def help_method(tools):
    """
    Displays help documentation for LPT to command line

    Parameters
    ----------
    tools       dict(tuple())
                dictionary mapping LPT tools to their TOOLNAME and TOOLTIP

    Returns
    -------

    """

    def add_tip(tool, tip) -> str:
        """
        Returns string of toolname and description
        """
        return tool if tip == "" else f"{tool: <30} - {tip}"

    epilog = "Available tools. - use <tool> -h for additional help\n\n" + "\n".join(
        sorted([add_tip(k, v[1]) for k, v in tools.items()])
    )

    parser = argparse.ArgumentParser(
        description="Lusid Python Tools Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    parser.add_argument(
        "tool", metavar="<tool>", help="Tool to run. Options are listed below."
    )
    parser.add_argument("arg1", metavar="<arg-1>", help=" - tool")
    parser.add_argument("arg2", metavar=" ... ", help=" - specific")
    parser.add_argument("arg3", metavar="<arg-n>", help=" - arguments")
    parser.parse_args(["--help"])
    exit()


def main():
    # get list of LPT tools
    tools = dict(find_tools())

    args = sys.argv
    if len(sys.argv) > 1:
        # If a tool has been specified as a command line arg
        first = sys.argv[1]
        if first in tools:
            # if Tool requested in available tools, import that module and run it's main method
            mod = importlib.import_module(f"lusidtools.lpt.{tools[first][0]}")
            sys.argv = [f"{sys.argv[0]} {first}"] + sys.argv[2:]
            mod.main()
            exit()

    # If no tools have been specified, then run the LPT help method to display available tools
    help_method(tools)


if __name__ == "__main__":
    main()
