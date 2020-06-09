import argparse

# Ensure standardisation of commonly used arguments


class Parser:

    # Create a parser and add in the standard arguments
    def __init__(self, description, sections=[]):
        self.parser = argparse.ArgumentParser(
            description=description, fromfile_prefix_chars="@"
        )
        self.post_processors = []
        self.arguments = []

        if "scope" in sections:
            self.add("scope", help="Scope")

        if "portfolio" in sections:
            self.add("portfolio", help="Portfolio id")

        if "date" in sections:
            self.add("date", help="date YYYY-MM-DD")

        if "input" in sections:
            self.add("input", help="input filename")

        if "properties" in sections:
            self.add("--properties", nargs="+", help="List of propertykeys", default=[])

        if "filename" in sections:
            self.add(
                "-f", "--filename", metavar="filename.csv", help="write to this file"
            )

        if "limit" in sections:
            self.add(
                "-l",
                "--limit",
                type=int,
                default=0,
                metavar="n",
                help="limit the number of results",
            )

        if "date_range" in sections:
            self.add("-s", "--start_date", dest="start_date", metavar="YYYY-MM-DD")
            self.add("-e", "--end_date", dest="end_date", metavar="YYYY-MM-DD")

        if "quiet" in sections:
            self.add(
                "-q",
                "--quiet",
                action="store_true",
                help="Quiet mode. Doesn't show the progress bar",
            )

        self.add(
            "--secrets-file",
            dest="secrets",
            default="secrets.json",
            help="path to secrets file",
        )

        self.add(
            "--environment",
            dest="env",
            default=["lusid"],
            nargs="+",
            help="choose a special LUSID environment. E.g. 'ipsum_lorem'",
        )

        self.add(
            "--stats",
            dest="stats",
            metavar="stats-file.csv",
            const="-",
            nargs="?",
            help="Write statistics to file. [Leave blank for stdout]",
        )

        if "asat" in sections:
            self.add(
                "--asat",
                dest="asat",
                metavar="YYYY-MM-DDTHH:MM:SS.000",
                help="as-at time",
            )

        if "test" in sections:
            self.add("--test", action="store_true", help="Run in test mode")

        # If a filename parameter is given then pass output via dfq
        # Can be supressed by the NODFQ option
        if "filename" in sections and "NODFQ" not in sections:
            self.add(
                "--dfq",
                nargs=argparse.REMAINDER,
                help="pass the output through to 'dfq' - see dfq --help for options",
            )

    def add(self, *args, **kwargs):
        # Add arguments to a list, so they can be removed
        self.arguments.append((args[0], args, kwargs))
        # self.parser.add_argument(*args,**kwargs)
        return self

    def remove(self, key):
        self.arguments = [tpl for tpl in self.arguments if tpl[0] != key]

    def post_process(self, fn):
        self.post_processors.append(fn)
        return self

    def parse(self, args=None):
        for arg in self.arguments:
            self.parser.add_argument(*arg[1], **arg[2])

        args = self.parser.parse_args(args)
        for fn in self.post_processors:
            fn(args)

        return args

    def extend(self, fn):
        if fn:
            fn(self)
        return self
