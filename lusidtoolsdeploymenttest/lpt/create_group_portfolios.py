from lusidtools.lpt import lpt
from lusidtools.lpt import lse
from lusidtools.lpt import stdargs
from lusidtools.lpt.either import Either

# Protfolio Group column names
GROUP_NAME = "Group Name"
CREATED_DATE = "Created Date"
PORTFOLIOS = "Portfolios"
SUB_GROUPS = "Sub-groups"
DISPLAY_NAME = "Display Name"
DESCRIPTION = "Description"
TOOLNAME = "port_grp_create"
TOOLTIP = "Display specified Instruments"


def parse(extend=None, args=None):
    return (
        stdargs.Parser("Create Portfolio Groups", ["scope", "filename"])
        .add("input", nargs="*")
        .extend(extend)
        .parse(args)
    )


def process_args(api, args):
    # Build the ResourceID variables
    def resource_id_vars(names):
        return [api.models.ResourceId(args.scope, code) for code in names]

    # Sort the dataframe in order for the sub-groups to be created before the groups (takes care of the dependency sub-group group)
    def sorted_group_df(df):

        df_sorted = df[df[SUB_GROUPS].isnull()]
        df_not_sorted = df[df[SUB_GROUPS].notnull()]

        while not df_not_sorted.empty:
            for i, row in df_not_sorted.iterrows():
                if row[SUB_GROUPS] in df_sorted[GROUP_NAME].to_list():
                    df_sorted = df_sorted.append(row)
                    df_not_sorted = df_not_sorted.drop(i)
            if (
                not any(
                    x in df_sorted[GROUP_NAME].to_list()
                    for x in df_not_sorted[SUB_GROUPS].to_list()
                )
                and not df_not_sorted.empty
            ):
                raise ValueError(
                    "Is a circular dependency between the groups and subgroups, or one of the subgroups does not exist {}".format(
                        df_not_sorted
                    )
                )

        return df_sorted

    def load_groups(iterator, group_dict):
        try:
            index, row = next(iterator)
            return api.call.create_portfolio_group(
                scope=args.scope,
                create_portfolio_group_request=api.models.CreatePortfolioGroupRequest(
                    code=row[GROUP_NAME],
                    created=row.get(CREATED_DATE, None),
                    values=group_dict[row[GROUP_NAME]][PORTFOLIOS],
                    sub_groups=group_dict[row[GROUP_NAME]][SUB_GROUPS],
                    display_name=row[DISPLAY_NAME],
                    description=row[DESCRIPTION],
                ),
            ).bind(lambda r: load_groups(iterator, group_dict))
        except StopIteration:
            return Either.Right(None)

    def create_groups():
        df = lpt.read_input(args.input[0])

        # Build the group dictionaries {'group_name': {'Portfolios: ['portfolio1]', 'portfolio2'], 'Sub-groups': ['sub-group1', 'sub-group2']}
        group_dict = {}
        group_dict.update(
            {
                group_name: {
                    SUB_GROUPS: resource_id_vars(
                        group_df[SUB_GROUPS].dropna().unique()
                    ),
                    PORTFOLIOS: resource_id_vars(
                        group_df[PORTFOLIOS].dropna().unique()
                    ),
                }
                for group_name, group_df in df.groupby(GROUP_NAME)
            }
        )

        df = sorted_group_df(df)
        df.drop_duplicates(subset=[GROUP_NAME, DISPLAY_NAME], keep="last", inplace=True)
        return load_groups(df.iterrows(), group_dict)

    return create_groups().bind(lambda r: Either.Right("Done!"))


# Standalone tool
def main(parse=parse, display_df=lpt.display_df):
    lpt.standard_flow(parse, lse.connect, process_args, display_df)
