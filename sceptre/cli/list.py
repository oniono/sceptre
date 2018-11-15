import click

from sceptre.context import SceptreContext
from sceptre.cli.helpers import (
    catch_exceptions,
    write
)
from sceptre.plan.plan import SceptrePlan


@click.group(name="list")
def list_group():
    """
    Commands for listing attributes of stacks.

    """
    pass


@list_group.command(name="resources")
@click.argument("path")
@click.pass_context
@catch_exceptions
def list_resources(ctx, path):
    """
    List resources for stack or stack_group.

    """
    context = SceptreContext(
        command_path=path,
        project_path=ctx.obj.get("project_path"),
        user_variables=ctx.obj.get("user_variables"),
        options=ctx.obj.get("options"),
        output_format=ctx.obj.get("output_format")
    )
    plan = SceptrePlan(context)
    responses = [response[1] for response in plan.describe_resources()]
    write(responses, context.output_format)


@list_group.command(name="outputs")
@click.argument("path")
@click.option(
    "-e", "--export", type=click.Choice(["envvar"]),
    help="Specify the export formatting."
)
@click.pass_context
@catch_exceptions
def list_outputs(ctx, path, export):
    """
    List outputs for stack.

    """
    context = SceptreContext(
        command_path=path,
        project_path=ctx.obj.get("project_path", None),
        user_variables=ctx.obj.get("user_variables", {}),
        options=ctx.obj.get("options", {}),
        output_format=ctx.obj.get("output_format", {})
    )

    plan = SceptrePlan(context)
    responses = [response[1] for response in plan.describe_outputs()]

    if export == "envvar":
        write("\n".join(
            "export SCEPTRE_{0}={1}".format(
                output["OutputKey"], output["OutputValue"]
            )
            for response in responses for output in response
        ))
    else:
        write(responses, context.output_format)


@list_group.command(name="change-sets")
@click.argument("path")
@click.pass_context
@catch_exceptions
def list_change_sets(ctx, path):
    """
    List change sets for stack.

    """
    context = SceptreContext(
        command_path=path,
        project_path=ctx.obj.get("project_path"),
        user_variables=ctx.obj.get("user_variables"),
        output_format=ctx.obj.get("output_format"),
        options=ctx.obj.get("options")
    )

    plan = SceptrePlan(context)
    responses = [response[1] for response in plan.list_change_sets()]

    for response in responses:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            del response['ResponseMetadata']
        write(response, context.output_format)
