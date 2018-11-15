# -*- coding: utf-8 -*-

"""
sceptre.plan.executor
This module implements a SceptrePlanExecutor, which is responsible for
executing the command specified in a SceptrePlan.
"""

from sceptre.plan.actions import StackActions
from botocore.exceptions import ClientError
from sceptre.config.graph import StackDependencyGraph


class SceptrePlanExecutor(object):

    def __init__(self):
        pass

    def execute(self, plan, *args):
        if plan.stack_group.stacks:
            stacks = stack_dependency_resolution(plan.stack_group.stacks)
            for stack in stacks:
                try:
                    result = getattr(StackActions(stack), plan.command)(*args)
                except(ClientError) as exp:
                    not_exists = exp.response.get("Error", {}).get("Message")
                    if not_exists and not_exists.endswith("does not exist"):
                        plan.errors.append(exp)
                        continue
                    else:
                        raise
                plan.responses.append(result)


def stack_dependency_resolution(stacks):
    all_dependencies = {
        stack.name: stack.dependencies
        for stack in stacks
    }

    stack_order = StackDependencyGraph(all_dependencies).as_dict().keys()

    return [
        next(stack for stack in stacks if stack.name == stack_name)
        for stack_name in stack_order
    ]
