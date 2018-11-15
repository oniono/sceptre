# -*- coding: utf-8 -*-

"""
sceptre.plan.executor
This module implements a SceptrePlanExecutor, which is responsible for
executing the command specified in a SceptrePlan.
"""
import logging

from concurrent.futures import ThreadPoolExecutor, as_completed
from sceptre.plan.actions import StackActions
from sceptre.stack_status import StackStatus


class SceptrePlanExecutor(object):

    def __init__(self, command, launch_order):
        self.logger = logging.getLogger(__name__)
        self.command = command
        self.launch_order = launch_order

        self.num_threads = len(max(launch_order, key=len))
        self.stack_statuses = {stack: StackStatus.PENDING
                               for batch in launch_order for stack in batch}

    def execute(self, *args):
        responses = {}

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            for batch in self.launch_order:
                futures = [executor.submit(self._execute, stack, *args)
                           for stack in batch]

                failed = False
                for future in as_completed(futures):
                    stack, status = future.result()
                    responses[stack] = status

                    if status == StackStatus.FAILED:
                        failed = True

                if failed:
                    break

        return responses

    def _execute(self, stack, *args):
        try:
            actions = StackActions(stack)
            status = getattr(actions, self.command)(*args)
        except:
            self.stack_statuses[stack] = StackStatus.FAILED
            self.logger.exception("Stack %s failed to %s", stack.name,
                                  self.command)
        else:
            self.stack_statuses[stack] = status

        return stack, self.stack_statuses[stack]
