# -*- coding: utf-8 -*-

"""
sceptre.plan.executor
This module implements a SceptrePlanExecutor, which is responsible for
executing the command specified in a SceptrePlan.
"""
import logging
import traceback
import threading

from concurrent.futures import ThreadPoolExecutor, as_completed
from sceptre.plan.actions import StackActions
from sceptre.stack_status import StackStatus


class SceptrePlanExecutor(object):

    def __init__(self):
        pass

    def execute(self, plan, *args):
        exec_env = ExecutionEnvironment(plan)
        exec_env.execute(*args)


class ExecutionEnvironment(object):

    def __init__(self, plan, *args):
        import ipdb
        ipdb.set_trace()
        self.logger = logging.getLogger(__name__)
        self.plan = plan
        self.num_threads = len(self.plan.launch_order)
        self.threading_events = self._get_threading_events()
        self.stack_statuses = self._get_initial_statuses()

    def execute(self, *args):
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = [
                executor.submit(
                    self.serial_executor,
                    stacks,
                    *args
                )
                for stacks in self.plan.launch_order
            ]
            for future in as_completed(futures):
                response = future.result()
                if response:
                    self.plan.responses.update(response)

    def serial_executor(self, stacks, *args):
        import ipdb
        ipdb.set_trace()
        for stack in stacks:
            if stack is not None:
                response = getattr(StackActions(stack), self.plan.command)
                self.plan.responses.append(response)
        # try:
        #     for stack in stacks:
        #         if stack in self.threading_events:
        #             self.threading_events[stack].wait()
        #             if self.stack_statuses[stack] != StackStatus.COMPLETE:
        #                 self.stack_statuses[stack.name] = StackStatus.FAILED

        #     if self.stack_statuses[stack.name] != StackStatus.FAILED:
        #         try:
        #             response = getattr(
        #                 StackActions(stack),
        #                 self.plan.command
        #             )(*args)
        #             self.stack_statuses[stack.name] = response
        #             self.plan.responses.append(response)
        #         except Exception:
        #             self.stack_statuses[stack.name] = StackStatus.FAILED

        #     self.threading_events[stack.name].set()
        # except Exception as e:
        #     print(e)
        #     traceback.print_exc()

    def _get_threading_events(self):
        """
        Returns a threading.Event() for each stack in every sub-stack.

        :returns: A threading.Event object for each stack, keyed by the
            stack's name.
        :rtype: dict
        """
        stacks = []
        for exec_list in self.plan.launch_order:
            for stack in exec_list:
                stacks.append(stack)

        events = {
            stack: threading.Event()
            for stack in stacks
        }
        self.logger.debug(events)
        return events

    def _get_initial_statuses(self):
        """
        Returns a "pending" sceptre.stack_status.StackStatus for each stack
        in every sub-stack.

        :returns: A "pending" stack status for each stack, keyed by the
            stack's name.
        :rtype: dict
        """
        stacks = []
        for exec_list in self.plan.launch_order:
            for stack in exec_list:
                stacks.append(stack)

        return {
            stack: StackStatus.PENDING
            for stack in stacks
        }
