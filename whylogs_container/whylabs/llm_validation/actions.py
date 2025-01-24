from enum import Enum
from typing import Any, Optional, Union

from pydantic import BaseModel

from langkit.core.workflow import WorkflowResult
from whylogs_container.whylabs.container.config import ConfigInstance

_default_block_message = "Message has been blocked because of a policy violation"
_default_flag_message = "Message has been flagged because of a policy violation"


class ActionType(Enum):
    FLAG_ACTION = "flag"
    BLOCK_ACTION = "block"
    PASS_ACTION = "pass"

    def __str__(self):
        return self.value


class Action(BaseModel):
    action_type: Union[ActionType, Any] = "pass"
    message: Optional[str] = None


def get_action(result: WorkflowResult, config: ConfigInstance, dataset_id: str, org_id: str) -> Action:
    policy = config.config.get_policy(dataset_id, org_id)

    block_message: Optional[str] = None
    flag_message: Optional[str] = None
    if policy and policy.actions:
        block_message = policy.actions.block_message
        flag_message = policy.actions.flag_message

    block_action: Optional[Action] = None
    flag_action: Optional[Action] = None

    for report in result.validation_results.report:
        if report.failure_level == "block":
            block_action = Action(action_type=ActionType.BLOCK_ACTION, message=block_message or _default_block_message)
        elif report.failure_level == "flag":
            flag_action = Action(action_type=ActionType.FLAG_ACTION, message=flag_message or _default_flag_message)

    if block_action:
        return block_action
    if flag_action:
        return flag_action
    else:
        return Action(action_type=ActionType.PASS_ACTION)


def worst_action(action: Action, score_action: Optional[Action]) -> Action:
    """
    Pick the worst action between a normal workflow and a score workflow that consumed the metrics of the former.

    If we have both then we prefer the one with the more severe flag. If they have the same flag level then we prefer the score_action.
    """
    if score_action is None:
        return action

    if score_action.action_type == ActionType.BLOCK_ACTION:
        return score_action
    if action.action_type == ActionType.BLOCK_ACTION:
        return action

    if score_action.action_type == ActionType.FLAG_ACTION:
        return score_action
    if action.action_type == ActionType.FLAG_ACTION:
        return action

    return score_action
