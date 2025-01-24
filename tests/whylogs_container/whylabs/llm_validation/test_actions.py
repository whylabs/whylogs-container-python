from whylogs_container.whylabs.llm_validation.actions import Action, ActionType, worst_action


def test_worse_action_both_exist_1():
    action = Action(action_type=ActionType.BLOCK_ACTION, message="block")
    score_action = Action(action_type=ActionType.FLAG_ACTION, message="flag")

    assert worst_action(action, score_action) == action


def test_worse_action_both_exist_2():
    action = Action(action_type=ActionType.FLAG_ACTION, message="flag")
    score_action = Action(action_type=ActionType.BLOCK_ACTION, message="block")

    assert worst_action(action, score_action) == score_action


def test_worse_action_both_exist_3():
    action = Action(action_type=ActionType.BLOCK_ACTION, message="block")
    score_action = Action(action_type=ActionType.BLOCK_ACTION, message="block")

    assert worst_action(action, score_action) == score_action


def test_worse_action_both_exist_4():
    action = Action(action_type=ActionType.FLAG_ACTION, message="flag")
    score_action = Action(action_type=ActionType.FLAG_ACTION, message="flag")

    assert worst_action(action, score_action) == score_action


def test_worse_action_both_exist_5():
    action = Action(action_type=ActionType.PASS_ACTION, message="pass")
    score_action = Action(action_type=ActionType.PASS_ACTION, message="pass")

    assert worst_action(action, score_action) == score_action


def test_worse_action_both_exist_6():
    action = Action(action_type=ActionType.PASS_ACTION, message="pass")
    score_action = Action(action_type=ActionType.BLOCK_ACTION, message="block")

    assert worst_action(action, score_action) == score_action


def test_worse_action_one_exists_1():
    action = Action(action_type=ActionType.BLOCK_ACTION, message="block")
    score_action = None

    assert worst_action(action, score_action) == action


def test_worse_action_one_exists_2():
    action = Action(action_type=ActionType.FLAG_ACTION, message="flag")
    score_action = None

    assert worst_action(action, score_action) == action


def test_worse_action_one_exists_3():
    action = Action(action_type=ActionType.PASS_ACTION, message="pass")
    score_action = None

    assert worst_action(action, score_action) == action
