import ray


@ray.remote
class SchedulingActor:
    """
    Actor in charge of gathering ObjectRefs and scheduling the tasks produced by the head node.
    """

    def __init__(self) -> None:
        self.head = ray.get_actor("simulation_head", namespace="doreisa")

    def ready(self) -> None:
        pass
